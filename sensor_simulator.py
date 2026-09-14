#!/usr/bin/env python3
"""
sensor_simulator.py
===================
Publishes *synthetic sensor readings* alongside the ground-truth replay.

Two kinds of sensor exist here:

  1. Derived sensors — a noisy, quantized, occasionally-broken reading of a
     variable that really is in the simulation (`TBuilding`, `T_ext`, `En_el`, …).
  2. Synthetic sensors — CO2, humidity and occupancy, which the physics model
     does not contain at all, generated from a time-of-day profile plus a
     bounded random walk.  (The file's own `PeopleNumber` is identically zero,
     so occupancy is invented rather than derived.)

Structurally this is a sibling of `engine.py` + `bridge.py`, not a layer on top
of it: its own CLI, its own MQTT client, its own pacing.  It deliberately does
*not* use `ReplayController` — sensors are an always-on stream and have no
play/pause/seek/speed semantics.  The ground truth is read through
`HDF5DataSource`, the same swappable `SimulationDataSource` the publisher uses,
so moving the project to another backend means changing one construction site
here and one there.

Usage
-----
    python sensor_simulator.py -f 20260623_baseline.hdf5 --delay 1.0

Wire contract
-------------
    sensors/<building>/<sensor_type>   {step, t, sensor_type, building,
                                        value, status, unit}
    sensors/_meta/sensors              retained discovery/provenance message

`status` is "ok" | "dropout" | "fault".  On a dropout `value` is *null* — never
omitted and never 0, because the whole point of this layer is to let the
dashboard tell "no reading" apart from "measured zero".
"""

import argparse
import json
import logging
import math
import random
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt

from datasources import HDF5DataSource, SimulationDataSource

# ---------------------------------------------------------------------------
# Logging – flush immediately so Docker / systemd capture every line
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("sensor_simulator")
sys.stdout.reconfigure(line_buffering=True)


#: pseudo-building id used for site-wide sensors (outdoor temperature, the
#: district thermostat schedule) — they are one reading, not one per building.
SITE_ID = "site"

#: the clean building id the dashboard's map/table code already extracts from
#: simulation entity names.  Reused verbatim so sensor and ground-truth data
#: key on exactly the same string.
_BUI_RE = re.compile(r"bui_\d+(?:_\d+)?")

#: heat pump "on" threshold on the part-load ratio `CR`.  Derived from the
#: reference run's distribution: CR sits above 0.5 for ~98 % of steps and the
#: 1st percentile is 0.41, so this reads as a plant that cycles off rarely.
HEAT_PUMP_ON_CR = 0.5


# ---------------------------------------------------------------------------
# Sensor definitions
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TargetRange:
    """
    What a *believable reading* of this quantity looks like, independent of what
    the simulation happens to produce.

    Every sensor type — derived and synthetic alike — is configured in exactly
    this shape, so "what range should this read in" is one table lookup rather
    than a value buried in generator code.
    """
    mean: float
    std: float
    lo: float
    hi: float


@dataclass(frozen=True)
class SensorSpec:
    """
    One sensor *type*.  Every imperfection is configured per type rather than
    globally: a thermostat that reports its own setpoint is far cleaner than a
    heat meter measuring tens of kilowatts, and a single noise knob could not
    express that.
    """
    type: str
    unit: str
    #: real-world minutes between reports; converted to source steps using the
    #: source's own dt, never hardcoded against the reference file's 600 s.
    interval_minutes: float

    #: what a plausible reading looks like — see `_calibrate`
    target: TargetRange = TargetRange(0.0, 1.0, float("-inf"), float("inf"))
    #: 'zscore' remaps position-within-distribution (offset is what's wrong),
    #: 'scale' rescales proportionally (preserves a physical zero),
    #: 'none' passes the raw value through (already dimensionless, e.g. on/off).
    calibration: str = "zscore"

    #: ground-truth lookup — None for the genuinely synthetic sensors
    source_kind: Optional[str] = None      # 'building' | 'heating' | 'weather' | 'schedule'
    source_variable: Optional[str] = None
    per_building: bool = True

    # -- per-reading imperfection ------------------------------------------
    abs_sigma: float = 0.0                 # gaussian noise, absolute units
    rel_sigma: float = 0.0                 # gaussian noise, fraction of |value|
    quantum: float = 0.1                   # reporting resolution
    drift_sigma: float = 0.0               # random-walk step of the calibration bias
    drift_limit: float = 0.0               # bias is clamped here (sensors are not unbounded)
    dropout_prob: float = 0.004
    fault_prob: float = 0.0005
    fault_len: Tuple[int, int] = (3, 12)   # consecutive samples a fault lasts
    spike: Optional[Tuple[float, float]] = None   # out-of-range value drawn from here

    # -- synthetic generation ----------------------------------------------
    synthetic: bool = False
    walk_sigma: float = 0.0                # bounded random walk on top of the target
    walk_limit: float = 0.0

    def is_due(self, step: int, every_n_steps: int) -> bool:
        return step % every_n_steps == 0


SENSOR_SPECS: List[SensorSpec] = [
    # -- derived from ground truth -----------------------------------------
    SensorSpec(
        # The reference run's TBuilding sits at 7–19 °C: correct physics for an
        # unheated period, but not what an occupied building's thermostat would
        # ever display.  Calibration keeps the shape and moves the range.
        type="indoor_temp", unit="C", interval_minutes=3,
        source_kind="building", source_variable="TBuilding",
        target=TargetRange(mean=21.0, std=1.5, lo=16.0, hi=26.0),
        abs_sigma=0.15, quantum=0.1, drift_sigma=0.004, drift_limit=0.6,
        dropout_prob=0.005, fault_prob=0.0006, spike=(45.0, 85.0),
    ),
    SensorSpec(
        type="outdoor_temp", unit="C", interval_minutes=5,
        source_kind="weather", source_variable="T_ext", per_building=False,
        target=TargetRange(mean=8.0, std=3.5, lo=-20.0, hi=40.0),
        abs_sigma=0.25, quantum=0.1, drift_sigma=0.005, drift_limit=0.8,
        dropout_prob=0.004, fault_prob=0.0006, spike=(-40.0, -25.0),
    ),
    SensorSpec(
        # 'scale', not 'zscore': electrical power has a physical zero, and a
        # meter that reads 600 W while the plant is off is exactly the kind of
        # implausible reading this calibration exists to prevent.
        type="power_meter", unit="W", interval_minutes=5,
        source_kind="heating", source_variable="En_el",
        target=TargetRange(mean=3000.0, std=1200.0, lo=0.0, hi=12000.0),
        calibration="scale",
        rel_sigma=0.01, abs_sigma=5.0, quantum=1.0,
        drift_sigma=0.6, drift_limit=60.0,
        dropout_prob=0.003, fault_prob=0.0005, spike=(60000.0, 90000.0),
    ),
    SensorSpec(
        type="heat_meter_supply", unit="W", interval_minutes=5,
        source_kind="heating", source_variable="Qt",
        target=TargetRange(mean=15000.0, std=8000.0, lo=0.0, hi=120000.0),
        calibration="scale",
        rel_sigma=0.015, abs_sigma=20.0, quantum=1.0,
        drift_sigma=2.0, drift_limit=200.0,
        dropout_prob=0.004, fault_prob=0.0006, spike=(200000.0, 400000.0),
    ),
    SensorSpec(
        type="heat_meter_return", unit="W", interval_minutes=5,
        source_kind="heating", source_variable="Qt_return",
        target=TargetRange(mean=12000.0, std=6000.0, lo=0.0, hi=100000.0),
        calibration="scale",
        rel_sigma=0.015, abs_sigma=20.0, quantum=1.0,
        drift_sigma=2.0, drift_limit=200.0,
        dropout_prob=0.004, fault_prob=0.0006, spike=(200000.0, 400000.0),
    ),
    SensorSpec(
        # A *reported* value, not a measured one: the thermostat knows its own
        # setpoint, so it gets much less noise and no calibration drift.
        type="thermostat_setpoint", unit="C", interval_minutes=10,
        source_kind="schedule", source_variable="Tset", per_building=False,
        target=TargetRange(mean=20.5, std=1.0, lo=15.0, hi=25.0),
        abs_sigma=0.02, quantum=0.1, drift_sigma=0.0,
        dropout_prob=0.002, fault_prob=0.0003, spike=(35.0, 40.0),
    ),
    SensorSpec(
        # Boolean-ish: 1 while the pump is running, 0 while it is off.  Nothing
        # to calibrate — an on/off state has no range to be unrealistic in — so
        # the target only records the bounds.  A fault spike emits 2, which is
        # out of range for a 0/1 channel and therefore unmistakable rather than
        # plausible-but-wrong.
        type="heat_pump_state", unit="bool", interval_minutes=5,
        source_kind="heating", source_variable="CR",
        target=TargetRange(mean=0.5, std=0.5, lo=0.0, hi=1.0),
        calibration="none",
        abs_sigma=0.0, quantum=1.0, drift_sigma=0.0,
        dropout_prob=0.003, fault_prob=0.0005, spike=(2.0, 2.0),
    ),

    # -- genuinely synthetic ------------------------------------------------
    # Same `target` config as the derived sensors.  These never had a raw value
    # to be unrealistic about, but expressing them in the same shape means "what
    # range does this sensor read in" is answered identically for all ten types.
    SensorSpec(
        type="co2_ppm", unit="ppm", interval_minutes=15, synthetic=True,
        target=TargetRange(mean=650.0, std=200.0, lo=400.0, hi=1600.0),
        abs_sigma=12.0, quantum=1.0, drift_sigma=0.4, drift_limit=40.0,
        walk_sigma=18.0, walk_limit=90.0,
        dropout_prob=0.006, fault_prob=0.0008, spike=(4500.0, 6000.0),
    ),
    SensorSpec(
        type="humidity_pct", unit="%", interval_minutes=15, synthetic=True,
        target=TargetRange(mean=48.0, std=10.0, lo=20.0, hi=85.0),
        abs_sigma=1.2, quantum=1.0, drift_sigma=0.05, drift_limit=4.0,
        walk_sigma=1.5, walk_limit=8.0,
        dropout_prob=0.005, fault_prob=0.0007, spike=(120.0, 140.0),
    ),
    SensorSpec(
        type="occupancy", unit="count", interval_minutes=10, synthetic=True,
        target=TargetRange(mean=2.5, std=1.8, lo=0.0, hi=8.0),
        abs_sigma=0.0, quantum=1.0,
        dropout_prob=0.004, fault_prob=0.0005, spike=(99.0, 99.0),
    ),
]

SPECS_BY_TYPE: Dict[str, SensorSpec] = {s.type: s for s in SENSOR_SPECS}


# ---------------------------------------------------------------------------
# Per-channel mutable state
# ---------------------------------------------------------------------------

@dataclass
class ChannelState:
    """
    State that persists across a channel's own samples — this is what makes the
    imperfections *sensor-like* rather than per-message randomness.  Notably the
    calibration bias accumulates over the channel's whole life and is never
    reset between reports (or between `--loop` passes: a real meter does not
    recalibrate itself because the replay wrapped).
    """
    drift: float = 0.0
    fault_remaining: int = 0
    fault_kind: Optional[str] = None       # 'stuck' | 'spike'
    fault_value: Optional[float] = None    # frozen for the whole fault episode
    last_value: Optional[float] = None
    walk: float = 0.0
    cumulative_kwh: float = 0.0
    occupancy_scale: float = 1.0           # per-building amplitude on the occupancy target
    phase: float = 0.0                     # per-building offset in the daily cycle


@dataclass
class Channel:
    building: str
    spec: SensorSpec
    every_n_steps: int
    entity: Optional[str] = None
    state: ChannelState = field(default_factory=ChannelState)
    stats: Optional["RawStats"] = None


# ---------------------------------------------------------------------------
# Calibration: raw simulation values -> believable sensor readings
# ---------------------------------------------------------------------------

#: How many steps to sample when profiling a raw series.  The source is a
#: finished static file, so this is a one-off startup cost; a full 4320-step
#: pass takes ~2.4 s and a strided one ~0.8 s, and mean/std/min/max of a smooth
#: physical series are indistinguishable between the two.  Raise it to exceed
#: the step count if an exact profile is ever wanted.
CALIBRATION_SAMPLES = 1200


@dataclass
class RawStats:
    """Observed distribution of one raw `(entity, variable)` series."""
    n: int = 0
    mean: float = 0.0
    std: float = 0.0
    lo: float = 0.0
    hi: float = 0.0

    @property
    def usable(self) -> bool:
        return self.n > 1 and self.std > 1e-12


def profile_raw_series(
    source: SimulationDataSource,
    wanted: set,
    start_step: int,
    end_step: int,
) -> Dict[Tuple[str, str], RawStats]:
    """
    One strided pass over the source, profiling only the `(entity, variable)`
    pairs the channels actually read.
    """
    span = end_step - start_step + 1
    stride = max(1, span // CALIBRATION_SAMPLES)

    acc: Dict[Tuple[str, str], List[float]] = {}
    n_samples = 0
    for step in range(start_step, end_step + 1, stride):
        values = source.read_step(step)
        n_samples += 1
        for entity, variable in wanted:
            raw = values.get(entity, {}).get(variable)
            if not isinstance(raw, (int, float)) or isinstance(raw, bool):
                continue
            raw = float(raw)
            a = acc.get((entity, variable))
            if a is None:
                acc[(entity, variable)] = [1, raw, raw * raw, raw, raw]
            else:
                a[0] += 1
                a[1] += raw
                a[2] += raw * raw
                if raw < a[3]: a[3] = raw
                if raw > a[4]: a[4] = raw

    stats = {}
    for key, (n, total, total_sq, lo, hi) in acc.items():
        mean = total / n
        variance = max(0.0, total_sq / n - mean * mean)
        stats[key] = RawStats(n=n, mean=mean, std=math.sqrt(variance), lo=lo, hi=hi)

    log.info(
        "Profiled %d raw series over %d sampled steps (stride %d) for calibration.",
        len(stats), n_samples, stride,
    )
    return stats


def calibrate(spec: SensorSpec, raw: float, stats: Optional[RawStats]) -> float:
    """
    Map a raw simulation value onto the sensor type's realistic target range,
    *preserving the shape of the signal* — the reading must still rise and fall
    with what the building is actually doing, just expressed in an absolute
    range a real instrument would report.

    `zscore` — the raw value's position within its own distribution is carried
    onto the target's. Because a pure `target.std` scaling can push a genuine
    multi-sigma excursion outside the bounds (the reference run's unheated
    period is a 5.4σ dip in `TBuilding`), the effective spread is shrunk just
    enough that the observed extremes land *on* the bounds instead of being
    clipped flat against them. Clamping remains as a backstop for values outside
    the profiled range.

    `scale` — proportional rescale, which keeps a physical zero at zero. Used
    for power and heat flow, where "off" must read as off.
    """
    if spec.calibration == "none" or stats is None or not stats.usable:
        return raw

    target = spec.target

    if spec.calibration == "scale":
        if stats.mean <= 0:
            return raw
        factor = target.mean / stats.mean
        if stats.hi > 0:
            # Keep the whole observed range inside the target's ceiling.
            factor = min(factor, target.hi / stats.hi)
        return _clamp(raw * factor, target.lo, target.hi)

    # zscore
    z = (raw - stats.mean) / stats.std
    span_z = max(
        (stats.hi - stats.mean) / stats.std,
        (stats.mean - stats.lo) / stats.std,
        1e-9,
    )
    headroom = min(target.mean - target.lo, target.hi - target.mean)
    effective_std = min(target.std, headroom / span_z) if headroom > 0 else target.std
    return _clamp(target.mean + z * effective_std, target.lo, target.hi)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

class SensorSimulator:
    """Turns a `SimulationDataSource` into a stream of imperfect sensor readings."""

    def __init__(
        self,
        source: SimulationDataSource,
        rng: random.Random,
        types: Optional[List[str]] = None,
        sim_start: Optional[datetime] = None,
        calibrate_readings: bool = True,
    ) -> None:
        self.source = source
        self.rng = rng

        time_axis = source.get_time_axis()
        self.time_axis = time_axis
        self.dt_seconds = (
            time_axis[1] - time_axis[0] if len(time_axis) >= 2 else 60.0
        )
        if self.dt_seconds <= 0:
            raise ValueError(f"Non-positive dt_seconds ({self.dt_seconds}) in the source.")

        #: real-world datetime the simulation's t=0 corresponds to.  A sensor
        #: does not know about simulation steps — it reports at a wall-clock
        #: instant — so every reading carries `sim_start + t seconds`.
        self.sim_start = sim_start or default_sim_start()

        self._entity_index = self._index_entities()
        self.buildings = sorted(
            b for (b, _kind) in self._entity_index if b != SITE_ID
        )
        self.buildings = sorted(set(self.buildings))

        wanted = set(types) if types else None
        self.channels = self._build_channels(wanted)
        self.calibrate_readings = calibrate_readings
        self._weather_stats: Optional[RawStats] = None
        if calibrate_readings:
            self._attach_calibration()

    def timestamp_for(self, t_seconds: float) -> datetime:
        return self.sim_start + timedelta(seconds=float(t_seconds))

    def _attach_calibration(self) -> None:
        """Profile each channel's raw series so `calibrate()` has a reference."""
        wanted = {
            (c.entity, c.spec.source_variable)
            for c in self.channels
            if c.entity and c.spec.source_variable and c.spec.calibration != "none"
        }
        # The synthetic humidity signal normalizes against the weather series,
        # so profile that too even if no outdoor_temp channel was requested.
        weather_entity = self._entity_index.get((SITE_ID, "weather"))
        if weather_entity:
            wanted.add((weather_entity, "T_ext"))
        if not wanted:
            return
        stats = profile_raw_series(
            self.source, wanted, 0, self.source.get_step_count() - 1
        )
        for channel in self.channels:
            channel.stats = stats.get((channel.entity, channel.spec.source_variable))
        self._weather_stats = stats.get((weather_entity, "T_ext")) if weather_entity else None

    # -- discovery ----------------------------------------------------------

    def _index_entities(self) -> Dict[Tuple[str, str], str]:
        """(building, kind) -> source entity path."""
        index: Dict[Tuple[str, str], str] = {}
        for entity in self.source.get_entities():
            if entity.startswith("building_"):
                kind = "building"
            elif entity.startswith("heating_"):
                kind = "heating"
            elif entity.startswith("weather"):
                index[(SITE_ID, "weather")] = entity
                continue
            elif entity.startswith("schedule"):
                index[(SITE_ID, "schedule")] = entity
                continue
            else:
                continue

            match = _BUI_RE.search(entity)
            if match:
                index[(match.group(0), kind)] = entity
        return index

    def steps_for(self, spec: SensorSpec) -> int:
        """Report interval in *source steps*, from the source's own dt."""
        return max(1, round(spec.interval_minutes * 60.0 / self.dt_seconds))

    def _build_channels(self, wanted: Optional[set]) -> List[Channel]:
        channels: List[Channel] = []
        for spec in SENSOR_SPECS:
            if wanted is not None and spec.type not in wanted:
                continue
            every_n = self.steps_for(spec)

            if not spec.per_building:
                entity = self._entity_index.get((SITE_ID, spec.source_kind or ""))
                if spec.source_kind and entity is None:
                    log.warning("No source entity for site sensor '%s' – skipped.", spec.type)
                    continue
                channels.append(
                    Channel(SITE_ID, spec, every_n, entity, self._new_state(SITE_ID))
                )
                continue

            for building in self.buildings:
                entity = None
                if spec.source_kind:
                    entity = self._entity_index.get((building, spec.source_kind))
                    if entity is None:
                        continue  # this building has no such subsystem
                channels.append(
                    Channel(building, spec, every_n, entity, self._new_state(building))
                )
        return channels

    def _new_state(self, building: str) -> ChannelState:
        return ChannelState(
            occupancy_scale=self.rng.uniform(0.5, 1.6),
            phase=self.rng.uniform(-0.7, 0.7),
        )

    # -- sampling -----------------------------------------------------------

    def sample_step(self, step: int) -> List[Dict[str, Any]]:
        """Every reading due at `step`, as ready-to-publish payload dicts."""
        due = [c for c in self.channels if c.spec.is_due(step, c.every_n_steps)]
        if not due:
            return []

        values = self.source.read_step(step)
        t = self.time_axis[step] if step < len(self.time_axis) else step * self.dt_seconds
        hour = (t / 3600.0) % 24.0

        readings = []
        for channel in due:
            reading = self._sample_channel(channel, step, t, hour, values)
            if reading is not None:
                readings.append(reading)
        return readings

    def _sample_channel(
        self,
        channel: Channel,
        step: int,
        t: float,
        hour: float,
        values: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        spec, state = channel.spec, channel.state

        truth = self._truth_for(channel, hour, values)
        if truth is None:
            return None

        payload: Dict[str, Any] = {
            "step": step,
            "t": t,
            # What a human-facing display should use. `step`/`t` stay for
            # correlating a reading against the ground-truth stream.
            "timestamp": self.timestamp_for(t).isoformat(),
            "sensor_type": spec.type,
            "building": channel.building,
            "value": None,
            "status": "ok",
            "unit": spec.unit,
        }

        # --- fault episodes take precedence: once broken, a sensor stays
        # --- broken for a few consecutive samples rather than flickering.
        if state.fault_remaining > 0:
            state.fault_remaining -= 1
            payload["value"] = state.fault_value
            payload["status"] = "fault"
            return payload

        if self.rng.random() < spec.fault_prob:
            # One behaviour per occurrence, never a blend of the two.
            kind = "stuck" if self.rng.random() < 0.5 else "spike"
            if kind == "stuck" and state.last_value is None:
                kind = "spike"
            if kind == "stuck":
                state.fault_value = state.last_value
            else:
                lo, hi = spec.spike if spec.spike else (truth * 10.0, truth * 10.0)
                state.fault_value = self._quantize(self.rng.uniform(lo, hi), spec.quantum)
            state.fault_kind = kind
            state.fault_remaining = self.rng.randint(*spec.fault_len) - 1
            payload["value"] = state.fault_value
            payload["status"] = "fault"
            return payload

        if self.rng.random() < spec.dropout_prob:
            # No reading happened at all. `value` stays null — explicitly not 0.
            payload["status"] = "dropout"
            return payload

        # --- a normal, slightly wrong reading ------------------------------
        if spec.drift_sigma:
            state.drift += self.rng.gauss(0.0, spec.drift_sigma)
            if spec.drift_limit:
                state.drift = max(-spec.drift_limit, min(spec.drift_limit, state.drift))

        sigma = spec.abs_sigma + spec.rel_sigma * abs(truth)
        reading = truth + state.drift + (self.rng.gauss(0.0, sigma) if sigma > 0 else 0.0)

        if spec.type == "heat_pump_state":
            reading = 1.0 if truth >= 0.5 else 0.0   # already boolean, keep it clean

        # The target bounds are the sensor's physical range, so drift and noise
        # cannot push a reading outside them either.
        reading = _clamp(reading, spec.target.lo, spec.target.hi)

        value = self._quantize(reading, spec.quantum)
        state.last_value = value
        payload["value"] = value

        if spec.type == "power_meter":
            # A real meter's register integrates what the meter itself measured,
            # bias included — so this accumulates the reported value, and a
            # dropout (no sample) simply contributes nothing.
            interval_s = channel.every_n_steps * self.dt_seconds
            state.cumulative_kwh += max(0.0, value) * interval_s / 3.6e6
            payload["cumulative"] = round(state.cumulative_kwh, 3)
            payload["cumulative_unit"] = "kWh"

        return payload

    def _truth_for(
        self,
        channel: Channel,
        hour: float,
        values: Dict[str, Dict[str, Any]],
    ) -> Optional[float]:
        """
        The value the sensor is *trying* to measure, before any imperfection.

        For derived sensors this is the raw ground truth **calibrated** into the
        type's realistic range; noise, drift, quantization, dropouts and faults
        then operate on the calibrated value exactly as before.
        """
        spec, state = channel.spec, channel.state

        if not spec.synthetic:
            variables = values.get(channel.entity or "", {})
            raw = variables.get(spec.source_variable or "")
            if raw is None or isinstance(raw, (list, dict)):
                return None
            raw = float(raw)
            if spec.type == "heat_pump_state":
                # An on/off state is derived from the raw part-load ratio before
                # any range mapping — there is no range to make realistic.
                return 1.0 if raw >= HEAT_PUMP_ON_CR else 0.0
            if not self.calibrate_readings:
                return raw
            return calibrate(spec, raw, channel.stats)

        # -- synthetic: drive the same target distribution from a normalized
        # -- signal in [0, 1], so `target` means the same thing for all types.
        target = spec.target
        occupancy_fraction = _occupancy_fraction(hour, state.phase)

        if spec.type == "occupancy":
            expected = _from_unit(occupancy_fraction, target) * state.occupancy_scale
            # People arrive in whole numbers, and not on a schedule.
            noisy = expected + self.rng.gauss(0.0, 0.45)
            return float(_clamp(round(noisy), target.lo, target.hi))

        if spec.type == "co2_ppm":
            # Outdoor baseline at an empty building, ventilation-limited
            # build-up while occupied.
            base = _from_unit(occupancy_fraction, target)
            state.walk = _bounded_walk(self.rng, state.walk, spec.walk_sigma, spec.walk_limit)
            return _clamp(base + state.walk, target.lo, target.hi)

        if spec.type == "humidity_pct":
            # Loosely anti-correlated with outdoor temperature: colder outside,
            # drier air indoors once it is heated. The outdoor reading is
            # normalized against its own profiled range, so this keeps working
            # whatever absolute range the weather data happens to use.
            t_ext = _site_value(values, self._entity_index, "weather", "T_ext")
            u = 0.5 if t_ext is None else 1.0 - _to_unit(t_ext, self._weather_stats)
            base = _from_unit(u, target)
            state.walk = _bounded_walk(self.rng, state.walk, spec.walk_sigma, spec.walk_limit)
            return _clamp(base + state.walk, target.lo, target.hi)

        return None

    @staticmethod
    def _quantize(value: float, quantum: float) -> float:
        """Round to the sensor's reporting resolution (0.1 °C, 1 ppm, whole %)."""
        if quantum <= 0:
            return float(value)
        steps = math.floor(value / quantum + 0.5)
        # Re-round: floating point makes e.g. 18.400000000000002 out of 184*0.1.
        return round(steps * quantum, 6)

    # -- provenance ---------------------------------------------------------

    def get_discovery_metadata(self, started_at: str) -> Dict[str, Any]:
        """
        What is actually being published, so the dashboard can build its
        dropdowns and table columns from the stream instead of hardcoding a
        sensor list — the same schema-free-discovery role `_meta/run` plays for
        the simulation side.
        """
        published = sorted({c.spec.type for c in self.channels})
        n_steps = self.source.get_step_count()
        return {
            "sensor_types": [
                {
                    "type": spec.type,
                    "unit": spec.unit,
                    "per_building": spec.per_building,
                    "synthetic": spec.synthetic,
                    "source": (
                        f"{spec.source_kind}/{spec.source_variable}"
                        if spec.source_variable else None
                    ),
                    "interval_minutes": spec.interval_minutes,
                    "interval_steps": self.steps_for(spec),
                    # The *effective* real-world spacing between reports: cadence
                    # is clamped to the step grid, so this is what the dashboard
                    # must space its time axis by, not `interval_minutes`.
                    "interval_seconds": self.steps_for(spec) * self.dt_seconds,
                    "resolution": spec.quantum,
                    "range": {
                        "mean": spec.target.mean,
                        "std": spec.target.std,
                        "lo": spec.target.lo,
                        "hi": spec.target.hi,
                    },
                    # Synthetic types are generated straight into `range`; there
                    # is no raw value under them to calibrate.
                    "calibration": "generated" if spec.synthetic else spec.calibration,
                }
                for spec in SENSOR_SPECS if spec.type in published
            ],
            "buildings": list(self.buildings),
            "site_id": SITE_ID,
            "n_channels": len(self.channels),
            "dt_seconds": self.dt_seconds,
            # Simulated wall-clock: what t=0 means and where the run ends.
            "sim_start": self.sim_start.isoformat(),
            "sim_end": self.timestamp_for(
                self.time_axis[n_steps - 1] if n_steps else 0.0
            ).isoformat(),
            "calibrated": self.calibrate_readings,
            # Wall-clock instant this process started, distinct from sim_start.
            "started_at": started_at,
            "ground_truth": self.source.get_run_metadata(),
        }


# ---------------------------------------------------------------------------
# Synthetic-signal helpers
# ---------------------------------------------------------------------------

def default_sim_start() -> datetime:
    """Today at local midnight, timezone-aware so the ISO string is unambiguous."""
    now = datetime.now().astimezone()
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_sim_start(raw: str) -> datetime:
    """Parse `--sim-start`; a naive value is interpreted as local time."""
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return parsed


def _occupancy_fraction(hour: float, phase: float) -> float:
    """
    A plausible residential day: mostly empty during working hours, a morning
    peak and a long evening peak.  `phase` shifts each building slightly so the
    district does not breathe in perfect unison.
    """
    h = (hour + phase) % 24.0
    morning = math.exp(-((h - 7.5) ** 2) / 2.0)
    evening = math.exp(-((h - 20.0) ** 2) / 6.0)
    night = 0.35 * math.exp(-((h - 2.0) ** 2) / 8.0)
    return min(1.0, morning + evening + night)


def _bounded_walk(rng: random.Random, current: float, sigma: float, limit: float) -> float:
    """Random walk that is pulled back toward zero, so it never runs away."""
    if sigma <= 0:
        return current
    nxt = current * 0.95 + rng.gauss(0.0, sigma)
    if limit <= 0:
        return nxt
    return max(-limit, min(limit, nxt))


def _from_unit(u: float, target: TargetRange) -> float:
    """[0, 1] -> the target distribution: 0 reads mean-std, 1 reads mean+std."""
    return target.mean + (2.0 * _clamp(u, 0.0, 1.0) - 1.0) * target.std


def _to_unit(value: float, stats: Optional[RawStats]) -> float:
    """A raw value's position within its own observed range, as [0, 1]."""
    if stats is None or stats.hi <= stats.lo:
        return 0.5
    return _clamp((value - stats.lo) / (stats.hi - stats.lo), 0.0, 1.0)


def _site_value(
    values: Dict[str, Dict[str, Any]],
    entity_index: Dict[Tuple[str, str], str],
    kind: str,
    variable: str,
) -> Optional[float]:
    entity = entity_index.get((SITE_ID, kind))
    if entity is None:
        return None
    raw = values.get(entity, {}).get(variable)
    return float(raw) if isinstance(raw, (int, float)) else None


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

_MQTT_CONNECTED = False
_MQTT_CONNECT_ERROR: Optional[str] = None


def _on_connect(client, userdata, flags, reason_code, properties=None) -> None:
    global _MQTT_CONNECTED, _MQTT_CONNECT_ERROR
    if reason_code == 0:
        _MQTT_CONNECTED = True
        log.info("Connected to MQTT broker.")
    else:
        _MQTT_CONNECT_ERROR = f"Connection refused – reason code {reason_code}"
        log.error(_MQTT_CONNECT_ERROR)


def _on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None) -> None:
    global _MQTT_CONNECTED
    _MQTT_CONNECTED = False
    if reason_code != 0:
        log.warning("Unexpected disconnect (reason code %s). Will attempt reconnect…", reason_code)


def build_mqtt_client(client_id: str = "sensor_simulator") -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=True,
    )
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    return client


def connect_broker(client: mqtt.Client, host: str, port: int, timeout: float = 10.0) -> None:
    global _MQTT_CONNECTED, _MQTT_CONNECT_ERROR
    _MQTT_CONNECTED = False
    _MQTT_CONNECT_ERROR = None

    log.info("Connecting to MQTT broker at %s:%d …", host, port)
    client.connect(host, port, keepalive=60)
    client.loop_start()

    deadline = time.monotonic() + timeout
    while not _MQTT_CONNECTED and _MQTT_CONNECT_ERROR is None:
        if time.monotonic() > deadline:
            client.loop_stop()
            raise RuntimeError(f"Timed out connecting to broker {host}:{port} after {timeout}s.")
        time.sleep(0.1)

    if _MQTT_CONNECT_ERROR:
        client.loop_stop()
        raise RuntimeError(_MQTT_CONNECT_ERROR)


# ---------------------------------------------------------------------------
# Publish loop
# ---------------------------------------------------------------------------

def publish_discovery(
    client: mqtt.Client,
    base_topic: str,
    simulator: SensorSimulator,
    started_at: str,
) -> Dict[str, Any]:
    metadata = simulator.get_discovery_metadata(started_at)
    topic = f"{base_topic}/_meta/sensors"
    # QoS 1 + retained, like sim/coesi5/_meta/run: a dashboard connecting
    # mid-stream must learn the sensor list immediately, not on the next report.
    client.publish(topic, json.dumps(metadata, allow_nan=False), qos=1, retain=True)
    log.info(
        "Published sensor discovery (retained) to '%s': %d types, %d buildings, %d channels",
        topic, len(metadata["sensor_types"]), len(metadata["buildings"]), metadata["n_channels"],
    )
    return metadata


def run_simulator(
    client: mqtt.Client,
    base_topic: str,
    simulator: SensorSimulator,
    delay: float,
    start_step: int,
    end_step: int,
    loop: bool,
    qos: int = 0,
) -> None:
    started_at = datetime.now(timezone.utc).isoformat()
    publish_discovery(client, base_topic, simulator, started_at)

    log.info(
        "Streaming sensors over steps %d–%d (delay=%.3fs, dt=%.0fs, loop=%s)",
        start_step, end_step, delay, simulator.dt_seconds, loop,
    )

    counts = {"ok": 0, "dropout": 0, "fault": 0}
    step = start_step
    while True:
        if step > end_step:
            if not loop:
                break
            step = start_step
            continue

        readings = simulator.sample_step(step)
        for payload in readings:
            topic = f"{base_topic}/{payload['building']}/{payload['sensor_type']}"
            # allow_nan=False for the same reason as the main publisher: a NaN
            # must fail loudly, not become a payload the browser silently drops.
            client.publish(topic, json.dumps(payload, allow_nan=False), qos=qos)
            counts[payload["status"]] += 1

        if readings:
            log.info(
                "Step %d: %d readings (run totals ok=%d dropout=%d fault=%d)",
                step, len(readings), counts["ok"], counts["dropout"], counts["fault"],
            )

        time.sleep(delay)
        step += 1

    log.info("Sensor stream finished: ok=%d dropout=%d fault=%d",
             counts["ok"], counts["dropout"], counts["fault"])


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sensor_simulator",
        description=(
            "Publishes synthetic, imperfect sensor readings derived from (and "
            "alongside) an HDF5 district simulation."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # Not `required=True` only so `--list-types` can run without a file; main()
    # enforces it for every path that actually reads data.
    parser.add_argument("--file", "-f", metavar="PATH",
                        help="Path to the HDF5 file holding the ground truth.")

    parser.add_argument("--host", default="localhost", metavar="HOST",
                        help="MQTT broker hostname or IP address.")
    parser.add_argument("--port", type=int, default=1883, metavar="PORT",
                        help="MQTT broker port.")
    parser.add_argument("--topic", "-t", default="sensors", metavar="TOPIC",
                        help="Base topic; readings go to <topic>/<building>/<sensor_type>.")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=0, metavar="QOS",
                        help="MQTT Quality of Service for readings.")
    parser.add_argument("--client-id", default="sensor_simulator", metavar="ID",
                        help="MQTT client identifier.")

    parser.add_argument("--delay", type=float, default=1.0, metavar="SECONDS",
                        help="Wall-clock delay between source steps.")
    parser.add_argument("--start-step", type=int, default=0, metavar="STEP",
                        help="First source step to sample.")
    parser.add_argument("--end-step", type=int, default=None, metavar="STEP",
                        help="Last source step to sample (default: the last step in the file).")

    # Looping is the default: a sensor stream that silently stops after one pass
    # leaves the dashboard with nothing to receive (readings are QoS 0 and
    # unretained) — the same failure mode `build.sh` had.
    parser.add_argument("--loop", dest="loop", action="store_true", default=True,
                        help="Restart from --start-step after --end-step (default).")
    parser.add_argument("--no-loop", dest="loop", action="store_false",
                        help="Stop after one pass instead of looping.")

    parser.add_argument("--sim-start", default=None, metavar="ISO8601",
                        help="Real-world datetime that the simulation's t=0 represents "
                             "(default: today at local midnight). Each reading's "
                             "'timestamp' is this plus its t.")
    parser.add_argument("--no-calibration", dest="calibrate", action="store_false",
                        default=True,
                        help="Publish raw source values instead of remapping them into "
                             "each sensor type's realistic range.")

    parser.add_argument("--seed", type=int, default=None, metavar="N",
                        help="Seed the noise/fault RNG for a reproducible run.")
    parser.add_argument("--types", default=None, metavar="LIST",
                        help="Comma-separated sensor types to publish (default: all).")
    parser.add_argument("--list-types", action="store_true",
                        help="Print the known sensor types and exit.")

    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    if args.list_types:
        print("\nKnown sensor types:")
        print(f"  {'type':22s} {'unit':6s} {'scope':13s} {'interval':10s} {'target range':24s} source")
        for spec in SENSOR_SPECS:
            origin = "synthetic" if spec.synthetic else f"{spec.source_kind}/{spec.source_variable}"
            scope = "per-building" if spec.per_building else "site-wide"
            rng_txt = f"{spec.target.mean:g}±{spec.target.std:g} [{spec.target.lo:g},{spec.target.hi:g}]"
            print(f"  {spec.type:22s} {spec.unit:6s} {scope:13s} ~{spec.interval_minutes:<9g} {rng_txt:24s} {origin}")
        print()
        return 0

    if not args.file:
        log.error("--file/-f is required.")
        return 1

    types = [t.strip() for t in args.types.split(",")] if args.types else None
    if types:
        unknown = [t for t in types if t not in SPECS_BY_TYPE]
        if unknown:
            log.error("Unknown sensor type(s): %s", ", ".join(unknown))
            return 1

    try:
        source = HDF5DataSource(args.file)
    except OSError as exc:
        log.error("Failed to read file: %s", exc)
        return 1

    n_steps = source.get_step_count()
    if n_steps <= 0:
        log.error("No valid 1D time-series datasets found in the HDF5 file.")
        return 1

    try:
        sim_start = parse_sim_start(args.sim_start) if args.sim_start else default_sim_start()
    except ValueError as exc:
        log.error("Could not parse --sim-start %r: %s", args.sim_start, exc)
        return 1

    rng = random.Random(args.seed)
    simulator = SensorSimulator(
        source, rng, types, sim_start=sim_start, calibrate_readings=args.calibrate
    )
    log.info(
        "Simulated clock: t=0 is %s (dt=%.0fs, calibration=%s)",
        simulator.sim_start.isoformat(), simulator.dt_seconds,
        "on" if args.calibrate else "off",
    )
    if not simulator.channels:
        log.error("No sensor channels could be built from this source.")
        return 1

    start_step = max(0, args.start_step)
    end_step = n_steps - 1 if args.end_step is None else min(args.end_step, n_steps - 1)
    if end_step < start_step:
        log.error("Empty step range: --start-step %d > --end-step %d", start_step, end_step)
        return 1

    client = build_mqtt_client(client_id=args.client_id)
    try:
        connect_broker(client, args.host, args.port)
    except (RuntimeError, OSError) as exc:
        log.error("Cannot connect to broker: %s", exc)
        return 1

    try:
        run_simulator(
            client=client,
            base_topic=args.topic,
            simulator=simulator,
            delay=args.delay,
            start_step=start_step,
            end_step=end_step,
            loop=args.loop,
            qos=args.qos,
        )
    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C). Shutting down gracefully…")
    finally:
        client.loop_stop()
        client.disconnect()
        log.info("MQTT client disconnected. Bye.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
