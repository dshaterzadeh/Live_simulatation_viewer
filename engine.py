#!/usr/bin/env python3
"""
engine.py
=========
The simulation-engine federate.  Stand-in for the real HELICS-based engine:
it replays an HDF5 file through `ReplayController` and publishes each step
into the federation, and it accepts control (play/pause/speed/setpoint) from
the bridge over a HELICS endpoint.

What a real engine replaces: this whole file.  What it keeps: the interface in
`federation.py`.  `bridge.py`, `sensor_simulator.py` and the dashboard do not
change.

Usage
-----
    python engine.py --file 20260623_baseline.hdf5 --delay 1.0 --loop \
        --helics-broker tcp://helics-broker:23404
"""

import argparse
import json
import logging
import math
import sys
from typing import Any, Callable, Dict, List, Optional, Tuple

import helics as h

from datasources import HDF5DataSource
from datasources.hdf5_source import explore_hdf5
from federation import (
    DEFAULT_BROKER, DEFAULT_CORE, EP_BRIDGE, EP_ENGINE, PUB_RUN, PUB_STEP, TIME_DELTA,
    create_federate, finalize, request_time,
)
from replay import ReplayController

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
    force=True,
)
log = logging.getLogger("hdf5_mqtt_publisher")
sys.stdout.reconfigure(line_buffering=True)


# ---------------------------------------------------------------------------
# Control seam
# ---------------------------------------------------------------------------
# THIS IS THE SEAM THE REAL ENGINE'S PHYSICS REPLACES.  A setpoint from the
# dashboard has to visibly change what is published next, so the stand-in does
# two legible things: it substitutes the commanded value for the recorded one,
# and it moves a coupled variable of the same entity in a way a reader can
# predict.  None of it is physically accurate and none of it is meant to be.

#: variable -> (lower, upper): a command outside this is clamped, and the ack
#: says so.  Anything not listed is accepted as-is if it is a finite number.
SETPOINT_LIMITS: Dict[str, Tuple[float, float]] = {
    "ZoneSetPoint": (10.0, 30.0),   # °C — a thermostat's dial
    "Qt": (0.0, 1.0e5),             # W  — heat-pump thermal output; 0 = standby
}

#: variable -> {coupled variable of the same entity: f(coupled_recorded, recorded, commanded)}
COUPLINGS: Dict[str, Dict[str, Callable[[float, float, float], float]]] = {
    # The room closes 60 % of the gap between where it is and the commanded
    # setpoint — a heating system that responds but never quite gets there.
    # (Not "60 % of the setpoint *change*": the recorded setpoint is 0 for
    # long stretches of this file, which would turn a 26 °C command into a
    # +15 °C jump.)
    "ZoneSetPoint": {"TBuilding": lambda t, rec, cmd: t + 0.6 * (cmd - t)},
    # Electrical input scales with thermal output; commanding 0 W parks the
    # pump in standby.  COP is left alone on purpose — it is a property of the
    # machine, not of what it is asked to do.
    "Qt": {"En_el": lambda e, rec, cmd: (e * cmd / rec) if rec else e},
}


def apply_control(entity: str, variable: str, hdf5_value: Any, override: Optional[float]) -> Any:
    """The commanded value replaces the recorded one while an override is set."""
    return hdf5_value if override is None else override


def apply_overrides(step_values: Dict[str, Dict[str, Any]],
                    overrides: Dict[Tuple[str, str], float]) -> None:
    """Apply every active override to one step's values, in place."""
    for (entity, variable), cmd in overrides.items():
        variables = step_values.get(entity)
        if not variables or variable not in variables:
            continue
        recorded = variables[variable]
        variables[variable] = apply_control(entity, variable, recorded, cmd)
        for coupled, fn in COUPLINGS.get(variable, {}).items():
            if coupled in variables and isinstance(variables[coupled], (int, float)) \
                    and isinstance(recorded, (int, float)):
                variables[coupled] = fn(variables[coupled], recorded, cmd)


# ---------------------------------------------------------------------------
# The federate
# ---------------------------------------------------------------------------

class Engine:
    def __init__(self, source: HDF5DataSource, controller: ReplayController, fed: Any) -> None:
        self.source = source
        self.controller = controller
        self.fed = fed
        self.pub_run = h.helicsFederateRegisterGlobalPublication(fed, PUB_RUN, h.HELICS_DATA_TYPE_STRING, "")
        self.pub_step = h.helicsFederateRegisterGlobalPublication(fed, PUB_STEP, h.HELICS_DATA_TYPE_STRING, "")
        self.endpoint = h.helicsFederateRegisterGlobalEndpoint(fed, EP_ENGINE, "")
        self.time = 0.0
        self.overrides: Dict[Tuple[str, str], float] = {}
        controller.on_tick = self.tick

    # -- federation clock ------------------------------------------------------

    def tick(self) -> None:
        """Advance the federation by one slice and act on anything the bridge sent.
        Called from the controller's wait loop, playing or paused."""
        self.time = request_time(self.fed, self.time + TIME_DELTA, "engine")
        while h.helicsEndpointHasMessage(self.endpoint):
            msg = h.helicsEndpointGetMessage(self.endpoint)
            self._dispatch(h.helicsMessageGetString(msg))

    # -- inbound control -------------------------------------------------------

    def _dispatch(self, raw: str) -> None:
        try:
            cmd = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Control: undecodable message %r", raw)
            return
        if not isinstance(cmd, dict):
            return
        command = cmd.get("command")
        if command == "play":
            self.controller.play()
            log.info("Control: play (from step %d)", self.controller.current_step)
            self._send_state()
        elif command == "pause":
            self.controller.pause()
            log.info("Control: pause (at step %d)", self.controller.current_step)
            self._send_state()
        elif command == "speed":
            if "multiplier" in cmd:
                self.controller.set_speed(cmd["multiplier"])
                log.info("Control: speed x%s", cmd["multiplier"])
                self._send_state()
            else:
                log.warning("Control: speed without a 'multiplier' field: %r", raw)
        elif command == "setpoint":
            self._setpoint(cmd)
        else:
            log.warning("Control: unknown command %r", command)

    def _send_state(self) -> None:
        """Transport state as the engine holds it — what the dashboard renders,
        instead of guessing from its own last click."""
        state = {
            "command": "state",
            "playing": not self.controller.paused,
            "speed": self.controller.speed,
            "step": self.controller.current_step,
        }
        h.helicsEndpointSendBytesTo(self.endpoint, json.dumps(state).encode(), EP_BRIDGE)

    def _setpoint(self, cmd: Dict[str, Any]) -> None:
        entity, variable = cmd.get("entity"), cmd.get("variable")
        requested = cmd.get("value")
        ack: Dict[str, Any] = {
            "command": "setpoint_ack", "entity": entity, "variable": variable,
            "requested": requested, "applied": None, "accepted": False, "reason": "",
            "step": self.controller.current_step,
        }
        known = self.source.get_entities()
        if entity not in known or variable not in known[entity]:
            ack["reason"] = "unknown entity/variable"
        elif requested is None:
            self.overrides.pop((entity, variable), None)
            ack.update(accepted=True, reason="override cleared")
        elif not isinstance(requested, (int, float)) or isinstance(requested, bool) \
                or not math.isfinite(requested):
            ack["reason"] = "value must be a finite number"
        else:
            applied = float(requested)
            lo, hi = SETPOINT_LIMITS.get(variable, (-math.inf, math.inf))
            if applied < lo or applied > hi:
                applied = min(max(applied, lo), hi)
                ack["reason"] = f"clamped to [{lo:g}, {hi:g}]"
            self.overrides[(entity, variable)] = applied
            ack.update(applied=applied, accepted=True)
        log.info("Control: setpoint %s/%s = %r -> %s", entity, variable, requested,
                 "applied %r" % ack["applied"] if ack["accepted"] else "rejected (%s)" % ack["reason"])
        h.helicsEndpointSendBytesTo(self.endpoint, json.dumps(ack, allow_nan=False).encode(), EP_BRIDGE)

    # -- the run ---------------------------------------------------------------

    def run(self) -> int:
        entities = self.source.get_entities()
        catalog = {e: {v: self.source.get_attributes(e, v) for v in vs} for e, vs in entities.items()}
        n_datasets = sum(len(v) for v in entities.values())
        if not n_datasets:
            log.error("No datasets to publish.")
            return 1

        log.info("Entering federation …")
        h.helicsFederateEnterExecutingMode(self.fed)
        h.helicsPublicationPublishString(
            self.pub_run,
            json.dumps({"metadata": self.controller.get_run_metadata(), "catalog": catalog}, allow_nan=False),
        )
        log.info("Published run metadata + catalog (%d datasets) to '%s'", n_datasets, PUB_RUN)
        self._send_state()

        n_steps = self.source.get_step_count()
        log.info(
            "Starting replay: %d time steps across %d datasets (delay=%.3fs)",
            self.controller.end_step - self.controller.start_step + 1, n_datasets, self.controller.delay,
        )
        published = 0
        for step, step_values in self.controller.steps():
            apply_overrides(step_values, self.overrides)
            h.helicsPublicationPublishString(
                self.pub_step, json.dumps({"step": step, "values": step_values}, allow_nan=False)
            )
            published += 1
            log.info("Published step %d / %d  (%.1f%%) at t=%.2f%s",
                     step + 1, n_steps, 100.0 * (step + 1) / n_steps, self.time,
                     f"  [{len(self.overrides)} override(s)]" if self.overrides else "")
        # Tell the bridge we are leaving, then one more grant so it is
        # guaranteed to see both the final step and the farewell before this
        # federate is gone.
        h.helicsEndpointSendBytesTo(self.endpoint, json.dumps({"command": "bye"}).encode(), EP_BRIDGE)
        self.tick()
        log.info("All %d steps published.", published)
        return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="engine",
        description="Replays an HDF5 simulation file into a HELICS federation, step by step.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--file", "-f", required=True, metavar="PATH", help="Path to the HDF5 file.")
    parser.add_argument("--explore", action="store_true", help="Print all datasets in the HDF5 file and exit.")
    parser.add_argument("--helics-broker", default=DEFAULT_BROKER, metavar="ADDR", help="HELICS broker address.")
    parser.add_argument("--helics-core", default=DEFAULT_CORE, metavar="TYPE", help="HELICS core type.")
    parser.add_argument("--delay", type=float, default=1.0, metavar="SECONDS",
                        help="Baseline delay in seconds between published time steps.")
    parser.add_argument("--start-step", type=int, default=0, metavar="STEP", help="First step to replay.")
    parser.add_argument("--end-step", type=int, default=None, metavar="STEP",
                        help="Last step to replay (default: the last step in the file).")
    parser.add_argument("--loop", action="store_true",
                        help="Restart from --start-step after --end-step instead of terminating.")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.explore:
        explore_hdf5(args.file)
        return 0

    try:
        source = HDF5DataSource(args.file)
    except OSError as exc:
        log.error("Failed to read file: %s", exc)
        return 1
    if source.get_step_count() <= 0:
        log.error("No valid 1D time-series datasets found in the HDF5 file.")
        return 1

    controller = ReplayController(
        source=source, delay=args.delay, start_step=args.start_step,
        end_step=args.end_step, loop=args.loop,
    )
    fed = create_federate("engine", args.helics_broker, args.helics_core)
    engine = Engine(source, controller, fed)
    try:
        return engine.run()
    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C). Shutting down gracefully…")
        return 0
    finally:
        finalize(fed)
        log.info("Left the federation. Bye.")


if __name__ == "__main__":
    sys.exit(main())
