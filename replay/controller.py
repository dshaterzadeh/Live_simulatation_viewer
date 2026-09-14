"""
replay/controller.py
====================
Owns *when* a step is replayed — nothing about where the data comes from,
nothing about how it is published, and nothing about how commands arrive.

`ReplayController.steps()` is a generator yielding `(step_index, {entity: {var:
value}})` at the configured pace.  Playback state (playing/paused, speed
multiplier) is mutated through `play()`, `pause()` and `set_speed()` by whoever
owns the transport — today the HELICS engine federate — and is re-evaluated at
the top of every loop iteration rather than blocking unconditionally on
`time.sleep`.

The optional `on_tick` callback is invoked from inside every wait slice
(`_TICK`, 50 ms), whether the replay is playing or paused.  It exists so a
caller that has to service something continuously — advancing a HELICS
federation's clock and draining its endpoint — can do so without the
controller knowing anything about HELICS, and without pacing leaking out of
this module.
"""

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, Optional, Tuple

from datasources.base import SimulationDataSource

log = logging.getLogger("hdf5_mqtt_publisher")

#: granularity at which a long delay is broken up so control changes are
#: still acted on promptly while paused or between steps
_TICK = 0.05


class ReplayController:
    def __init__(
        self,
        source: SimulationDataSource,
        delay: float = 1.0,
        start_step: int = 0,
        end_step: Optional[int] = None,
        loop: bool = False,
        on_tick: Optional[Callable[[], None]] = None,
    ) -> None:
        self.source = source
        self.delay = delay
        self.start_step = max(0, start_step)
        last_step = source.get_step_count() - 1
        self.end_step = last_step if end_step is None else min(end_step, last_step)
        self.loop = loop
        self.on_tick = on_tick

        self._state_lock = threading.Lock()
        self._paused = False
        self._speed = 1.0
        self._wake = threading.Event()

        self.current_step = self.start_step
        self.started_at: Optional[str] = None

    # -- playback state -----------------------------------------------------

    @property
    def paused(self) -> bool:
        with self._state_lock:
            return self._paused

    @property
    def speed(self) -> float:
        with self._state_lock:
            return self._speed

    def play(self) -> None:
        with self._state_lock:
            self._paused = False
        self._wake.set()

    def pause(self) -> None:
        with self._state_lock:
            self._paused = True
        self._wake.set()

    def set_speed(self, multiplier: float) -> None:
        multiplier = float(multiplier)
        if multiplier <= 0:
            log.warning("Ignoring non-positive speed multiplier %s", multiplier)
            return
        with self._state_lock:
            self._speed = multiplier
        self._wake.set()

    # -- the paced loop -----------------------------------------------------

    def steps(self) -> Iterator[Tuple[int, Dict[str, Dict[str, Any]]]]:
        """Generator yielding (step_index, {entity: {var: value}}) at the configured pace."""
        if self.source.get_step_count() <= 0 or self.end_step < self.start_step:
            return

        if self.started_at is None:
            self.started_at = datetime.now(timezone.utc).isoformat()

        step = self.start_step
        while True:
            with self._state_lock:
                paused = self._paused

            if paused:
                self._wait(_TICK)
                continue

            if step > self.end_step:
                if not self.loop:
                    break
                step = self.start_step
                continue

            self.current_step = step
            yield step, self.source.read_step(step)

            is_last = step >= self.end_step and not self.loop
            if not is_last:
                self._wait(self.delay / self.speed)
            step += 1

    def _wait(self, duration: float) -> None:
        """Sleep in `_TICK` slices, servicing `on_tick` in each, and return early
        when play/pause/speed changes the state."""
        deadline = time.monotonic() + duration
        while True:
            if self.on_tick is not None:
                self.on_tick()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            if self._wake.wait(min(remaining, _TICK)):
                self._wake.clear()
                return

    # -- provenance ---------------------------------------------------------

    def get_run_metadata(self) -> Dict[str, Any]:
        """Provenance for this run: the source's own metadata + the replay config."""
        time_axis = self.source.get_time_axis()
        dt_seconds = None
        if len(time_axis) >= 2:
            dt_seconds = time_axis[1] - time_axis[0]

        if self.started_at is None:
            self.started_at = datetime.now(timezone.utc).isoformat()

        metadata = dict(self.source.get_run_metadata())
        metadata.update(
            {
                "n_steps": self.source.get_step_count(),
                "dt_seconds": dt_seconds,
                "start_step": self.start_step,
                "end_step": self.end_step,
                "delay": self.delay,
                "loop": self.loop,
                "started_at": self.started_at,
            }
        )
        return metadata
