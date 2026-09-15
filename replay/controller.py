"""
replay/controller.py
====================
Owns *which* steps are replayed, in what order, and the run's provenance —
nothing about where the data comes from, how it is published, or how fast.

Pacing is no longer here.  Since the HELICS split the federation's clock is
simulation time and the *broker* paces it with a time barrier
(`helics_broker.py`): play, pause, step and pace hold every federate back at
its next time request, and the engine simply asks for the next step's time
and publishes when it is granted.  A real engine behaves the same way, which
is the point.

`ReplayController.steps()` therefore yields `(step_index, {entity: {var:
value}})` as fast as the consumer asks, wrapping to `start_step` after
`end_step` when `loop` is set and counting the pass it is on.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, Optional, Tuple

from datasources.base import SimulationDataSource

log = logging.getLogger("hdf5_mqtt_publisher")


class ReplayController:
    def __init__(
        self,
        source: SimulationDataSource,
        start_step: int = 0,
        end_step: Optional[int] = None,
        loop: bool = False,
        sim_start: Optional[datetime] = None,
    ) -> None:
        self.source = source
        self.start_step = max(0, start_step)
        last_step = source.get_step_count() - 1
        self.end_step = last_step if end_step is None else min(end_step, last_step)
        self.loop = loop
        # What simulation time 0 means as a calendar instant.  The file has no
        # opinion; the default matches sensor_simulator.py's so both streams
        # agree on the date.
        self.sim_start = sim_start or datetime.now().astimezone().replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        self.current_step = self.start_step
        self.current_pass = 0
        self.started_at: Optional[str] = None

    # -- the step iterator --------------------------------------------------

    def steps(self) -> Iterator[Tuple[int, Dict[str, Dict[str, Any]]]]:
        """Generator yielding (step_index, {entity: {var: value}}) in replay order."""
        if self.source.get_step_count() <= 0 or self.end_step < self.start_step:
            return

        if self.started_at is None:
            self.started_at = datetime.now(timezone.utc).isoformat()

        step = self.start_step
        while True:
            if step > self.end_step:
                if not self.loop:
                    break
                step = self.start_step
                self.current_pass += 1
                log.info("Replay wrapped: starting pass %d", self.current_pass + 1)
            self.current_step = step
            yield step, self.source.read_step(step)
            step += 1

    # -- provenance ---------------------------------------------------------

    @property
    def dt_seconds(self) -> Optional[float]:
        time_axis = self.source.get_time_axis()
        return (time_axis[1] - time_axis[0]) if len(time_axis) >= 2 else None

    def sim_time(self, step: int) -> Optional[datetime]:
        """Calendar instant of a step: sim_start + step * dt."""
        dt = self.dt_seconds
        return None if dt is None else self.sim_start + timedelta(seconds=step * dt)

    def get_run_metadata(self) -> Dict[str, Any]:
        """Provenance for this run: the source's own metadata + the replay config."""
        if self.started_at is None:
            self.started_at = datetime.now(timezone.utc).isoformat()

        n_steps = self.source.get_step_count()
        end = self.sim_time(n_steps)
        metadata = dict(self.source.get_run_metadata())
        metadata.update(
            {
                "n_steps": n_steps,
                "dt_seconds": self.dt_seconds,
                "start_step": self.start_step,
                "end_step": self.end_step,
                "loop": self.loop,
                "start_time": self.sim_start.isoformat(),
                "end_time": end.isoformat() if end else None,
                "started_at": self.started_at,
            }
        )
        return metadata
