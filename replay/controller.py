"""
replay/controller.py
====================
Owns *when* a step is replayed — nothing about where the data comes from and
nothing about how it is published.

`ReplayController.steps()` is a generator yielding `(step_index, {entity: {var:
value}})` at the configured pace.  Playback state (playing/paused, seek target,
speed multiplier) is mutated from an optional MQTT control listener subscribed
to `<base_topic>/_control/#`, and is re-evaluated at the top of every loop
iteration rather than blocking unconditionally on `time.sleep`.
"""

import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional, Tuple

import paho.mqtt.client as mqtt

from datasources.base import SimulationDataSource

log = logging.getLogger("hdf5_mqtt_publisher")

#: granularity at which a long delay is broken up so control messages are
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
    ) -> None:
        self.source = source
        self.delay = delay
        self.start_step = max(0, start_step)
        last_step = source.get_step_count() - 1
        self.end_step = last_step if end_step is None else min(end_step, last_step)
        self.loop = loop

        self._state_lock = threading.Lock()
        self._paused = False
        self._speed = 1.0
        self._seek_target: Optional[int] = None
        self._wake = threading.Event()

        self.current_step = self.start_step
        self.started_at: Optional[str] = None

        self._control_client: Optional[mqtt.Client] = None

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

    def seek(self, step: int) -> None:
        target = max(self.start_step, min(int(step), self.end_step))
        with self._state_lock:
            self._seek_target = target
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
                target, self._seek_target = self._seek_target, None
                paused = self._paused
            if target is not None:
                step = target

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
        """Sleep, but return early when a control message changes the state."""
        deadline = time.monotonic() + duration
        while True:
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

    # -- control listener ---------------------------------------------------

    def start_control_listener(
        self,
        host: str,
        port: int,
        base_topic: str,
        client_id: str = "replay_control",
        timeout: float = 10.0,
    ) -> None:
        """Subscribe to `<base_topic>/_control/#` on a connection of our own."""
        topic = f"{base_topic}/_control/#"
        connected = threading.Event()

        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code == 0:
                client.subscribe(topic)
                connected.set()
            else:
                log.error("Control listener connection refused – reason code %s", reason_code)

        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=True,
        )
        client.on_connect = on_connect
        client.on_message = self._on_control_message
        client.connect(host, port, keepalive=60)
        client.loop_start()

        if not connected.wait(timeout):
            client.loop_stop()
            client.disconnect()
            raise RuntimeError(
                f"Timed out subscribing the control listener to '{topic}'."
            )

        self._control_client = client
        log.info("Control listener subscribed to '%s'", topic)

    def stop_control_listener(self) -> None:
        if self._control_client is None:
            return
        try:
            self._control_client.loop_stop()
            self._control_client.disconnect()
        except Exception as exc:  # pragma: no cover – shutdown best effort
            log.warning("Error stopping control listener: %s", exc)
        self._control_client = None

    def _on_control_message(self, client, userdata, message) -> None:
        command = message.topic.rsplit("/", 1)[-1]
        raw = message.payload.decode("utf-8", errors="replace").strip()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        if command == "play":
            self.play()
            log.info("Control: play (from step %d)", self.current_step)
        elif command == "pause":
            self.pause()
            log.info("Control: pause (at step %d)", self.current_step)
        elif command == "seek":
            if "step" in payload:
                self.seek(payload["step"])
                log.info("Control: seek -> step %s", payload["step"])
            else:
                log.warning("Control: seek without a 'step' field: %r", raw)
        elif command == "speed":
            if "multiplier" in payload:
                self.set_speed(payload["multiplier"])
                log.info("Control: speed x%s", payload["multiplier"])
            else:
                log.warning("Control: speed without a 'multiplier' field: %r", raw)
        else:
            log.warning("Control: unknown command '%s'", command)
