#!/usr/bin/env python3
"""
helics_broker.py
================
The HELICS broker *and* the run controller.

The broker is the one party that can hold a whole federation back without
any model federate knowing: a HELICS **time barrier** stops every federate
at its next time request, and moving the barrier forward on a wall-clock
schedule paces them.  So play / pause / step / pace live here, driven over
MQTT from the dashboard, and the engine and bridge never see a transport
command at all.  That is exactly what makes the same controls work for a real
engine (CosimGym-style federates that run as fast as they compute).

Commands, on <topic>/_control/…  (payloads are JSON; empty = {}):

    play                     resume advancing the barrier at the current pace
    pause                    freeze the barrier at the federates' current time
    step                     while paused: allow exactly one more period
    pace   {"sim_seconds_per_second": 600}   0 = free-run (no barrier)

State, retained on <topic>/_meta/state:

    {"mode": "paced" | "free" | "paused", "pace": 600.0,
     "allowed_sim_time": 3000.0, "period": 600.0, "at": "…"}

The process also writes a readiness sentinel from `helicsBrokerIsConnected()`
for the compose health check, and stands up a fresh broker every time a
federation ends (a bounded run finishing, or a crash) so the engine and bridge
can always re-form one.
"""

import argparse
import json
import logging
import pathlib
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import helics as h
import paho.mqtt.client as mqtt

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] helics_broker – %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout, force=True)
log = logging.getLogger("helics_broker")

SENTINEL = pathlib.Path("/tmp/helics-broker.ready")
_TICK = 0.05


# ---------------------------------------------------------------------------
# Broker queries
# ---------------------------------------------------------------------------

def _broker_query(broker: Any, what: str) -> Any:
    query = h.helicsCreateQuery("root", what)
    try:
        result = h.helicsQueryBrokerExecute(query, broker)
    finally:
        h.helicsQueryFree(query)
    if isinstance(result, str):
        try:
            result = json.loads(result)
        except ValueError:
            return None
    return result


def dead_core(broker: Any) -> bool:
    """True when the broker reports a core in the disconnected/error state
    while the federation as a whole is still up."""
    status = _broker_query(broker, "global_status")
    if not isinstance(status, dict):
        return False
    inner = status.get("status")
    if not isinstance(inner, dict):
        return False
    cores = inner.get("cores", [])
    return any(isinstance(c, dict) and c.get("state") in ("disconnected", "error") for c in cores)


def federates_time(broker: Any) -> Optional[float]:
    """The lowest granted time across the federation, or None before it forms."""
    tree = _broker_query(broker, "global_time")
    if not isinstance(tree, dict):
        return None
    times = []
    for core in tree.get("cores", []):
        for fed in core.get("federates", []):
            t = fed.get("granted_time")
            if isinstance(t, (int, float)):
                times.append(float(t))
    return min(times) if times else None


# ---------------------------------------------------------------------------
# Run control: the barrier as a transport
# ---------------------------------------------------------------------------

class RunControl:
    """Owns the barrier.  `allowed` is the highest simulation time federates
    may be granted; HELICS barriers are exclusive, so the barrier is set half
    a period above it."""

    def __init__(self, broker: Any, period: float, pace: float, base_topic: str) -> None:
        self.broker = broker
        self.period = period
        self.pace = pace                # sim seconds per real second; 0 = free-run
        self.base_topic = base_topic
        self.lock = threading.Lock()
        self.mode = "free" if pace <= 0 else "paced"
        self.allowed = 0.0              # step 0 is allowed from the start
        self._anchor_wall = time.monotonic()
        self._anchor_allowed = 0.0
        self._last_barrier: Optional[float] = None
        self.client: Optional[mqtt.Client] = None
        self._apply_barrier(force=True)

    # -- barrier -----------------------------------------------------------------

    def _apply_barrier(self, force: bool = False) -> None:
        if self.mode == "free":
            if self._last_barrier is not None or force:
                h.helicsBrokerClearTimeBarrier(self.broker)
                self._last_barrier = None
            return
        # Quantised to whole periods so the barrier only moves when a new
        # step becomes allowed — one broker call per step, not per tick.
        barrier = (int(self.allowed / self.period) + 0.5) * self.period
        if force or barrier != self._last_barrier:
            h.helicsBrokerSetTimeBarrier(self.broker, barrier)
            self._last_barrier = barrier

    def tick(self) -> None:
        """Called every _TICK: move the barrier along the wall clock when paced."""
        with self.lock:
            if self.mode == "paced":
                self.allowed = self._anchor_allowed + (time.monotonic() - self._anchor_wall) * self.pace
                self._apply_barrier()

    # -- commands ----------------------------------------------------------------

    def play(self) -> None:
        with self.lock:
            if self.mode != "paused":
                return
            self.mode = "free" if self.pace <= 0 else "paced"
            self._anchor_wall = time.monotonic()
            self._anchor_allowed = self.allowed
            self._apply_barrier(force=True)
        log.info("Control: play (%s, allowed sim time %.0f)", self.mode, self.allowed)
        self.publish_state()

    def pause(self) -> None:
        with self.lock:
            # Freeze where the federates actually are, not where the pace had
            # allowed them to be: an engine slower than the pace would otherwise
            # keep running until it caught up with a stale barrier.
            now = federates_time(self.broker)
            if now is not None:
                self.allowed = max(0.0, now)
            self.mode = "paused"
            self._apply_barrier(force=True)
        log.info("Control: pause (allowed sim time %.0f)", self.allowed)
        self.publish_state()

    def step(self) -> None:
        with self.lock:
            if self.mode != "paused":
                log.warning("Control: step ignored — not paused")
                return
            self.allowed += self.period
            self._apply_barrier(force=True)
        log.info("Control: step (allowed sim time %.0f)", self.allowed)
        self.publish_state()

    def set_pace(self, sim_seconds_per_second: float) -> None:
        pace = float(sim_seconds_per_second)
        if pace < 0:
            log.warning("Control: ignoring negative pace %s", pace)
            return
        with self.lock:
            self.pace = pace
            if self.mode != "paused":
                # `allowed` only advances while paced; after a free-run the
                # federates are far past it, and a barrier anchored there would
                # stall them until the wall clock caught up. Anchor on where they
                # actually are (never behind where they were already allowed).
                now = federates_time(self.broker)
                if now is not None:
                    self.allowed = max(self.allowed, now)
                self.mode = "free" if pace <= 0 else "paced"
                self._anchor_wall = time.monotonic()
                self._anchor_allowed = self.allowed
                self._apply_barrier(force=True)
        log.info("Control: pace %s", "free-run" if pace <= 0 else f"{pace:g} sim s / real s")
        self.publish_state()

    # -- MQTT --------------------------------------------------------------------

    def state(self) -> Dict[str, Any]:
        with self.lock:
            return {"mode": self.mode, "pace": self.pace, "allowed_sim_time": self.allowed,
                    "period": self.period, "at": datetime.now(timezone.utc).isoformat()}

    def publish_state(self) -> None:
        if self.client is None:
            return
        self.client.publish(f"{self.base_topic}/_meta/state", json.dumps(self.state()), qos=1, retain=True)

    def on_message(self, client, userdata, message) -> None:
        rel = message.topic[len(self.base_topic) + len("/_control/"):]
        raw = message.payload.decode("utf-8", errors="replace").strip()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        if rel == "play":
            self.play()
        elif rel == "pause":
            self.pause()
        elif rel == "step":
            self.step()
        elif rel == "pace":
            if "sim_seconds_per_second" in payload:
                self.set_pace(payload["sim_seconds_per_second"])
            else:
                log.warning("Control: pace without 'sim_seconds_per_second': %r", raw)
        # setpoint/* belongs to the bridge; anything else is not ours.


def connect_mqtt(control: RunControl, host: str, port: int) -> Optional[mqtt.Client]:
    """Best effort: the broker must keep serving the federation even without
    Mosquitto — it just cannot be driven from the dashboard then."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="helics_run_control", clean_session=True)
    connected = threading.Event()

    def on_connect(c, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            c.subscribe(f"{control.base_topic}/_control/#")
            connected.set()

    client.on_connect = on_connect
    client.on_message = control.on_message
    try:
        client.connect(host, port, keepalive=60)
    except OSError as exc:
        log.warning("MQTT unavailable (%s): run control is off, federation runs at the initial pace", exc)
        return None
    client.loop_start()
    if not connected.wait(10.0):
        log.warning("MQTT connect timed out: run control is off")
        client.loop_stop()
        return None
    control.client = client
    control.publish_state()
    log.info("Run control listening on '%s/_control/#'", control.base_topic)
    return client


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        prog="helics_broker",
        description="HELICS broker + run control. Every flag overrides the .env variable named in its help.",
    )
    # The federation is static by design — exactly the engine and the bridge —
    # so the count is a property of the architecture, not a deployment knob.
    parser.add_argument("--federates", type=int, default=2, help="Federates to wait for (engine + bridge).")
    parser.add_argument("--port", type=int, help="HELICS_BROKER_PORT")
    parser.add_argument("--core", help="HELICS_CORE")
    parser.add_argument("--period", type=float, help="PERIOD: simulation seconds per step.")
    parser.add_argument("--pace", type=float, help="PACE: initial simulation seconds per real second; 0 = free-run.")
    parser.add_argument("--mqtt-host", help="MQTT_HOST")
    parser.add_argument("--mqtt-port", type=int, help="MQTT_PORT")
    parser.add_argument("--topic", "-t", help="TOPIC: base topic for _control/# and _meta/state.")
    args = parser.parse_args()
    args.port = config.resolve(args.port, "HELICS_BROKER_PORT", int)
    args.core = config.resolve(args.core, "HELICS_CORE")
    args.period = config.resolve(args.period, "PERIOD", float)
    args.pace = config.resolve(args.pace, "PACE", float)
    args.mqtt_host = config.resolve(args.mqtt_host, "MQTT_HOST")
    args.mqtt_port = config.resolve(args.mqtt_port, "MQTT_PORT", int)
    args.topic = config.resolve(args.topic, "TOPIC")

    stop = False

    def _sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    # A HELICS federation is static: it forms once, with exactly --federates
    # members, and ends when they leave.  This process outlives federations —
    # when one ends (a bounded run finishing, or a federate crashing and being
    # restarted) it stands up a fresh broker for the next pair, so `restart:`
    # policies only ever have to bring back the engine and bridge.
    while not stop:
        broker = h.helicsCreateBroker(
            args.core, "coesi_broker",
            # --tick/--timeout: declare a silent core lost after ~10 s, not 30.
            f"-f {args.federates} --port {args.port} --external --tick 2000 --timeout 10000"
        )
        if not h.helicsBrokerIsConnected(broker):
            log.error("Broker failed to bind on port %d", args.port)
            return 1
        # The barrier is in place before any federate joins, so a free-running
        # engine cannot sprint ahead during the first tick.
        control = RunControl(broker, args.period, args.pace, args.topic)
        client = connect_mqtt(control, args.mqtt_host, args.mqtt_port)
        SENTINEL.write_text(str(time.time()))
        log.info("Broker up on %s port %d, waiting for %d federates; pace %s, period %.0f s",
                 args.core, args.port, args.federates,
                 "free-run" if args.pace <= 0 else f"{args.pace:g} sim s / real s", args.period)

        dead_since = None
        last_check = time.monotonic()
        while not stop and h.helicsBrokerIsConnected(broker):
            control.tick()
            time.sleep(_TICK)
            if time.monotonic() - last_check < 0.5:
                continue
            last_check = time.monotonic()
            # A member that died uncleanly can leave the federation wedged: the
            # survivor waits for grants that never come and a restarted member
            # is refused ("already in initialization mode"). The broker is the
            # one party that sees every core, so it is the one to pull the plug.
            if dead_core(broker):
                dead_since = dead_since or time.monotonic()
                if time.monotonic() - dead_since > 5.0:
                    log.warning("A federate's core is disconnected; tearing the federation down")
                    break
            else:
                dead_since = None

        try:
            SENTINEL.unlink()
        except FileNotFoundError:
            pass
        if client is not None:
            client.loop_stop()
            client.disconnect()
        h.helicsBrokerDisconnect(broker)
        h.helicsBrokerFree(broker)
        if stop:
            log.info("Broker shutting down (signal)")
        else:
            log.info("Federation finished; recycling the broker for the next one")
            time.sleep(0.5)

    h.helicsCloseLibrary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
