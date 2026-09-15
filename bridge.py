#!/usr/bin/env python3
"""
bridge.py
=========
The HELICS ↔ MQTT bridge federate.  Subscribes to the engine's per-step
publication and republishes it to MQTT exactly as `hdf5_mqtt_publisher.py`
used to — same topics, same `{step, total_steps, dataset, attributes, values}`
payload, same latching zero filter, same order — publishes the run's
provenance, progress and controls catalog as retained `_meta/*` topics, and
carries the dashboard's setpoints back into the federation as endpoint
messages.  Play/pause/step/pace are not routed here: the broker owns those.

This is the half of the old publisher that does *not* change when the real
engine arrives.

Usage
-----
    python bridge.py --host mosquitto --topic sim/coesi5 --period 600 \
        --helics-broker tcp://helics-broker:23404
"""

import argparse
import json
import logging
import queue
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import helics as h
import paho.mqtt.client as mqtt

from federation import (
    DEFAULT_BROKER, DEFAULT_CORE, DEFAULT_PERIOD, EP_BRIDGE, EP_ENGINE, PUB_RUN, PUB_STEP,
    create_federate, engine_alive, finalize,
)

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
log = logging.getLogger("hdf5_mqtt_publisher")
# Ensure stdout is unbuffered in container environments
sys.stdout.reconfigure(line_buffering=True)


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------

_MQTT_CONNECTED = False
_MQTT_CONNECT_ERROR: Optional[str] = None


def _on_connect(
    client: mqtt.Client,
    userdata: Any,
    flags: Dict,
    reason_code: int,
    properties: Any = None,
) -> None:
    global _MQTT_CONNECTED, _MQTT_CONNECT_ERROR
    if reason_code == 0:
        _MQTT_CONNECTED = True
        log.info("Connected to MQTT broker.")
    else:
        _MQTT_CONNECT_ERROR = f"Connection refused – reason code {reason_code}"
        log.error(_MQTT_CONNECT_ERROR)


def _on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    disconnect_flags: Any,
    reason_code: int,
    properties: Any = None,
) -> None:
    global _MQTT_CONNECTED
    _MQTT_CONNECTED = False
    if reason_code != 0:
        log.warning(
            "Unexpected disconnect (reason code %s). Will attempt reconnect…",
            reason_code,
        )


def _on_publish(
    client: mqtt.Client,
    userdata: Any,
    mid: int,
    reason_code: Any = None,
    properties: Any = None,
) -> None:
    # Intentionally silent – high-frequency publishes would spam the log.
    pass


def build_mqtt_client(client_id: str = "hdf5_publisher") -> mqtt.Client:
    """Create and configure a Paho MQTT client with v2 callback API."""
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        clean_session=True,
    )
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_publish = _on_publish
    return client


def connect_broker(
    client: mqtt.Client,
    host: str,
    port: int,
    timeout: float = 10.0,
) -> None:
    """
    Connect to the broker and block until connected or timeout expires.
    Raises RuntimeError on failure.
    """
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
            raise RuntimeError(
                f"Timed out connecting to broker {host}:{port} after {timeout}s."
            )
        time.sleep(0.1)

    if _MQTT_CONNECT_ERROR:
        client.loop_stop()
        raise RuntimeError(_MQTT_CONNECT_ERROR)


# ---------------------------------------------------------------------------
# Publisher — relocated from hdf5_mqtt_publisher.publish_time_series
# ---------------------------------------------------------------------------

def _is_nonzero(value: Any) -> bool:
    """True if a step value (scalar or nested list) holds anything non-zero."""
    if isinstance(value, list):
        return any(_is_nonzero(v) for v in value)
    try:
        return bool(value != 0)
    except Exception:
        return bool(value)


def publish_run_metadata(
    client: mqtt.Client,
    base_topic: str,
    metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Publish the run's provenance once, retained, so a dashboard connecting
    mid-run immediately learns which run/config produced what it sees.
    """
    topic = f"{base_topic}/_meta/run"
    # QoS 1 (not 0 like the telemetry): a single low-frequency message where
    # guaranteed delivery actually matters.
    client.publish(topic, json.dumps(metadata, allow_nan=False), qos=1, retain=True)
    log.info("Published run metadata (retained) to '%s': %s", topic, metadata)
    return metadata


def publish_step(
    client: mqtt.Client,
    base_topic: str,
    step: int,
    n_steps: int,
    step_values: Dict[str, Dict[str, Any]],
    catalog: Dict[str, Dict[str, Dict]],
    has_been_nonzero: Set[str],
    qos: int = 0,
) -> int:
    """
    Publish one step to its per-variable sub-topics.  Filters out zeroes with
    the latching rule: a dataset is suppressed only while it has *never* been
    non-zero.  Returns the number of messages published.

    Blocks until the step's last message has actually been written to the
    socket.  `client.publish` only *queues*; without this, a fast engine hands
    us steps quicker than Paho's network thread can send them and the queue
    grows without bound (measured: ~40 MB/s at 500×, an OOM kill in minutes).
    Because the engine is in lockstep with us, waiting here is what turns
    "free-run" into "as fast as MQTT can take it" instead of "as fast as memory
    can fill".
    """
    published_count = 0
    last_info = None
    for entity, variables in step_values.items():
        for variable, value in variables.items():
            clean_path = f"{entity}/{variable}"

            # Check if it has ever been non-zero
            if clean_path not in has_been_nonzero:
                if _is_nonzero(value):
                    has_been_nonzero.add(clean_path)
                else:
                    continue # Suppress publishing this variable

            # The topic becomes: base_topic/clean_path
            # e.g., sim/coesi5/heating_frassinetto_hp2_bui_0155_0/COP
            topic = f"{base_topic}/{clean_path}"

            payload: Dict[str, Any] = {
                "step": step,
                "total_steps": n_steps,
                "dataset": clean_path,
                "attributes": catalog.get(entity, {}).get(variable, {}),
                "values": value,
            }

            message = json.dumps(payload, allow_nan=False)
            last_info = client.publish(topic, message, qos=qos)
            published_count += 1
    if last_info is not None:
        try:
            last_info.wait_for_publish(timeout=30.0)
        except (RuntimeError, ValueError) as exc:   # not queued / timed out
            log.warning("Step %d: MQTT send did not complete in time (%s)", step, exc)
    return published_count


# ---------------------------------------------------------------------------
# The federate
# ---------------------------------------------------------------------------

class Bridge:
    def __init__(self, client: mqtt.Client, base_topic: str, fed: Any, qos: int, period: float) -> None:
        self.client = client
        self.base_topic = base_topic
        self.fed = fed
        self.qos = qos
        self.period = period
        self.run_meta: Dict[str, Any] = {}
        self.sub_run = h.helicsFederateRegisterSubscription(fed, PUB_RUN, "")
        self.sub_step = h.helicsFederateRegisterSubscription(fed, PUB_STEP, "")
        self.endpoint = h.helicsFederateRegisterGlobalEndpoint(fed, EP_BRIDGE, "")
        self.time = 0.0
        self.n_steps = 0
        self.catalog: Dict[str, Dict[str, Dict]] = {}
        self.has_been_nonzero: Set[str] = set()
        self.published_steps = 0
        # Paho delivers on its own network thread; HELICS is not thread-safe,
        # so commands are queued here and sent from the federation loop.
        self.commands: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.engine_gone = False
        self.last_step, self.last_pass = 0, 0
        self._last_roster_check = time.monotonic()

    # -- MQTT control listener: setpoints only ---------------------------------
    # (play/pause/step/pace are answered by helics_broker.py on the same
    # _control/ prefix; subscribing narrowly keeps the two from overlapping.)

    def subscribe_control(self) -> None:
        topic = f"{self.base_topic}/_control/setpoint/#"
        self.client.on_message = self._on_control_message
        self.client.subscribe(topic)
        log.info("Setpoint listener subscribed to '%s'", topic)

    def _on_control_message(self, client, userdata, message) -> None:
        rel = message.topic[len(self.base_topic) + len("/_control/"):]
        raw = message.payload.decode("utf-8", errors="replace").strip()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}

        if rel.startswith("setpoint/"):
            if rel.endswith("/ack"):
                return                      # our own retained acks come back on the wildcard
            target = rel[len("setpoint/"):]
            if "/" not in target:
                log.warning("Control: setpoint topic needs <entity>/<variable>: %r", message.topic)
                return
            entity, variable = target.split("/", 1)
            # Empty payload or {"value": null} clears the override.
            self.commands.put({"command": "setpoint", "entity": entity, "variable": variable,
                               "value": payload.get("value")})
        else:
            log.warning("Control: unknown command '%s'", rel)

    # -- HELICS -> MQTT ---------------------------------------------------------

    def _on_run(self, raw: str) -> None:
        blob = json.loads(raw)
        metadata = blob.get("metadata", {})
        self.run_meta = metadata
        self.catalog = blob.get("catalog", {})
        self.n_steps = int(metadata.get("n_steps", 0))
        publish_run_metadata(self.client, self.base_topic, metadata)
        # What the dashboard may command, retained so a late page builds its
        # setpoint panel from the engine's word, not from names baked into HTML.
        controls = blob.get("controls", [])
        self.client.publish(f"{self.base_topic}/_meta/controls", json.dumps(controls, allow_nan=False),
                            qos=1, retain=True)
        log.info("Published controls catalog (retained): %d controllable inputs", len(controls))

    def _on_step(self, raw: str) -> None:
        blob = json.loads(raw)
        step, values = blob["step"], blob["values"]
        count = publish_step(self.client, self.base_topic, step, self.n_steps, values,
                             self.catalog, self.has_been_nonzero, self.qos)
        self.published_steps += 1
        self.last_step, self.last_pass = step, blob.get("pass", 0)
        self._publish_progress(step, self.last_pass, finished=False)
        log.info("Published step %d / %d  (%.1f%%) to %d active topics",
                 step + 1, self.n_steps, 100.0 * (step + 1) / max(self.n_steps, 1), count)

    def _publish_progress(self, step: int, pass_no: int, finished: bool) -> None:
        """Where the run is, retained: step, pass, the step's calendar time, and
        whether the engine has finished.  The dashboard's progress bar and its
        date axis read this — never the wall clock."""
        sim_time = None
        dt, start = self.run_meta.get("dt_seconds"), self.run_meta.get("start_time")
        if isinstance(dt, (int, float)) and start:
            try:
                sim_time = (datetime.fromisoformat(start) + timedelta(seconds=step * dt)).isoformat()
            except ValueError:
                sim_time = None
        body = {"step": step, "pass": pass_no, "sim_time": sim_time, "total_steps": self.n_steps,
                "finished": finished, "at": datetime.now(timezone.utc).isoformat()}
        # QoS 0 while running (one more small message per step); QoS 1 for the
        # final word, which a late subscriber must not miss.
        self.client.publish(f"{self.base_topic}/_meta/progress", json.dumps(body, allow_nan=False),
                            qos=1 if finished else 0, retain=True)

    def _on_engine_message(self, raw: str) -> None:
        try:
            ack = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Undecodable message from engine: %r", raw)
            return
        if ack.get("command") == "bye":
            log.info("Engine announced it is leaving the federation.")
            self.engine_gone = True
            return
        if ack.get("command") != "setpoint_ack":
            log.warning("Unexpected message from engine: %r", ack)
            return
        topic = f"{self.base_topic}/_control/setpoint/{ack['entity']}/{ack['variable']}/ack"
        body = {k: ack.get(k) for k in ("entity", "variable", "requested", "applied", "accepted", "reason", "step")}
        body["at"] = datetime.now(timezone.utc).isoformat()
        # Retained + QoS 1, like _meta/run: the last applied setpoint is state a
        # late subscriber must see immediately, and it reflects what the engine
        # actually did, not what we asked for.
        self.client.publish(topic, json.dumps(body, allow_nan=False), qos=1, retain=True)
        log.info("Setpoint ack -> '%s': %s", topic, body)

    # -- the loop ---------------------------------------------------------------

    def run(self) -> int:
        log.info("Entering federation …")
        h.helicsFederateEnterExecutingMode(self.fed)
        while True:
            # Lockstep with the engine on the step grid: we ask for the next
            # step's time, the engine cannot be granted it until we have, and
            # so no step can be published before we have read the previous one.
            # Under a barrier this blocks — that *is* pause; setpoints queued
            # meanwhile are sent at the next grant.
            self.time = h.helicsFederateRequestTime(self.fed, self.time + self.period)
            if h.helicsInputIsUpdated(self.sub_run):
                self._on_run(h.helicsInputGetString(self.sub_run))
            if h.helicsInputIsUpdated(self.sub_step):
                self._on_step(h.helicsInputGetString(self.sub_step))
            while h.helicsEndpointHasMessage(self.endpoint):
                self._on_engine_message(h.helicsMessageGetString(h.helicsEndpointGetMessage(self.endpoint)))
            if self.engine_gone:
                # A bounded run finished (or the engine was stopped): nothing
                # more will arrive, and a bridge with no engine is not a bridge.
                self._publish_progress(self.last_step, self.last_pass, finished=True)
                break
            # Safety net for an engine that died without saying goodbye: once
            # it is gone our time requests are granted instantly, so this loop
            # would otherwise spin forever.
            now = time.monotonic()
            if now - self._last_roster_check > 5.0:
                self._last_roster_check = now
                if not engine_alive(self.fed):
                    log.warning("Engine is no longer in the federation; stopping.")
                    break
            while not self.commands.empty():
                cmd = self.commands.get_nowait()
                h.helicsEndpointSendBytesTo(self.endpoint, json.dumps(cmd).encode(), EP_ENGINE)
                log.info("Control -> engine: %s", cmd)
        log.info("Engine finished; %d steps bridged.", self.published_steps)
        return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bridge",
        description="Bridges the HELICS engine federate to MQTT: telemetry out, control in.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="localhost", metavar="HOST", help="MQTT broker hostname or IP address.")
    parser.add_argument("--port", type=int, default=1883, metavar="PORT", help="MQTT broker port.")
    parser.add_argument("--topic", "-t", default="sim/hdf5/data", metavar="TOPIC", help="MQTT topic to publish to.")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=0, metavar="QOS",
                        help="MQTT Quality of Service level.")
    parser.add_argument("--client-id", default="hdf5_publisher", metavar="ID", help="MQTT client identifier.")
    parser.add_argument("--helics-broker", default=DEFAULT_BROKER, metavar="ADDR", help="HELICS broker address.")
    parser.add_argument("--helics-core", default=DEFAULT_CORE, metavar="TYPE", help="HELICS core type.")
    parser.add_argument("--period", type=float, default=DEFAULT_PERIOD, metavar="SECONDS",
                        help="Simulation seconds per step — must match the engine's and the broker's.")
    parser.add_argument("--no-control", action="store_true",
                        help="Do not listen on <topic>/_control/setpoint/# .")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    client = build_mqtt_client(client_id=args.client_id)
    try:
        connect_broker(client, args.host, args.port)
    except RuntimeError as exc:
        log.error("Cannot connect to broker: %s", exc)
        return 1

    fed = create_federate("bridge", args.helics_broker, args.period, args.helics_core)
    bridge = Bridge(client, args.topic, fed, args.qos, args.period)
    if not args.no_control:
        bridge.subscribe_control()

    try:
        return bridge.run()
    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C). Shutting down gracefully…")
        return 0
    finally:
        finalize(fed)
        client.loop_stop()
        client.disconnect()
        log.info("MQTT client disconnected. Bye.")


if __name__ == "__main__":
    sys.exit(main())
