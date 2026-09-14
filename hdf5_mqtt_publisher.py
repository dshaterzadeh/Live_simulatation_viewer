#!/usr/bin/env python3
"""
hdf5_mqtt_publisher.py
======================
Reads an HDF5 simulation file and publishes each time-step as a JSON
payload to an MQTT broker, simulating a live data stream.

Usage
-----
    python hdf5_mqtt_publisher.py \
        --file 20260623_baseline.hdf5 \
        --host localhost \
        --port 1883 \
        --topic sim/coesi5/baseline \
        --delay 0.5

Dependencies
------------
    pip install h5py paho-mqtt

Author  : IoT / Backend Automation Script
Version : 1.0.0
"""

import argparse
import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional

import paho.mqtt.client as mqtt

from datasources import HDF5DataSource, SimulationDataSource
from datasources.hdf5_source import explore_hdf5
from replay import ReplayController

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
# Publisher
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
    controller: ReplayController,
) -> Dict[str, Any]:
    """
    Publish the run's provenance once, retained, so a dashboard connecting
    mid-run immediately learns which run/config produced what it sees.
    """
    metadata = controller.get_run_metadata()
    topic = f"{base_topic}/_meta/run"
    # QoS 1 (not 0 like the telemetry): a single low-frequency message where
    # guaranteed delivery actually matters.
    client.publish(topic, json.dumps(metadata, allow_nan=False), qos=1, retain=True)
    log.info("Published run metadata (retained) to '%s': %s", topic, metadata)
    return metadata


def publish_time_series(
    client: mqtt.Client,
    base_topic: str,
    source: SimulationDataSource,
    controller: ReplayController,
    qos: int = 0,
) -> None:
    """
    Publish every step the controller hands out to its respective sub-topic.
    Filters out zeroes. All pacing lives in the controller.
    """
    n_datasets = sum(len(v) for v in source.get_entities().values())
    if not n_datasets:
        log.error("No datasets to publish.")
        return

    n_steps = source.get_step_count()

    log.info(
        "Starting publish: %d time steps across %d topics (delay=%.3fs, QoS=%d)",
        controller.end_step - controller.start_step + 1, n_datasets,
        controller.delay, qos,
    )

    publish_run_metadata(client, base_topic, controller)

    has_been_nonzero = set()
    published_steps = 0

    for step, step_values in controller.steps():
        published_count = 0
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
                    "attributes": source.get_attributes(entity, variable),
                    "values": value,
                }

                message = json.dumps(payload, allow_nan=False)
                client.publish(topic, message, qos=qos)
                published_count += 1

        published_steps += 1
        log.info(
            "Published step %d / %d  (%.1f%%) to %d active topics",
            step + 1, n_steps, 100.0 * (step + 1) / n_steps, published_count
        )

    log.info("All %d steps published successfully.", published_steps)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="hdf5_mqtt_publisher",
        description=(
            "Reads an HDF5 simulation file and publishes each time-step "
            "as a JSON payload to an MQTT broker."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- File / dataset
    parser.add_argument(
        "--file", "-f",
        required=True,
        metavar="PATH",
        help="Path to the HDF5 file.",
    )
    parser.add_argument(
        "--explore",
        action="store_true",
        help="Print all datasets in the HDF5 file and exit.",
    )

    # --- MQTT
    parser.add_argument(
        "--host",
        default="localhost",
        metavar="HOST",
        help="MQTT broker hostname or IP address.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=1883,
        metavar="PORT",
        help="MQTT broker port.",
    )
    parser.add_argument(
        "--topic", "-t",
        default="sim/hdf5/data",
        metavar="TOPIC",
        help="MQTT topic to publish to.",
    )
    parser.add_argument(
        "--qos",
        type=int,
        choices=[0, 1, 2],
        default=0,
        metavar="QOS",
        help="MQTT Quality of Service level.",
    )
    parser.add_argument(
        "--client-id",
        default="hdf5_publisher",
        metavar="ID",
        help="MQTT client identifier.",
    )

    # --- Timing
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        metavar="SECONDS",
        help="Delay in seconds between published time steps.",
    )

    # --- Replay window / transport
    parser.add_argument(
        "--start-step",
        type=int,
        default=0,
        metavar="STEP",
        help="First step to replay.",
    )
    parser.add_argument(
        "--end-step",
        type=int,
        default=None,
        metavar="STEP",
        help="Last step to replay (default: the last step in the file).",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Restart from --start-step after --end-step instead of terminating.",
    )
    parser.add_argument(
        "--no-control",
        action="store_true",
        help="Do not listen on <topic>/_control/# for play/pause/seek/speed.",
    )

    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    # ── Explore mode ──────────────────────────────────────────────────────
    if args.explore:
        explore_hdf5(args.file)
        return 0

    # ── Data source ───────────────────────────────────────────────────────
    try:
        source = HDF5DataSource(args.file)
    except OSError as exc:
        log.error("Failed to read file: %s", exc)
        return 1

    if source.get_step_count() <= 0:
        log.error("No valid 1D time-series datasets found in the HDF5 file.")
        return 1

    controller = ReplayController(
        source=source,
        delay=args.delay,
        start_step=args.start_step,
        end_step=args.end_step,
        loop=args.loop,
    )

    # ── MQTT setup ────────────────────────────────────────────────────────
    client = build_mqtt_client(client_id=args.client_id)

    try:
        connect_broker(client, args.host, args.port)
    except RuntimeError as exc:
        log.error("Cannot connect to broker: %s", exc)
        return 1

    if not args.no_control:
        try:
            controller.start_control_listener(
                host=args.host,
                port=args.port,
                base_topic=args.topic,
                client_id=f"{args.client_id}_control",
            )
        except (RuntimeError, OSError) as exc:
            log.warning("Control listener unavailable (%s) – replay continues.", exc)

    # ── Publish ───────────────────────────────────────────────────────────
    try:
        publish_time_series(
            client=client,
            base_topic=args.topic,
            source=source,
            controller=controller,
            qos=args.qos,
        )
    except KeyboardInterrupt:
        log.info("Interrupted by user (Ctrl+C). Shutting down gracefully…")
    finally:
        controller.stop_control_listener()
        client.loop_stop()
        client.disconnect()
        log.info("MQTT client disconnected. Bye.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
