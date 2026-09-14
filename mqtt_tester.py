#!/usr/bin/env python3
"""
mqtt_tester.py
==============
A rich terminal subscriber that listens to an MQTT topic and
displays each incoming message with colour, statistics, and a
live sparkline so you can verify the publisher is working
in real time.

Usage
-----
    # subscribe to everything the publisher sends
    python mqtt_tester.py --host localhost --topic "sim/coesi5/#"

    # subscribe to a single leaf topic
    python mqtt_tester.py --topic "sim/coesi5/weather" --max-messages 50

Dependencies
------------
    pip install paho-mqtt rich
"""

import argparse
import json
import signal
import sys
import time
from collections import deque
from datetime import datetime
from typing import Any, Deque, Dict, List, Optional

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Optional rich import – fall back to plain print if not installed
# ---------------------------------------------------------------------------
try:
    from rich import box
    from rich.columns import Columns
    from rich.console import Console
    from rich.live import Live
    from rich.panel import Panel
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# ---------------------------------------------------------------------------
# State shared between MQTT callbacks and the display loop
# ---------------------------------------------------------------------------
_messages: List[Dict[str, Any]] = []
_value_history: Deque[float] = deque(maxlen=40)
_stats = {
    "received": 0,
    "first_ts": None,
    "last_ts":  None,
    "last_step": None,
    "total_steps": None,
    "topic": "—",
    "dataset": "—",
}
_stop = False
_console = Console() if HAS_RICH else None


# ---------------------------------------------------------------------------
# Sparkline helper
# ---------------------------------------------------------------------------
SPARK_CHARS = "▁▂▃▄▅▆▇█"

def sparkline(values: Deque[float]) -> str:
    if not values:
        return "no data yet"
    mn, mx = min(values), max(values)
    rng = mx - mn or 1.0
    bars = [SPARK_CHARS[min(7, int((v - mn) / rng * 8))] for v in values]
    return "".join(bars)


# ---------------------------------------------------------------------------
# Build the live Rich layout
# ---------------------------------------------------------------------------
def _make_table(args: argparse.Namespace) -> "Table":
    table = Table(
        title=f"[bold cyan]MQTT Live Feed[/bold cyan]  ·  [yellow]{args.topic}[/yellow]",
        box=box.ROUNDED,
        border_style="bright_blue",
        header_style="bold magenta",
        show_lines=True,
        expand=True,
    )
    table.add_column("#",       style="dim",          width=5,  justify="right")
    table.add_column("Time",    style="cyan",          width=10)
    table.add_column("Step",    style="green",         width=14, justify="right")
    table.add_column("Topic",   style="yellow",        width=32, overflow="fold")
    table.add_column("Dataset", style="bright_white",  width=40, overflow="fold")
    table.add_column("Value",   style="bright_green",  width=16, justify="right")
    table.add_column("Spark",   style="bright_yellow", width=42, overflow="fold")
    return table


def _populate_table(table: "Table") -> None:
    """Fill the table with the last N messages."""
    # Show last 15 messages to keep the terminal clean
    window = _messages[-15:]
    for msg in window:
        val = msg.get("values")
        # Scalar display
        if isinstance(val, (int, float)):
            val_str = f"{val:.4f}"
        elif isinstance(val, list):
            val_str = f"[{len(val)}×]"
        else:
            val_str = str(val)

        step      = msg.get("step", "?")
        total     = msg.get("total_steps", "?")
        progress  = f"{step + 1}/{total}"
        pct       = int((step + 1) / total * 100) if isinstance(total, int) else 0
        step_cell = f"[green]{progress}[/green] [dim]{pct}%[/dim]"

        table.add_row(
            str(msg["_seq"]),
            msg["_time"],
            step_cell,
            msg.get("_topic", ""),
            msg.get("dataset", ""),
            val_str,
            sparkline(_value_history),
        )


def _make_stats_panel() -> "Panel":
    rx     = _stats["received"]
    first  = _stats["first_ts"] or "—"
    last   = _stats["last_ts"] or "—"
    s      = _stats["last_step"]
    tot    = _stats["total_steps"]
    ds     = _stats["dataset"]

    pct_bar = ""
    if s is not None and tot:
        filled = int(s / tot * 30)
        pct_bar = f"[cyan]{'█' * filled}{'░' * (30 - filled)}[/cyan] [bold]{int(s/tot*100)}%[/bold]"

    text = Text()
    text.append(f"  Messages received : ", style="dim")
    text.append(f"{rx}\n", style="bold green")
    text.append(f"  Dataset           : ", style="dim")
    text.append(f"{ds}\n", style="bright_white")
    text.append(f"  First message     : ", style="dim")
    text.append(f"{first}\n", style="cyan")
    text.append(f"  Last message      : ", style="dim")
    text.append(f"{last}\n", style="cyan")
    text.append(f"  Progress          : ", style="dim")
    text.append(f"{pct_bar}\n" if pct_bar else "—\n")
    text.append(f"  Sparkline (last 40): ", style="dim")
    text.append(sparkline(_value_history), style="bright_yellow")

    return Panel(
        text,
        title="[bold]Statistics[/bold]",
        border_style="blue",
        expand=True,
    )


# ---------------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------------
def _on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        topic = userdata["topic"]
        client.subscribe(topic, qos=userdata.get("qos", 0))
        if not HAS_RICH:
            print(f"[INFO] Connected. Subscribed to '{topic}'")
    else:
        print(f"[ERROR] Connection refused – reason code {reason_code}", file=sys.stderr)


def _on_message(client, userdata, msg):
    global _stop
    try:
        payload = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        payload = {"raw": msg.payload.decode("utf-8", errors="replace")}

    now = datetime.now().strftime("%H:%M:%S")
    seq = _stats["received"] + 1
    _stats["received"] = seq
    _stats["first_ts"] = _stats["first_ts"] or now
    _stats["last_ts"]  = now
    _stats["last_step"] = payload.get("step")
    _stats["total_steps"] = payload.get("total_steps")
    _stats["dataset"] = payload.get("dataset", "—")

    # Track scalar values for sparkline
    val = payload.get("values")
    if isinstance(val, (int, float)):
        _value_history.append(float(val))

    entry = dict(payload)
    entry["_seq"]   = seq
    entry["_time"]  = now
    entry["_topic"] = msg.topic
    _messages.append(entry)

    # Check max_messages limit
    limit = userdata.get("max_messages")
    if limit and seq >= limit:
        _stop = True

    if not HAS_RICH:
        # Plain fallback print
        print(
            f"[{now}] #{seq:>4}  step={payload.get('step','?')}/{payload.get('total_steps','?')}  "
            f"val={val}  topic={msg.topic}"
        )


def _on_disconnect(client, userdata, disconnect_flags, reason_code, properties=None):
    if reason_code != 0:
        print(f"[WARN] Unexpected disconnect (code {reason_code})", file=sys.stderr)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="mqtt_tester",
        description="Subscribe to an MQTT topic and display a live message feed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host",         default="localhost", metavar="HOST")
    parser.add_argument("--port",         type=int, default=1883, metavar="PORT")
    parser.add_argument("--topic", "-t",  default="sim/coesi5/#", metavar="TOPIC",
                        help="MQTT topic filter (wildcards supported: + and #)")
    parser.add_argument("--qos",          type=int, choices=[0,1,2], default=0)
    parser.add_argument("--max-messages", type=int, default=None, metavar="N",
                        help="Stop after receiving N messages (default: unlimited)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    global _stop
    args = parse_args()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="mqtt_tester",
        clean_session=True,
    )
    client.user_data_set({"topic": args.topic, "qos": args.qos,
                           "max_messages": args.max_messages})
    client.on_connect    = _on_connect
    client.on_message    = _on_message
    client.on_disconnect = _on_disconnect

    try:
        client.connect(args.host, args.port, keepalive=60)
    except ConnectionRefusedError:
        print(
            f"\n[ERROR] Could not connect to broker at {args.host}:{args.port}.\n"
            "Make sure Mosquitto is running:  mosquitto -v\n",
            file=sys.stderr,
        )
        sys.exit(1)

    client.loop_start()

    def _sigint(sig, frame):
        global _stop
        _stop = True

    signal.signal(signal.SIGINT, _sigint)

    if HAS_RICH:
        with Live(refresh_per_second=4, screen=False) as live:
            while not _stop:
                table = _make_table(args)
                _populate_table(table)
                stats = _make_stats_panel()
                live.update(Columns([stats, table], equal=False, expand=True))
                time.sleep(0.25)
    else:
        # plain mode – messages are printed from the callback
        print(f"[INFO] Listening on '{args.topic}' at {args.host}:{args.port}  (Ctrl+C to stop)")
        while not _stop:
            time.sleep(0.1)

    client.loop_stop()
    client.disconnect()

    # Final summary (rich)
    if HAS_RICH and _console:
        _console.print(Rule("[bold cyan]Session Summary[/bold cyan]"))
        _console.print(f"  Total messages received : [bold green]{_stats['received']}[/bold green]")
        _console.print(f"  Dataset                 : [bright_white]{_stats['dataset']}[/bright_white]")
        _console.print(f"  First / Last            : {_stats['first_ts']} → {_stats['last_ts']}")
    else:
        print(f"\n--- Summary ---")
        print(f"Total messages: {_stats['received']}")

    print("\nBye.")


if __name__ == "__main__":
    main()
