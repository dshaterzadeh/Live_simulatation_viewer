#!/usr/bin/env python3
"""
helics_broker.py
================
Runs the HELICS broker in-process, writes a readiness sentinel once it is
bound and accepting connections, and stands up a fresh broker every time a
federation ends so the engine and bridge can always re-form one.

Why not `helics_broker` from the wheel: it works, but gives the container's
health check nothing to look at except a TCP port.  Here the broker's own
`helicsBrokerIsConnected()` is what creates `/tmp/helics-broker.ready`, so the
compose health check (`test -f` + a TCP probe of the port) is the broker
saying it is ready, not a guess.  A throwaway federate would be the more
literal probe, but with a fixed `-f 2` federate count it would join — and
break — the real federation.
"""

import argparse
import json
import logging
import pathlib
import signal
import sys
import time

import helics as h

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] helics_broker – %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout, force=True)
log = logging.getLogger("helics_broker")

SENTINEL = pathlib.Path("/tmp/helics-broker.ready")


def dead_core(broker) -> bool:
    """True when the broker reports a core in the disconnected/error state
    while the federation as a whole is still up."""
    query = h.helicsCreateQuery("root", "global_status")
    try:
        status = h.helicsQueryBrokerExecute(query, broker)
    finally:
        h.helicsQueryFree(query)
    if isinstance(status, str):
        try:
            status = json.loads(status)
        except ValueError:
            return False
    if not isinstance(status, dict):
        # e.g. "#disconnected" while the broker is tearing down: no signal.
        return False
    inner = status.get("status")
    if not isinstance(inner, dict):
        return False
    cores = inner.get("cores", [])
    return any(isinstance(c, dict) and c.get("state") in ("disconnected", "error") for c in cores)


def main() -> int:
    parser = argparse.ArgumentParser(prog="helics_broker")
    parser.add_argument("--federates", type=int, default=2, help="Federates to wait for (engine + bridge).")
    parser.add_argument("--port", type=int, default=23404)
    parser.add_argument("--core", default="zmq")
    args = parser.parse_args()

    stop = False

    def _sig(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    # A HELICS federation is static: it forms once, with exactly --federates
    # members, and ends when they leave.  This process outlives federations —
    # when one ends (a bounded LOOP=0 run finishing, or a federate crashing and
    # being restarted) it stands up a fresh broker for the next pair, so
    # `restart:` policies only ever have to bring back the engine and bridge.
    while not stop:
        broker = h.helicsCreateBroker(
            args.core, "coesi_broker",
            # --tick/--timeout: declare a silent core lost after ~10 s, not 30.
            f"-f {args.federates} --port {args.port} --external --tick 2000 --timeout 10000"
        )
        if not h.helicsBrokerIsConnected(broker):
            log.error("Broker failed to bind on port %d", args.port)
            return 1
        SENTINEL.write_text(str(time.time()))
        log.info("Broker up on %s port %d, waiting for %d federates", args.core, args.port, args.federates)

        dead_since = None
        while not stop and h.helicsBrokerIsConnected(broker):
            time.sleep(0.5)
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
