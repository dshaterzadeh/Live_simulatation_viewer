"""
federation.py
=============
The HELICS interface between the simulation engine and the MQTT bridge — the
one contract a real engine has to honour when it replaces `engine.py`.

Names, payload shapes and the federation's clock are all defined here so that
neither federate hard-codes the other's details.

Publications (engine -> bridge), both JSON strings:

    PUB_RUN   once, at start:   {"metadata": {...run provenance...},
                                 "catalog":  {entity: {variable: attributes}}}
    PUB_STEP  once per step:    {"step": int, "values": {entity: {variable: value}}}

Endpoints (messages, JSON strings):

    EP_ENGINE  bridge -> engine   {"command": "play" | "pause"}
                                  {"command": "speed", "multiplier": float}
                                  {"command": "setpoint", "entity": str,
                                   "variable": str, "value": number | null}
    EP_BRIDGE  engine -> bridge   {"command": "setpoint_ack", "entity", "variable",
                                   "requested", "applied", "accepted", "reason", "step"}
                                  {"command": "state", "playing": bool, "speed": float,
                                   "step": int}     at start and on every play/pause/speed
                                  {"command": "bye"}   sent once, before the engine leaves

Clock: HELICS time is advanced by the engine in TIME_DELTA slices from its own
wall clock — it keeps running while the replay is paused, which is what lets a
`play` message reach a paused engine at all.  Simulation time is *not* HELICS
time here; it is `step * dt_seconds` with `dt_seconds` taken from the run
metadata.  A real engine will make HELICS time the simulation clock; the bridge
does not care which, it only reads what arrives at each grant.
"""

import json
import logging
import os
import time
from typing import Any, List

import helics as h

log = logging.getLogger("hdf5_mqtt_publisher")

#: HELICS seconds per engine tick — also the bridge's request granularity
TIME_DELTA = 0.05

PUB_RUN = "engine/run"
PUB_STEP = "engine/step"
EP_ENGINE = "engine/control"
EP_BRIDGE = "bridge/control"

DEFAULT_BROKER = "tcp://localhost:23404"
DEFAULT_CORE = "zmq"

#: A time request that is not granted within this many seconds means the other
#: federate is gone and the broker has not noticed: the federation is wedged.
GRANT_TIMEOUT = 20.0


def create_federate(name: str, broker_address: str, core_type: str = DEFAULT_CORE) -> Any:
    """A combination federate (values + messages) joined to the shared broker."""
    info = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreTypeFromString(info, core_type)
    h.helicsFederateInfoSetCoreInitString(
        info, f"--federates=1 --broker_address={broker_address}"
    )
    h.helicsFederateInfoSetTimeProperty(info, h.HELICS_PROPERTY_TIME_DELTA, TIME_DELTA)
    # Values only matter when they change; the per-step blob always changes.
    h.helicsFederateInfoSetFlagOption(info, h.HELICS_FLAG_ONLY_UPDATE_ON_CHANGE, False)
    fed = h.helicsCreateCombinationFederate(name, info)
    h.helicsFederateInfoFree(info)
    log.info("HELICS federate '%s' created (core=%s, broker=%s)", name, core_type, broker_address)
    return fed


def request_time(fed: Any, requested: float, who: str) -> float:
    """`helicsFederateRequestTime` with a watchdog.  Blocking forever is the one
    failure mode a federate must not have: with the peer dead and the broker
    still counting it, the stack would sit wedged until someone noticed.  The
    process exits instead, so compose restarts it into a fresh federation."""
    h.helicsFederateRequestTimeAsync(fed, requested)
    deadline = time.monotonic() + GRANT_TIMEOUT
    while not h.helicsFederateIsAsyncOperationCompleted(fed):
        if time.monotonic() > deadline:
            log.error("%s: no time grant for %.0f s — the federation is wedged; exiting for a restart.",
                      who, GRANT_TIMEOUT)
            os._exit(3)
        time.sleep(0.002)
    return h.helicsFederateRequestTimeComplete(fed)


def engine_alive(fed: Any) -> bool:
    """Whether the engine's core is still connected, from a root `global_status`
    query.  (The `federates` roster keeps listing a federate after it leaves,
    so it cannot answer this.)  pyhelics returns queries as parsed objects;
    a raw JSON string is handled too, for safety."""
    query = h.helicsCreateQuery("root", "global_status")
    try:
        status = h.helicsQueryExecute(query, fed)
    finally:
        h.helicsQueryFree(query)
    if isinstance(status, str):
        try:
            status = json.loads(status)
        except ValueError:
            return True                       # unreadable: don't kill the bridge on a hunch
    cores = (status or {}).get("status", {}).get("cores", [])
    for core in cores:
        if str(core.get("attributes", {}).get("name", "")).startswith("engine_"):
            return core.get("state") != "disconnected"
    return False


def finalize(fed: Any) -> None:
    """Leave the federation cleanly; safe to call more than once."""
    try:
        h.helicsFederateDisconnect(fed)
        h.helicsFederateDestroy(fed)
    except Exception as exc:  # pragma: no cover – shutdown best effort
        log.warning("HELICS finalize: %s", exc)
