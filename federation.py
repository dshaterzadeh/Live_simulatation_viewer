"""
federation.py
=============
The HELICS interface between the simulation engine and the MQTT bridge — the
one contract a real engine has to honour when it replaces `engine.py`.

Names, payload shapes and the federation's clock are all defined here so that
neither federate hard-codes the other's details.

Publications (engine -> bridge), both JSON strings:

    PUB_RUN   once, at start:   {"metadata": {...run provenance...},
                                 "catalog":  {entity: {variable: attributes}},
                                 "controls": [{entity, variable, units, min, max, couples}]}
    PUB_STEP  once per step:    {"step": int, "pass": int,
                                 "values": {entity: {variable: value}}}

Endpoints (messages, JSON strings):

    EP_ENGINE  bridge -> engine   {"command": "setpoint", "entity": str,
                                   "variable": str, "value": number | null}
    EP_BRIDGE  engine -> bridge   {"command": "setpoint_ack", "entity", "variable",
                                   "requested", "applied", "accepted", "reason", "step"}
                                  {"command": "bye"}   sent once, before the engine leaves

Clock: HELICS time **is simulation time**, in seconds.  The engine requests
`t + period` for every step and publishes when granted; it never sleeps.
Play, pause, step and pace are not the engine's business at all — the broker
(`helics_broker.py`) holds the whole federation back with a time barrier.
A real engine (CosimGym-style, `request_time(granted + period)`, as fast as it
computes) therefore needs nothing added to be driven from the dashboard.
"""

import json
import logging
from typing import Any, List

import helics as h

log = logging.getLogger("hdf5_mqtt_publisher")

PUB_RUN = "engine/run"
PUB_STEP = "engine/step"
EP_ENGINE = "engine/control"
EP_BRIDGE = "bridge/control"

# The period (simulation seconds per step), broker address and core type are
# configuration, not contract: the engine takes the period from its source, the
# bridge and broker from PERIOD, and all three take the broker from
# HELICS_BROKER_HOST/PORT and HELICS_CORE (see config.py, .env.example).


def create_federate(name: str, broker_address: str, period: float, core_type: str) -> Any:
    """A combination federate (values + messages) joined to the shared broker,
    stepping on a `period` grid so grants land on whole steps."""
    info = h.helicsCreateFederateInfo()
    h.helicsFederateInfoSetCoreTypeFromString(info, core_type)
    h.helicsFederateInfoSetCoreInitString(
        info, f"--federates=1 --broker_address={broker_address}"
    )
    h.helicsFederateInfoSetTimeProperty(info, h.HELICS_PROPERTY_TIME_PERIOD, period)
    # Values only matter when they change; the per-step blob always changes.
    h.helicsFederateInfoSetFlagOption(info, h.HELICS_FLAG_ONLY_UPDATE_ON_CHANGE, False)
    fed = h.helicsCreateCombinationFederate(name, info)
    h.helicsFederateInfoFree(info)
    log.info("HELICS federate '%s' created (core=%s, broker=%s, period=%.0f s)", name, core_type, broker_address, period)
    return fed


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
