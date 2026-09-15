# Pattern: the broker owns pacing; no federate implements transport control

A note that stands on its own — it does not depend on this repository's code.

## The problem

You have a HELICS co-simulation whose federates do the natural thing: `request_time
(granted + period)`, compute, publish, repeat — as fast as they can. You want a human to
pause it, step it, and slow it to a watchable rate from outside, without any model
knowing that a human exists. Putting play/pause/speed *inside* a federate is the trap: it
works for that one federate, it has to be re-implemented in every other one, and it
disappears the day that federate is replaced.

## The pattern

Use the broker's **time barrier**. HELICS lets whoever holds the broker set a time
`T` past which no federate is granted a time request (`helicsBrokerSetTimeBarrier`,
`helicsBrokerClearTimeBarrier`; also reachable through the broker's command interface).
Every federate blocks in its own `request_time` call until the barrier moves. That gives
you every transport control for free, for the whole federation at once:

| control | what the barrier does |
|---|---|
| **pause** | freeze it at the federates' current time (query `global_time` for where they actually are, not where you had allowed them to be) |
| **step** | move it forward by exactly one period |
| **pace** | move it along the wall clock at *N* simulated seconds per real second, quantised to whole periods |
| **free-run** | clear it |

Nothing in any model changes. The barrier can also be set *before* the federates join,
so a free-running federation cannot sprint ahead during startup.

## Three consequences to design around

1. **Pace is an upper bound.** A barrier can hold a federation back; it cannot make a
   slow model step faster. Whatever shows "speed" to a person must show the rate
   actually achieved next to the rate requested.
2. **"Speed ×" is the wrong unit.** A multiplier of a base delay assumes a base delay,
   which a real model does not have. Simulated time per real second (1 min/s, 1 h/s,
   1 day/s, free-run) means the same thing for any engine.
3. **Anything that talks to the paused federation is paused too.** A federate that
   relays external commands (a dashboard bridge) is blocked in `request_time` like
   everyone else, so a command it receives while paused is delivered at its next grant
   — i.e. after resume. Make that visible, or route commands that must act *during* a
   pause (pause/step/pace themselves) to the broker process, which is not a federate
   and never blocks.

## What it buys you

- The engine and every model federate stay exactly what they are in a headless run.
- Swapping a stand-in engine for the real one does not touch the controls.
- Two operators, or a reloaded page, agree on the state because the broker publishes it
  (here: a retained MQTT message on every transition) rather than each client guessing.
- There is still no rewind — a federation's clock only moves forward. "Step" is the
  precise inspection tool; "scrub back" has to come from stored results, not from the
  federation.

## When the broker is not yours

If an orchestrator launches the broker (as CosimGym's `ScenarioManager` does with
`helics_broker`), the barrier has to be set through the HELICS command/query channel
from a federate you do own — typically an *observer* federate that subscribes to the
values you want to watch and holds no timing of its own. If that channel is unavailable,
the fallback is a "pacer" publication that every model subscribes to: models then depend
on the pacer's time and can be held back by it. It works, but it costs one line in every
model's configuration, which is exactly what the barrier avoids.
