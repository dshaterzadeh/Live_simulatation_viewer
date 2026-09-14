# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

A **scientific research tool**, not a production monitoring system: a district
building-energy simulation stored in HDF5 (`20260623_baseline.hdf5` — 289 datasets,
4320 steps, 600 s resolution) is replayed step-by-step over MQTT into a browser
dashboard. There is no database, no server-side state, no build step, and no test suite.

The priorities that drive design decisions here, in order:

1. **Interactive inspection** — researchers need to pause, scrub and re-watch specific
   moments, not just watch playback run.
2. **Provenance** — every run publishes what produced it, so a screenshot is
   self-describing.
3. **Swappable data source** — the source layer must be replaceable (e.g. InfluxDB)
   without touching pacing, publishing or the dashboard.

[ARCHITECTURE.md](ARCHITECTURE.md) is the deep reference — every component, algorithm
and contract with line-level pointers. Read the relevant chapter before changing a
component; keep it updated when you change one.

## Layout

```
run.sh                     The one entry point: up / down / restart / status / logs / dev
docker-compose.yml         broker (health-checked) → publisher, sensors, frontend
Dockerfile                 publisher + sensor image (pinned via requirements.txt)
hdf5_mqtt_publisher.py     MQTT connection lifecycle, zero filter, payload build, CLI
sensor_simulator.py        Synthetic sensor stream (sensors/#) — own CLI, client and pacing
datasources/base.py        SimulationDataSource ABC — the swappable contract
datasources/hdf5_source.py HDF5DataSource — the ONLY module that imports h5py
replay/controller.py       ReplayController — pacing, play/pause/seek/speed, run metadata
mqtt_web_tester.html       The entire frontend: CSS + markup + JS in one file
mqtt_tester.py             Terminal subscriber (Rich TUI)
local_server.py            Static server + /proxy/ CORS stripper for the UrbanSim API
mosquitto.conf             Dual listener: TCP 1883 + WebSockets 9001
```

Dependency direction is strictly `datasources/ → replay/ → hdf5_mqtt_publisher.py`.
`sensor_simulator.py` is a *sibling* of the publisher: it depends on `datasources/` only,
never on `replay/` or the publisher.

## Running it

```bash
./run.sh up          # everything: broker, publisher, sensors, frontend (Docker Compose)
                     # → http://localhost:8002/mqtt_web_tester.html → Connect
./run.sh logs        # follow the publisher (./run.sh logs sensors|mosquitto|frontend)
./run.sh down
```

`run.sh` is a thin wrapper over `docker-compose.yml`; the service definitions, port
mappings and health ordering live there, not in the script. The publisher and sensors
are **not started until the broker's health check passes**, and everything runs
`restart: unless-stopped`.

Looping is the default and is the point: without it the replay ends after the last step,
both MQTT connections tear down, and — since telemetry is QoS 0 and unretained — the
dashboard has nothing to receive and nothing is listening on `_control/#`.
`LOOP=0 ./run.sh up` gives a bounded one-shot run; `SEED=42 ./run.sh up` makes the sensor
noise reproducible. Speed is a runtime concern: `DELAY` (default 1.0) is the baseline
pace and the dashboard's speed selector divides it, so don't bake a fast delay into the
compose file.

Host-side iteration (what to use while changing the Python):

```bash
./run.sh dev         # broker + frontend in Docker; publisher + sensors from .venv at --delay 0.3
```

or the two processes by hand:

```bash
.venv/bin/python hdf5_mqtt_publisher.py -f 20260623_baseline.hdf5 -t sim/coesi5 --delay 0.3
.venv/bin/python sensor_simulator.py    -f 20260623_baseline.hdf5 -t sensors    --delay 0.3 --seed 42
```

`.venv/` is built from `requirements.txt` (h5py, paho-mqtt, numpy, rich). Use
`.venv/bin/python`, not bare `python3`. The frontend is bind-mounted into its container,
so HTML edits only need a browser reload.

Driving a running replay:

```bash
mosquitto_pub -t sim/coesi5/_control/pause -m ''
mosquitto_pub -t sim/coesi5/_control/seek  -m '{"step": 2000}'
mosquitto_pub -t sim/coesi5/_control/speed -m '{"multiplier": 4.0}'
mosquitto_pub -t sim/coesi5/_control/play  -m ''
```

## Invariants — do not break these without being asked

- **Only `datasources/hdf5_source.py` may import `h5py`.** This is the mechanical
  guarantee that the source layer is swappable; it is checkable with one grep.
- **The wire contract is fixed**: topics `sim/coesi5/<entity>/<variable>`, payload
  `{step, total_steps, dataset, attributes, values}`. New behaviour goes on new reserved
  topics (`_meta/*`, `_control/*`), never by reshaping the telemetry payload.
- **The latching zero filter stays as-is**: a dataset is suppressed only while it has
  *never* been non-zero; once latched, every subsequent value is published, zeros
  included. A stateless "skip zeros" filter would leave the dashboard rendering stale
  values as if live.
- **`python hdf5_mqtt_publisher.py -f <file>` with no other flags must keep behaving as
  it does today** — same ~175 topics, same message content and order. All new CLI flags
  are opt-in.
- **No decimation or downsampling in the publisher.** Full fidelity is what gets
  published; rendering-side downsampling belongs in the dashboard's charting code only
  (`compressSeries` already does this).
- **Pacing lives in `ReplayController`, nowhere else.** No `time.sleep` in the publish
  loop.
- **`allow_nan=False` on `json.dumps` is deliberate** — a NaN-producing solver run must
  fail loudly rather than emit payloads the browser silently drops.
- **The sensor `status` field is load-bearing.** `"dropout"` always carries `value: null`
  — never `0`, never an omitted key — and `"fault"` always carries the bad value plus its
  tag. Every renderer downstream (chart gap, table em dash, map "no data" grey) keys off
  this. A change that lets a dropout reach the dashboard as a number, or that renders one
  as `0` or as a default colour, breaks the only thing this layer is for.
- **The sensors namespace is separate and the frontend routes by topic prefix.** Sensor
  payloads never enter the simulation dataset parser and vice versa. New sensor behaviour
  goes on `sensors/*`, never by reshaping the telemetry payload.
- **There is one chart implementation, parameterised by context** (`simChart` /
  `sensorChart`). The step-ordered insertion and deterministic decimation in it have each
  been fixed once already; do not write a second charting or table path that could
  reintroduce either bug. Same for `buildGridTable`/`paintGridTable`.
- **The sensor simulator has no control channel.** It does not use `ReplayController` and
  must keep streaming at its own cadence while the replay is paused, sought or sped up.
- **Sensor cadence derives from the source's `dt_seconds`**, never hardcoded against the
  reference file's 600 s.
- **Sensor values are calibrated into a per-type `TargetRange` before noise is applied.**
  Raw simulation values are correct but not automatically plausible as sensor readings
  (`TBuilding` is legitimately 7–19 °C when unheated). Calibration is affine, so the shape
  of the signal is preserved — tune a range by editing its `SENSOR_SPECS` row, never by
  special-casing a value in the sampling path.
- **The Sensors tab plots real time; the Live Chart and Data Table plot steps.** Chart
  *storage* stays step-keyed either way — `step` is the monotonic integral key the
  ordering and decimation fixes depend on — and only the render-time x accessor differs.
  Sensor axis ticks must stay whole multiples of that sensor's `interval_seconds`, or they
  point at clock times no reading exists for.

## Conventions

- Match the surrounding style rather than importing a house style: this code uses plain
  functions, module-level logging to the single `hdf5_mqtt_publisher` logger, and
  comments that explain *why*.
- The frontend has **no build step and no framework**. Add inline `<script>` handlers
  and CSS custom properties in the existing blocks; reuse the one already-instantiated
  Paho client rather than opening another.
- The top bar is width-constrained, and the transport controls have first claim on it.
  Broker host, port and topic live behind the **Config** drop-down; anything else set once
  per session belongs there too, not inline. Keep the input ids (`host`, `port`, `topic`)
  — `toggleConnection()` and `controlBase()` read them by id.
- Prefer relocating code over rewriting it when refactoring — a pure move stays
  reviewable, and byte-identical output is then verifiable.

## Verifying a change

There is no test suite, so verify against the real thing:

- **Publisher parity** — replay a few steps through both the old and new paths and
  compare the `(topic, payload)` sequences exactly. A fake client with a
  `publish(topic, msg, qos, retain)` method is enough; ordering matters as much as
  content.
- **Control behaviour** — run the publisher at `--delay 0.3`, publish `pause`, confirm
  step advancement stops while the connection stays open, then `seek` and confirm the
  *next* published step is the target (not target + 1).
- **Retained metadata** — subscribe a fresh client to `sim/coesi5/_meta/run` mid-run and
  confirm it arrives immediately with `retain=True`.
- **Sensor realism** — after touching `SENSOR_SPECS` or the calibration, replay the whole
  file and confirm every type stays inside its configured bounds *and* still correlates
  with the raw series (`r > 0.97` for `indoor_temp` against `TBuilding`). Bounds alone are
  not enough: clamping everything to a constant would also satisfy them.
- **Sensor stream** — run `sensor_simulator.py --seed 42`, subscribe a *fresh* client to
  `sensors/#` mid-run and confirm the retained `_meta/sensors` arrives immediately; check
  every dropout has `value: null` and every fault has a non-null value. Confirm cadence
  independence by publishing `_control/pause` and `_control/speed` to the *simulation* and
  measuring that the sensor readings/second does not move.
- **Frontend** — `node --check` the extracted `<script>` block at minimum; the handlers
  can also be exercised in Node against a small DOM shim. For chart or table changes,
  re-run the ordering and decimation checks against **both** tabs: feed the same points in
  shuffled order and confirm the rendered points are identical to the in-order feed, and
  that incremental decimation of a growing series equals one-shot decimation. Then load the page and watch
  the transport bar track the `Step:` counter.

Scratch files, logs and harnesses go in the session scratchpad, not in the repo root.
