# CLAUDE.md

Guidance for Claude Code when working in this repository.

## What this is

A **scientific research tool**, not a production monitoring system: a district
building-energy simulation stored in HDF5 (`20260623_baseline.hdf5` — 289 datasets,
4320 steps, 600 s resolution) is replayed step-by-step by a **HELICS engine federate**,
carried by a **bridge federate** onto MQTT, and rendered in a browser dashboard that can
talk back (play/pause/step/pace and *setpoints* that change what the engine publishes
next). The HDF5 replay is a stand-in for a real HELICS-based simulation engine; the
split exists so that swapping the real engine in means replacing one federate. There
is no database, no server-side state, no build step, and no test suite.

The priorities that drive design decisions here, in order:

1. **Interactive inspection** — researchers need to pause, re-watch and *intervene* in
   specific moments, not just watch playback run. (Seek was removed with the HELICS
   split: a federation's clock cannot run backwards. It is a known regression, not a
   feature; do not reintroduce it by side-stepping the federation.)
2. **Provenance** — every run publishes what produced it, so a screenshot is
   self-describing; every setpoint is acknowledged with what was *actually* applied.
3. **Swappable engine and data source** — `bridge.py`, `sensor_simulator.py` and the
   dashboard must not change when the real engine replaces `engine.py`; the source
   layer must be replaceable without touching pacing, publishing or the dashboard.

[ARCHITECTURE.md](ARCHITECTURE.md) is the deep reference — every component, algorithm
and contract with line-level pointers. Read the relevant chapter before changing a
component; keep it updated when you change one. Its chapter 17 is the production path:
which components are temporary (the HDF5 stand-in), which are permanent, what an engine
adapter must honour, and the phased procedure for attaching the real co-simulation.

## Layout

```
run.sh                     The one entry point: up / down / restart / status / logs / dev
.env.example               Every runtime setting, documented; copy to .env (gitignored) — .env is required
config.py                  require()/optional()/resolve(): how every script reads .env; no fallbacks
docker-compose.yml         mosquitto + helics-broker (health-checked) → engine, bridge, sensors, frontend
Dockerfile                 one image for engine / bridge / helics-broker / sensors (pinned via requirements.txt)
federation.py              The HELICS contract: publication + endpoint names, payload shapes, clock
engine.py                  HELICS engine federate: HDF5 replay, control seam (apply_control), acks
bridge.py                  HELICS ↔ MQTT bridge: telemetry out (zero filter, payload), _control/* in
helics_broker.py           HELICS broker + run control: play/pause/step/pace as a time barrier; recycles per federation
sensor_simulator.py        Synthetic sensor stream (sensors/#) — own CLI, client and pacing
datasources/base.py        SimulationDataSource ABC — the swappable contract
datasources/hdf5_source.py HDF5DataSource — the ONLY module that imports h5py
replay/controller.py       ReplayController — which steps in what order, passes, dated provenance (no pacing)
mqtt_web_tester.html       The entire frontend: CSS + markup + JS in one file
mqtt_tester.py             Terminal subscriber (Rich TUI)
local_server.py            Static server + /config.json for the page + /proxy/ CORS stripper for the UrbanSim API
```

There is no `mosquitto.conf`: the compose file writes the listener config from `.env`
at start-up, so `MQTT_PORT`/`MQTT_WS_PORT` are declared once.

Dependency direction is strictly `datasources/ → replay/ → engine.py`, and
`federation.py → {engine.py, bridge.py}`. `config.py` is imported by every entry point
and imports nothing from the project. `replay/` no longer imports `paho.mqtt` at
all — it is pure pacing plus control state. `bridge.py` never imports `datasources/`
or `replay/`: everything it publishes came over HELICS. `sensor_simulator.py` is a
*sibling* of the engine: it depends on `datasources/` only, never on `replay/`,
`federation.py` or the engine.

## Running it

```bash
cp .env.example .env # once; every setting lives here and nothing has a fallback
./run.sh up          # everything: both brokers, engine, bridge, sensors, frontend (Docker Compose)
                     # → http://localhost:8002/mqtt_web_tester.html → Connect
./run.sh logs        # follow the engine (./run.sh logs bridge|helics-broker|sensors|mosquitto|frontend)
./run.sh down
```

`run.sh` is a thin wrapper over `docker-compose.yml`; the service definitions, port
mappings and health ordering live there, not in the script. The engine and bridge are
**not started until the HELICS broker's health check passes** (the bridge also waits
for Mosquitto). The HELICS federation is static — exactly two federates, no late
joining — so `engine`/`bridge` restart `on-failure` only (a clean `LOOP=0` finish stays
finished; a crash of either takes the other down within ~30 s and both restart), and
`helics_broker.py` recycles itself after every federation.

Looping is the default and is the point: without it the replay ends after the last step,
the federation dissolves, the bridge exits, and — since telemetry is QoS 0 and unretained
— the dashboard has nothing to receive and nothing is listening on `_control/#`.
`LOOP=0 ./run.sh up` gives a bounded one-shot run; `SEED=42 ./run.sh up` makes the sensor
noise reproducible (a shell export wins over `.env` for that run, as it does for Compose).
Pace is a runtime concern: `PACE` (600 simulated seconds per real second = one step per
second; `0` = free-run) is only the *initial* pace and the dashboard changes it live, so
don't bake a fast value into `.env`. `PERIOD` is the file's dt and must match on broker,
bridge and engine.

**Configuration has no fallbacks.** Every script resolves its settings through
`config.py`: a CLI flag if given, else the `.env` variable named in the flag's `--help`,
else a `config: X is not set` error naming the variable. `docker-compose.yml` uses
`${VAR:?}` for the same reason and injects `.env` into every container with `env_file`;
its only `environment:` overrides are the three addresses that must point at a container
(`MQTT_HOST`, `HELICS_BROKER_HOST`, `FRONTEND_BIND`). The page gets its broker host,
port and topic from `local_server.py`'s `/config.json`, never from values in the HTML.

Host-side iteration (what to use while changing the Python):

```bash
./run.sh dev         # Mosquitto, sensors, frontend in Docker; HELICS broker + engine + bridge from .venv
```

or the three processes by hand (the HELICS broker runs host-side in dev: a ZMQ core
needs the broker to connect *back* to each federate, which a broker inside Docker
Desktop's VM cannot reliably do to processes on the Mac):

```bash
set -a; . .env; set +a               # .env holds the host-side addresses (127.0.0.1)
.venv/bin/python helics_broker.py
.venv/bin/python bridge.py
.venv/bin/python engine.py
.venv/bin/python sensor_simulator.py --delay 0.3 --seed 42   # flags override .env
```

`.venv/` is built from `requirements.txt` (h5py, paho-mqtt, numpy, rich, helics). Use
`.venv/bin/python`, not bare `python3`. The frontend is bind-mounted into its container,
so HTML edits only need a browser reload — unless the editor *replaced* the file (new
inode: `sed -i`, `cat >`, most "atomic save" editors), which a single-file bind mount on
Docker Desktop does not follow; then `docker compose up -d --force-recreate frontend`.

Driving a running replay:

```bash
mosquitto_pub -t sim/coesi5/_control/pause -m ''
mosquitto_pub -t sim/coesi5/_control/step  -m ''                                  # one step while paused
mosquitto_pub -t sim/coesi5/_control/pace  -m '{"sim_seconds_per_second": 3600}'  # 0 = free-run
mosquitto_pub -t sim/coesi5/_control/play  -m ''
mosquitto_sub -t sim/coesi5/_meta/state -C 1      # retained {mode, pace, allowed_sim_time, period}
mosquitto_sub -t sim/coesi5/_meta/progress -C 1   # retained {step, pass, sim_time, total_steps, finished}
E=heating_frassinetto_hp2_proc_0-0.heating_frassinetto_hp2_bui_0027_0
mosquitto_pub -t sim/coesi5/_control/setpoint/$E/Qt -m '{"value": 0}'   # park the heat pump
mosquitto_sub -t sim/coesi5/_control/setpoint/$E/Qt/ack -C 1            # retained: what was applied
mosquitto_pub -t sim/coesi5/_control/setpoint/$E/Qt -n                  # clear the override
```

There is no `seek` and no `speed`. Play/pause/step/pace are answered by
`helics_broker.py`, never by the engine or the bridge; a setpoint is the only command
that reaches the engine. Unknown commands are logged and ignored.

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
- **`engine.py` + `bridge.py` must publish exactly what `hdf5_mqtt_publisher.py` did** —
  same ~175 topics, same message content and order — for the same file and flags.
  The bridge-parity check below is how that is proven; the baseline sequence is what
  the retired publisher produced. All new CLI flags are opt-in.
- **`federation.py` is the contract a real engine has to honour.** Publication and
  endpoint names, the JSON shapes, `TIME_DELTA`. Change it only with both federates,
  and never so that `bridge.py` has to know which engine is behind it.
- **The engine's `apply_control` is the seam the real physics replaces.** A setpoint
  must visibly change what is published next — it is never a no-op — and the retained
  `_control/setpoint/<entity>/<variable>/ack` must reflect what the engine *applied*
  (clamped, rejected, cleared), never merely what the bridge forwarded.
- **No decimation or downsampling in the engine or bridge.** Full fidelity is what gets
  published; rendering-side downsampling belongs in the dashboard's charting code only
  (`compressSeries` already does this).
- **HELICS time is simulation time, and pacing lives in the broker's time barrier,
  nowhere else.** The engine requests `t + period` per step and publishes when granted;
  it never sleeps and never sees play/pause/step/pace. `helics_broker.py` holds the whole
  federation back with `helicsBrokerSetTimeBarrier` and moves the barrier on a wall
  clock to pace it. This is what makes the same controls work for a real engine whose
  federates run as fast as they compute. Do not add pacing to the engine, the bridge
  or `ReplayController`.
- **Pace can only slow a run.** Never present a pace as achieved; the dashboard shows the
  actual step rate next to the requested pace, and must keep doing so.
- **HELICS calls happen on one thread.** Paho delivers on its own network thread; the
  bridge queues commands and sends them from the federation loop. Do not call `helics*`
  from an MQTT callback.
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
  must keep streaming at its own cadence while the run is paused, stepped or re-paced.
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
- **The page is styled with coesi-frontend-main's tokens.** Section 1 of the `<style>`
  block is `src/index.css`'s `--coesi-*` palette copied verbatim (paste over it when the
  frontend's changes; never edit it here); section 2 (`--proto-*`) holds what this page
  needs that the frontend has no token for yet, and is what to add to `index.css` when
  merging. Component CSS and the script use only those tokens — the script reads them
  through `tokens()`, so there are no colour literals in JS. Dark mode is
  `<html data-theme>` from `localStorage["coesi.theme"]`, exactly as the frontend's
  `ThemeContext` does; never `@media (prefers-color-scheme)` in CSS.
- **The header carries controls only, with fixed geometry.** Tabs, Config, Connect,
  play/pause/step, pace, Run info, Logs, the connection dot — every item a set width, no
  wrapping, the empty spacer the only thing that flexes. Anything that *changes while the
  run advances* (step, simulated time, progress, run metadata) goes in the **Run drawer**,
  never in the header: a growing number moves everything to its right. Broker host, port
  and topic live behind the **Config** drop-down; anything else set once per session
  belongs there too. Keep the input ids (`host`, `port`, `topic`) — `toggleConnection()`
  and `controlBase()` read them by id — and the readout ids (`lblStep`, `lblTime`,
  `lblScrub`, `lblRate`, `scrubber`, `metaBadge`, `metaPopover`), which the transport code
  writes wherever they live. Never put `overflow: hidden` on the header: it clips the
  dropdown panels. New popovers use `.dropdown-panel` + `closeDropdowns()`; new side
  panels use `.drawer` + `toggleDrawer()` — one of each open at a time.
- `.env.example` keeps comments on their own lines: Docker's env-file parser treats a
  trailing `# comment` as part of the value (it once handed the engine
  `SIM_START='# optional…'`).
- Prefer relocating code over rewriting it when refactoring — a pure move stays
  reviewable, and byte-identical output is then verifiable.

## Verifying a change

There is no test suite, so verify against the real thing:

- **Bridge parity** — run `helics_broker.py --pace 0` + `bridge.py` + `engine.py
  --end-step 4` host-side with `mosquitto_sub -t 'sim/coesi5/#' -F '%t\t%p'` capturing,
  and compare the `(topic, payload)` sequence against the baseline the retired
  `hdf5_mqtt_publisher.py` produced for the same steps (869 telemetry messages, in
  order; `_meta/run` equal apart from `started_at`). Ordering matters as much as content.
- **Run control** — with `--loop` and `--pace 1200`, count bridge "Published step" lines
  over 5 s (expect ~10); publish `pause`, confirm the count stops; publish `step` twice
  and confirm exactly two more; publish `pace 6000` + `play` and confirm ~10 steps/s;
  `pace 0` and confirm free-run (>100 steps/s). `_meta/state` must be retained and
  reflect each transition; a setpoint published while paused must be acknowledged right
  after `play`.
- **Setpoint round-trip** — publish `_control/setpoint/<entity>/<variable>` with a
  value, confirm the ack arrives retained (`mosquitto_sub -F '%r'` prints `1`) with the
  applied value, and that the next telemetry for that variable — and its coupled one
  (`TBuilding` for `ZoneSetPoint`, `En_el` for `Qt`) — changes within one step. Then an
  out-of-range value (ack says clamped), an unknown entity (ack says rejected,
  `applied: null`), and an empty payload (ack says cleared; telemetry returns to the
  recorded value).
- **Late-subscriber correctness** — a fresh client subscribing to
  `_control/setpoint/#` mid-run sees every last-applied ack immediately; the
  dashboard's setpoint panel must show *that*, not the last thing clicked.
- **Retained metadata** — subscribe a fresh client to `sim/coesi5/_meta/run` mid-run and
  confirm it arrives immediately with `retain=True`.
- **Sensor realism** — after touching `SENSOR_SPECS` or the calibration, replay the whole
  file and confirm every type stays inside its configured bounds *and* still correlates
  with the raw series (`r > 0.97` for `indoor_temp` against `TBuilding`). Bounds alone are
  not enough: clamping everything to a constant would also satisfy them.
- **Sensor stream** — run `sensor_simulator.py --seed 42`, subscribe a *fresh* client to
  `sensors/#` mid-run and confirm the retained `_meta/sensors` arrives immediately; check
  every dropout has `value: null` and every fault has a non-null value. Confirm cadence
  independence by publishing `_control/pause` and `_control/pace` to the *simulation* and
  measuring that the sensor readings/second does not move.
- **Frontend** — `node --check` the extracted `<script>` block at minimum; the handlers
  can also be exercised in Node against a small DOM shim. For chart or table changes,
  re-run the ordering and decimation checks against **both** tabs: feed the same points in
  shuffled order and confirm the rendered points are identical to the in-order feed, and
  that incremental decimation of a growing series equals one-shot decimation. Then load the page and watch
  the transport bar track the `Step:` counter.

Scratch files, logs and harnesses go in the session scratchpad, not in the repo root.
