# Live Simulation Viewer

A browser dashboard for watching a district energy simulation run as a HELICS
co-simulation, and for interfering with it while it runs: pause it, advance one step,
slow it down or let it free-run, send a setpoint to the engine and see the effect in
the next step. A synthetic sensor network runs next to the simulation so the dashboard
can show "measured" readings side by side with the model's ground truth.

The simulation engine is currently a stand-in. Instead of a live model, `engine.py`
replays a recorded run from an HDF5 file written by the COESI/UrbanSim engine
(`20260623_baseline.hdf5`: 289 time series over 41 entities, 4320 steps of 600 s). It
joins a HELICS federation as the engine federate and publishes one step at a time. A
second federate, `bridge.py`, forwards each step to MQTT and passes setpoints from the
dashboard back to the engine. `sensor_simulator.py` reads the same file and publishes
noisy, sometimes missing or faulty sensor readings on a separate topic tree. The
dashboard is one HTML file with no build step. It subscribes to everything and draws a
table, a chart, a sensor view and a 3D map.

The point of the replay is to have the whole chain working before the real engine
exists. When the CosimGym-based engine is ready it takes the place of `engine.py` and
nothing else changes: the bridge, the dashboard and the sensors only depend on the
HELICS contract in `federation.py`. For the same reason, play, pause, step and pace are
not implemented in the engine. They are done by the HELICS broker with a time barrier,
which works for any federate that asks for time, including ones that were never written
with a pause button in mind. See *Migration to the real engine* below.

There is no database, no server-side state and no test suite; changes are verified
against the running system (CLAUDE.md lists the checks). Two rules shaped most
decisions: every run publishes what produced it, and every setpoint is acknowledged
with what the engine actually applied, so a screenshot or a log line can always be
traced back to its source.

---

## System architecture

```mermaid
flowchart LR
    subgraph data["Data"]
        H5[("20260623_baseline.hdf5<br/>289 series · 4320 steps · 600 s")]
    end

    subgraph fed["HELICS federation — Docker Compose"]
        direction TB
        DS["datasources/<br/>HDF5DataSource<br/><i>only module importing h5py</i>"]
        RC["replay/<br/>ReplayController<br/>which steps, provenance"]
        ENG["engine.py — engine federate<br/>request_time(t + 600 s) · apply_control · acks"]
        HB(("helics_broker.py<br/>broker + run control<br/><b>time barrier</b>"))
        BRG["bridge.py — bridge federate<br/>zero-filter · payload · setpoints in"]
    end

    subgraph mq["MQTT — Docker Compose"]
        BR(("Mosquitto<br/>1883 TCP<br/>9001 WS"))
        SEN["sensor_simulator.py<br/>calibrate · noise · dropouts · faults"]
        SRV["local_server.py<br/>static + /proxy/"]
    end

    subgraph browser["Browser"]
        UI["mqtt_web_tester.html<br/>Data Table · Live Chart + Setpoints · Sensors · 3D Map"]
    end

    API[("UrbanSim API<br/>building GeoJSON")]

    H5 --> DS
    DS --> RC --> ENG
    H5 --> SEN
    ENG -- "engine/run (once)<br/>engine/step (per step)" --> HB --> BRG
    HB -. "barrier holds every federate" .-> ENG
    BRG -- "setpoint" --> HB -.-> ENG
    ENG -. "setpoint_ack · bye" .-> HB
    BRG -- "sim/coesi5/&lt;entity&gt;/&lt;var&gt;<br/>_meta/run · _meta/progress · _meta/controls<br/>_control/setpoint/…/ack (all retained)" --> BR
    HB -- "_meta/state (retained)" --> BR
    BR -- "_control/play · pause · step · pace" --> HB
    BR -- "_control/setpoint/…" --> BRG
    SEN -- "sensors/&lt;building&gt;/&lt;type&gt;" --> BR
    BR -- "WebSocket 9001" --> UI
    UI -- "sim/coesi5/_control/*" --> BR
    SRV -- "http://localhost:8002" --> UI
    UI -- "/proxy/…" --> SRV --> API
```


Six containers, one entry point (`./run.sh up`):

| service | what it is | why it exists |
|---|---|---|
| `mosquitto` | Eclipse Mosquitto with two listeners | Browsers speak MQTT only over WebSockets (9001); Python clients use TCP (1883) |
| `helics-broker` | `helics_broker.py` | The HELICS broker **and the run control**: play/pause/step/pace over MQTT, done with a time barrier on the whole federation; health-checked; recycles after every federation |
| `engine` | `engine.py` | The engine federate: publishes each step at its simulation time, as fast as the broker lets it; applies setpoints and acknowledges them |
| `bridge` | `bridge.py` (same image) | The bridge federate: turns each HELICS step into ~175 MQTT topics plus retained `_meta/*`, turns `_control/setpoint/*` into HELICS messages |
| `sensors` | `sensor_simulator.py` (same image) | A believable sensor network derived from the same file, on its own cadence, no control channel |
| `frontend` | `local_server.py` | Serves the dashboard and strips CORS from the UrbanSim GeoJSON API |

Dependency direction in the Python is strict — `datasources/ → replay/ → engine.py`, and
`federation.py → {engine.py, bridge.py}`. `datasources/hdf5_source.py` is the only module
that imports `h5py` (a data-source swap touches one construction site); `replay/` imports
neither `paho.mqtt` nor `helics` (it only picks steps and writes provenance); `bridge.py`
imports neither `datasources/` nor `replay/` (everything it publishes arrived over HELICS).
That last line is the migration guarantee: **`federation.py` is the interface the real
engine has to speak, and `bridge.py`, the broker's run control, the sensors and the
dashboard do not change when it does.**

## How a step flows

```mermaid
sequenceDiagram
    autonumber
    participant K as Broker (run control)
    participant E as Engine (federate)
    participant P as Bridge (federate)
    participant B as Mosquitto
    participant D as Dashboard

    Note over K: barrier set before anyone joins: nothing runs ahead
    E->>P: HELICS engine/run {metadata, catalog, controls}
    P->>B: _meta/run · _meta/controls — retained
    K->>B: _meta/state {mode, pace} — retained

    loop every step — as fast as the barrier allows
        E->>K: request_time(t + 600 s)
        K-->>E: granted when the barrier has passed t + 600
        E->>P: HELICS engine/step {step, pass, values}
        P->>P: latching zero filter
        P->>B: ~175 × sim/coesi5/entity/var {step, total_steps, dataset, attributes, values}
        P->>B: _meta/progress {step, pass, sim_time} — retained
        B-->>D: table, chart, map repaint once — progress shows the simulated date
    end

    Note over D,K: pause · step · pace
    D->>B: _control/pause
    B-->>K: barrier frozen at the federates' current time
    D->>B: _control/step
    B-->>K: barrier moved by one period → exactly one more step
    D->>B: _control/pace {"sim_seconds_per_second": 3600}
    B-->>K: barrier advances 3600 sim-s per real second

    Note over D,E: setpoint
    D->>B: _control/setpoint/&lt;entity&gt;/Qt {"value": 0}
    B-->>P: setpoint listener → queue
    P->>E: HELICS {"command": "setpoint", …}  (delivered at the next grant)
    E->>E: validate · clamp · store override — next step publishes it
    E->>P: HELICS {"command": "setpoint_ack", applied, accepted, reason, step}
    P->>B: …/Qt/ack — retained
    B-->>D: panel shows "applied 0 at step n"
```


Two things in that diagram are deliberate. The engine has **no pacing code at all**: it
asks for the next step's time and publishes when granted, which is what a real model
federate does; pause and pace exist only as the broker's barrier. And there is **no seek**:
a federation's clock only runs forward, so the progress bar is read-only and **Step** (one
step at a time while paused) is the honest replacement for scrubbing. Browsing history
properly needs the run's stored results (InfluxDB) — see *Known limitations*.

The sensor simulator runs its own read loop independently — **it has no control
channel**, so it keeps reporting at each sensor type's own interval while the replay is
paused or sped up. That independence is the point: sensors in the real world do not care
what the analyst is doing.

---

## Quick start

Prerequisites: **Docker Desktop** (or Docker Engine with the Compose plugin). Nothing
else — Python only matters for host-side development.

```bash
git clone https://github.com/dshaterzadeh/Live_simulatation_viewer.git
cd Live_simulatation_viewer
cp .env.example .env       # once — every runtime setting lives here; run.sh refuses to start without it
./run.sh up
```

Then open **http://localhost:8002/mqtt_web_tester.html** and click **Connect**.

`run.sh up` builds the image once, starts both brokers, waits for their health checks,
and then starts the engine, bridge, sensor simulator and dashboard server. The stack keeps
running until `./run.sh down`.

`.env` is the single source of truth and **nothing has a fallback**: `docker-compose.yml`
injects it into every container, `run.sh` exports it for host-side runs, and every Python
entry point reads it through `config.py` — a CLI flag if given, else the variable named in
the flag's `--help`, else a `config: X is not set` error. A stale or half-copied `.env`
therefore stops with the variable's name rather than silently running against a made-up
port. The dashboard gets its broker host, port and topic from the same file via
`local_server.py`'s `/config.json`; nothing is hard-coded in the HTML.

```bash
./run.sh status            # container states + the URL
./run.sh logs              # follow the engine   (also: logs bridge|helics-broker|sensors|mosquitto|frontend)
./run.sh restart
./run.sh down
```

The settings you are most likely to touch, with the value `.env.example` ships. A
variable exported in the shell wins over `.env` for one run (`LOOP=0 ./run.sh up`), as it
does for Compose; `.env.example` documents every other one.

| variable | in `.env.example` | effect |
|---|---|---|
| `PACE` | `600` | Initial pace in **simulated seconds per real second** (600 = one 10-minute step per second). `0` = free-run. Changed live from the dashboard, so don't bake a fast value in |
| `PERIOD` | `600` | Simulated seconds per step — the file's dt. Broker, bridge and engine all read it |
| `LOOP` | `1` | `LOOP=0 ./run.sh up` replays once; the run then reads *finished* and the federation dissolves |
| `SEED` | blank (random) | `SEED=42 ./run.sh up` makes sensor noise, dropouts and faults reproducible |
| `SENSOR_DELAY` | `1.0` | Baseline pace of the sensor simulator, independent of `PACE` |
| `SIM_START` | blank (today, local midnight) | Calendar instant of simulation time 0 (ISO 8601). Progress and the Live Chart axis are dated from it |
| `MQTT_PORT` / `MQTT_WS_PORT` | `1883` / `9001` | Mosquitto's TCP and WebSocket listeners — declared once here; the compose file writes the listener config from them |
| `TOPIC` / `SENSORS_TOPIC` | `sim/coesi5` / `sensors` | Base topics for telemetry (+ `_meta/*`, `_control/*`) and for the sensor stream |
| `FRONTEND_PORT` | `8002` | Where `local_server.py` serves the page |

`SEED` and `SIM_START` are the only optional ones — blank means "not set", never a guess.
Keep comments in `.env` on their own lines: Docker's env-file parser treats a trailing
`# comment` as part of the value.

Looping is on by default for a reason: telemetry is QoS 0 and unretained, so an engine
that exits after one pass dissolves the federation and leaves the dashboard with nothing
arriving and no one listening for control commands — it looks broken when it has simply
finished. With `LOOP=0` the engine and bridge exit cleanly after the pass and stay down;
the HELICS broker recycles itself and waits for the next pair.

## Using the dashboard

**Run control** (top): ▶/❚❚ play–pause, ⏭ **step** (exactly one step, while paused), a
read-only progress bar with the **simulated date** and percentage, a **pace** selector
(real time · 1 min/s · 10 min/s · 1 h/s · 6 h/s · 1 day/s · free-run) and the **actual
rate** the engine is achieving. These publish to `sim/coesi5/_control/*` and are answered
by the HELICS broker, which holds the whole federation with a time barrier — so every
connected browser sees the same run, and a reloaded page shows the true state (it is
retained on `_meta/state`). Pace can only slow a run down; nothing makes the engine compute
faster, which is why the actual rate is shown next to the requested one. The `run:` badge
shows which file produced what you are looking at and the dated span it covers, with the
full provenance record on hover.

**Data Table** — rows are buildings, column groups are model entities, cells are
colour-coded by magnitude (red positive, blue negative). Columns appear as series become
active. *Last* / *Max* toggles between the latest value and the running maximum. A
drawer at the bottom shows the raw message log.

**Live Chart** — pick up to 8 buildings and a shared parameter; the series build as steps
arrive, with a crosshair tooltip and a full-timeline or last-400-steps range. The x-axis
is **simulated calendar time** (`start_time + step × dt` from `_meta/run`); storage stays
keyed by step. A new pass (or, with a real engine, a new RL episode) starts the chart
over instead of overwriting the previous one point by point.

The sidebar's **Setpoint → engine** panel is the two-way loop. Its buildings and inputs
come from the engine's retained **controls catalog** (`_meta/controls`: entity, variable,
units, min/max, and which variables move with it) — nothing is hard-coded in the page.
Pick a building and an input, type a value, **Apply**. The engine substitutes it for the
recorded value from the next step on and moves the coupled variable (`TBuilding` follows
`ZoneSetPoint`; `En_el` scales with `Qt`, so `0` parks the pump). The status line shows
only what the engine *acknowledged*: `applied 26 at step 153`, `clamped to [10, 30]`,
`rejected: …`, or `no override` after **Clear**. Acks are retained, so a reloaded page
shows the true state before any telemetry arrives. A setpoint sent while paused is applied
on the next step after play.

**Sensors** — the same chart and table machinery pointed at the `sensors/#` stream. The
x-axis is *real time* (each reading carries a timestamp), ticks land only on instants a
reading exists for, a **dropout draws a visible gap** in the line and an em dash in the
table, and a **fault** is plotted but ringed in amber. The sensor list, units and columns
come from the retained `sensors/_meta/sensors` message, so adding a sensor type in Python
needs no frontend change.

**3D Map** — enter a UrbanSim project id, **Load Map**, pick a parameter. Buildings are
extruded and coloured by their live value. The *Ground truth / Sensor reading* toggle
switches the colour source; in sensor mode a building with no current trustworthy reading
renders in a flat grey that is deliberately outside the value scale.

Broker host, port and base topic live behind the **Config** drop-down, pre-filled from
`/config.json` (i.e. from `.env`). Anything that changes while the run advances — step,
simulated time, progress, provenance — lives in the **Run** drawer, so the header keeps a
fixed geometry. The page is styled with coesi-frontend-main's `--coesi-*` design tokens
(copied verbatim into the first `<style>` section, with a small `--proto-*` section for
what this page needs and the frontend has no token for yet), and follows the same
`<html data-theme>` / `localStorage["coesi.theme"]` dark-mode convention, so it drops
into that frontend without a restyle.

## Driving the replay from a terminal

Anything that can publish MQTT can drive the run or push a setpoint:

```bash
mosquitto_pub -t sim/coesi5/_control/pause -m ''
mosquitto_pub -t sim/coesi5/_control/step  -m ''                                  # one step, while paused
mosquitto_pub -t sim/coesi5/_control/pace  -m '{"sim_seconds_per_second": 3600}'  # 1 simulated hour per second; 0 = free-run
mosquitto_pub -t sim/coesi5/_control/play  -m ''
mosquitto_sub -t sim/coesi5/_meta/state -C 1                                      # retained: {mode, pace, allowed_sim_time, period}
mosquitto_sub -t sim/coesi5/_meta/progress -C 1                                   # retained: {step, pass, sim_time, total_steps, finished}

E=heating_frassinetto_hp2_proc_0-0.heating_frassinetto_hp2_bui_0027_0
mosquitto_pub -t sim/coesi5/_control/setpoint/$E/Qt -m '{"value": 0}'          # park the heat pump
mosquitto_sub -t sim/coesi5/_control/setpoint/$E/Qt/ack -C 1 -F '%r %p'        # 1 = retained; shows what was applied
mosquitto_pub -t sim/coesi5/_control/setpoint/$E/Qt -n                         # clear the override
```

There is also a terminal subscriber with a live table and stats panel:

```bash
.venv/bin/python mqtt_tester.py -t 'sim/coesi5/#'
```

## Wire contract

Topics: `sim/coesi5/<entity>/<variable>`, for example
`sim/coesi5/heating_frassinetto_hp2_proc_0-0.heating_frassinetto_hp2_bui_0027_0/COP`.
Payload, one per topic per step:

```json
{"step": 2211, "total_steps": 4320,
 "dataset": "heating_frassinetto_hp2_proc_0-0.heating_frassinetto_hp2_bui_0027_0/COP",
 "attributes": {}, "values": 3.1336430317848407}
```

A sensor reading, for comparison:

```json
{"step": 224, "t": 134400.0, "timestamp": "2026-09-15T13:20:00+00:00",
 "sensor_type": "indoor_temp", "building": "bui_0001_0",
 "value": 21.8, "status": "ok", "unit": "C"}
```

Reserved sub-namespaces under the same base topic:

| topic | direction | notes |
|---|---|---|
| `_meta/run` | bridge → world | run provenance incl. `start_time`, `end_time`, `dt_seconds`; retained, QoS 1 |
| `_meta/state` | broker → world | `{mode: paced\|free\|paused, pace, allowed_sim_time, period, at}`; retained |
| `_meta/progress` | bridge → world | `{step, pass, sim_time, total_steps, finished, at}`; retained |
| `_meta/controls` | bridge → world | `[{entity, variable, units, min, max, couples}]`; retained |
| `_control/play`, `_control/pause`, `_control/step` | world → broker | empty payload |
| `_control/pace` | world → broker | `{"sim_seconds_per_second": 3600}`; `0` = free-run |
| `_control/setpoint/<entity>/<variable>` | world → engine (via bridge) | `{"value": 26}`; empty or `{"value": null}` clears |
| `_control/setpoint/<entity>/<variable>/ack` | engine → world | `{entity, variable, requested, applied, accepted, reason, step, at}`; retained, QoS 1 |

There is no `_control/seek` and no `_control/speed`. New behaviour goes on new reserved
topics — the telemetry payload shape is fixed, which is also why there is no `sim_time`
field in it: it is `start_time + step × dt_seconds`, both in `_meta/run`.

### Where topic names live

Three layers, treated differently on purpose:

- **Base topics are configuration.** `TOPIC` (`sim/coesi5`) and `SENSORS_TOPIC`
  (`sensors`) come from `.env`. The bridge, broker and sensor simulator read them
  through `config.py`; the page gets them from `/config.json`. Two stacks on one
  Mosquitto only need two values of `TOPIC`.
- **The reserved suffixes are protocol.** `_meta/run`, `_meta/state`, `_meta/progress`,
  `_meta/controls`, `_control/play|pause|step|pace`, `_control/setpoint/…`, `…/ack` and
  `sensors/_meta/sensors` are literals in `bridge.py`, `helics_broker.py`,
  `sensor_simulator.py` and the page. They are not settings, deliberately: they are the
  agreement between those four processes, and a setting would let two of them disagree.
- **Entity and variable segments are data.** The bridge builds
  `<TOPIC>/<entity>/<variable>` from the keys of the `values` object in each
  `engine/step` message, and the page builds its building and model lists from what
  arrives. Neither holds a list of names. (One leftover: the page groups buildings with
  a `bui_\d+` regex; migration phase 1 replaces it with the engine's catalog.)

On the HELICS side the four names in `federation.py` (`engine/run`, `engine/step`,
`engine/control`, `bridge/control`) are hardcoded, and that is the contract. CosimGym's
federates publish differently, one typed value per variable named
`<federate>.<instance>/<var>`, and that difference is absorbed by the adapter translating
into `engine/step`, not by the bridge learning CosimGym's names. If it later proves
better for the bridge to subscribe to the federation's publications directly, HELICS can
list them at runtime (a broker query for `publications`), so the bridge could discover
them instead of being told. That would be a change to `federation.py` and `bridge.py`
together, and nothing on the MQTT side would notice, because none of the three layers
above depends on a HELICS name.

Between the two federates the contract is `federation.py`: one string publication
`engine/run` (metadata + attribute catalog + controls, once), one string publication
`engine/step` (`{"step", "pass", "values": {entity: {variable: value}}}`, per step), and
two endpoints (`engine/control` for setpoints in, `bridge/control` for acks and a farewell
`bye` out). **HELICS time is simulation time**: the engine requests `t + period` per step
and never sleeps. The broker's time barrier is the only thing that ever holds it back.

Sensor readings live on `sensors/<building>/<type>` and always carry
`status: "ok" | "dropout" | "fault"`. A dropout has `value: null` — never `0`, never a
missing key — which is what lets every renderer tell "no data" from "measured zero".
Sensor values are calibrated into a plausible per-type range before noise is applied
(the raw `TBuilding` is legitimately 7–19 °C when unheated; a thermostat would never
display that), so they track the simulation's shape (r ≈ 0.99) but are not directly
comparable to its absolute numbers. `--no-calibration` publishes raw values.

Full details — payload schema, QoS choices, the zero filter, calibration maths, every
frontend function — are in [ARCHITECTURE.md](ARCHITECTURE.md).

## Project layout

```
run.sh                      one entry point: up / down / restart / status / logs / dev
.env.example                every runtime setting, documented; copy to .env (gitignored) — .env is required
config.py                   require() / optional() / resolve(): how every script reads .env; no fallbacks
docker-compose.yml          mosquitto (listeners written from .env) + helics-broker (health-checked) → engine, bridge, sensors, frontend
Dockerfile                  one image for engine / bridge / helics-broker / sensors, deps pinned via requirements.txt
federation.py               the HELICS contract: publication + endpoint names, payload shapes, clock
engine.py                   engine federate — HDF5 replay at simulation time, apply_control (the seam the real physics replaces), acks
bridge.py                   bridge federate — HELICS → MQTT telemetry + retained _meta/*, MQTT setpoints → HELICS
helics_broker.py            HELICS broker + run control: play/pause/step/pace as a time barrier; recycles per federation
sensor_simulator.py         synthetic sensor stream — own CLI, own client, own pacing
datasources/base.py         SimulationDataSource — the swappable source contract
datasources/hdf5_source.py  HDF5DataSource — the only module that imports h5py
replay/controller.py        ReplayController — which steps in what order, passes, dated provenance (no pacing)
mqtt_web_tester.html        the entire frontend, no framework, no build step
mqtt_tester.py              terminal subscriber (Rich TUI)
local_server.py             static server + /config.json for the page + /proxy/ CORS stripper for the GeoJSON API
20260623_baseline.hdf5      the reference simulation output (10 MB)
ARCHITECTURE.md             the deep reference: every component, algorithm and contract (ch. 17: the production path)
broker-pacing-pattern.md    the broker-owns-pacing pattern as a standalone note, independent of this code
CLAUDE.md                   working conventions and invariants for the codebase
```

There is no `mosquitto.conf`: the compose file writes the listener config from `.env` at
start-up, so `MQTT_PORT` / `MQTT_WS_PORT` are declared once.

## Development

Mosquitto never changes while iterating, the federation does. `dev` keeps Mosquitto,
the sensors and the dashboard server in Docker and runs the HELICS broker, bridge and
engine from a local venv in the foreground, all stopped by one Ctrl+C:

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt   # once
./run.sh dev
```

(The HELICS broker runs on the host in dev mode rather than in Docker: a ZMQ core needs
the broker to connect *back* to each federate, which a broker inside Docker Desktop's VM
cannot reliably do to processes on the Mac. The three processes take no arguments —
`run.sh` has exported `.env`, whose host-side addresses are `127.0.0.1`. To run them by
hand instead: `set -a; . .env; set +a`, then `.venv/bin/python helics_broker.py`,
`bridge.py`, `engine.py`; a CLI flag such as `--pace 0` overrides `.env` for that process.)

The dashboard is bind-mounted into its container, so editing `mqtt_web_tester.html` only
needs a browser reload — unless the editor *replaced* the file (new inode: `sed -i`,
most "atomic save" editors), which a single-file bind mount on Docker Desktop does not
follow; then `docker compose up -d --force-recreate frontend`.

Other useful commands:

```bash
.venv/bin/python engine.py -f 20260623_baseline.hdf5 --explore                  # list datasets
.venv/bin/python sensor_simulator.py --list-types                             # types, ranges, cadences
.venv/bin/python sensor_simulator.py -f 20260623_baseline.hdf5 --sim-start 2026-01-12T00:00:00
```

There is no test suite; changes are verified against the running system. The checks
worth repeating are listed under *Verifying a change* in [CLAUDE.md](CLAUDE.md) —
bridge parity (the engine+bridge pair publishes byte-for-byte what the old single
publisher did), control behaviour, the setpoint round-trip, late-subscriber correctness,
retained metadata, sensor realism and the chart's ordering/decimation invariants.

## Migration to the real engine

The HDF5 replay is temporary. The migration replaces the *left* of this picture and
touches nothing on the *right*; the middle is the interface a replacement has to honour.

```mermaid
flowchart LR
  subgraph T["TEMPORARY — deleted at swap"]
    direction TB
    F["20260623_baseline.hdf5"]
    S["datasources/hdf5_source.py"]
    C["replay/controller.py"]
    E["engine.py"]
    F --> S --> C --> E
  end
  subgraph M["THE SEAM — kept"]
    direction TB
    W["federation.py<br/>engine/run · engine/step<br/>engine/control · bridge/control"]
    N["engine adapter (NEW)<br/>wraps CosimGym, speaks federation.py"]
  end
  subgraph P["PERMANENT — unchanged"]
    direction TB
    B["helics_broker.py<br/>time-barrier run control"]
    R["bridge.py"]
    Q["mosquitto"]
    D["dashboard + local_server.py"]
    X["sensor_simulator.py"]
    K["config.py · .env · compose · run.sh"]
  end
  E -- "HELICS" --> W --> R
  N -. "plugs in here" .-> W
  B -. "barrier holds any federate" .- W
```

### What is deleted, what stays, what is built

| | component | at swap |
|---|---|---|
| **temporary** | `engine.py`, `replay/controller.py`, `datasources/hdf5_source.py`, the `.hdf5`, `HDF5_FILE` in `.env` | deleted in one cut — a live engine computes forward, it has no "pass over a recording" |
| **seam** | `federation.py` (publication + endpoint names, payload shapes, clock), `datasources/base.py` (source ABC) | kept — the adapter targets `federation.py`; the ABC gains a "next available step" variant for a live source |
| **permanent** | `helics_broker.py`, `bridge.py`, Mosquitto, the dashboard, `local_server.py`, `sensor_simulator.py`, `config.py` / `.env` / compose / `run.sh` | unchanged — none of them ever depended on what produces the data |
| **new** | **the engine adapter**: one federate wrapping CosimGym's `ScenarioManager` / `BaseFederate`s | selected by an `ENGINE=hdf5\|cosim` switch in `.env`; runs against the same broker, bridge and page |

### What the adapter must honour

This is `federation.py`, and it is the entire contract:

1. Join the federation as **`engine`** (the broker's liveness check looks for the
   `engine_` core-name prefix).
2. Publish **`engine/run` once**: `{metadata, catalog, controls}` — provenance, the
   entity/variable catalog the dashboard builds its lists from, and the controls
   catalog (`entity, variable, units, min, max, couples`) that populates the setpoint
   panel. Nothing about entities is hard-coded downstream.
3. Publish **`engine/step` per granted step**: `{"step", "pass", "values": {entity:
   {variable: value}}}`. CosimGym's own convention is `<federate>.<instance>/<var>`,
   one typed publication per variable — the **adapter translates** into this shape,
   the bridge does not learn a second one.
4. **Request time honestly**: `request_time(t + period)`, publish when granted, never
   sleep. That is all a federate does anyway — and it is what lets the broker's barrier
   pause, step and pace it without CosimGym having any such feature.
5. Answer **`engine/control` setpoints** with **`bridge/control` acks** that say what
   was *applied* — clamped, rejected (`applied: null`), or cleared — never merely what
   was requested. `apply_control()` in `engine.py` is the stand-in for this; the real
   physics replaces it.
6. Send **`bye`** before leaving, so the bridge finishes cleanly instead of timing out.

The one topological requirement: **run control lives in the process that owns the root
HELICS broker**, because the time barrier is a broker call. Either CosimGym's federation
connects to `helics_broker.py` (the `.env` `HELICS_BROKER_HOST/PORT` already point at it —
the natural route), or its `ScenarioManager` keeps spawning its own broker and the
~100-line `RunControl` class moves into that process. Two knobs change: `--federates 2`
becomes the real count, `PERIOD` the real scenario's step. Pause granularity is one step
of whichever federate is slowest, because a federate is held at its *next* request.

### Procedure — five phases, the system running throughout

Every phase ends with the same proof: the bridge-parity, run-control and setpoint checks
in [CLAUDE.md](CLAUDE.md) pass unchanged, because nothing right of the seam is allowed to
notice.

| phase | work | gate |
|---|---|---|
| **0 — done** | One `.env`, no fallbacks; the page on coesi-frontend-main's tokens, configured from `/config.json` | static checks, `docker compose config`, DOM-shim smoke test |
| **1 — harden the seam** | Dashboard builds building/model lists from `engine/run`'s `catalog`, not the `bui_\d+` regex. Decide the sensor simulator's live data path. Script the bridge-parity check | parity against today's baseline; dashboard renders a renamed entity set |
| **2 — adapter, side by side** | Add the adapter as a second engine entry point, `ENGINE=hdf5\|cosim` picks which the `engine` service runs. The replay stays as the regression reference | both engines pass run-control and setpoint checks against the same broker, bridge and page |
| **3 — sensors** | Point the simulator at the live engine's values, or retire it for real sensor ingestion (CosimGym interface federates), keeping the `sensors/*` contract and `status` semantics | cadence independence, dropout `null`, retained `_meta/sensors` |
| **4 — frontend merge** | Port the page into coesi-frontend-main as a route: tokens already shared, `/config.json` → the app's config, the Paho client → a hook, the chart kept with its ordering/decimation checks, the map onto the app's deck.gl; `local_server.py` dissolves into the app's server | the frontend checks, in the merged app |
| **5 — retire the stand-in** | Delete `engine.py`, `datasources/hdf5_source.py`, `replay/`, the `.hdf5`, `HDF5_FILE`, the `ENGINE` switch. Production hardening: MQTT auth + TLS in `.env`, drop `allow_anonymous` | the full check list once, on the production stack |

Chapter 17 of [ARCHITECTURE.md](ARCHITECTURE.md) is the per-function version of this —
including what deliberately stays hardcoded (contract vs. structure vs. temporary) and
why there is no YAML yet.

## Known limitations and next steps

- **Local-machine trust model.** The broker allows anonymous connections and
  `_control/*` is a write path; the `/proxy/` endpoint fetches any URL. The proxy is
  loopback-only, but do not run this stack on an untrusted network.
- **No seek, and no history yet.** A federation's clock cannot run backwards, so the
  progress bar is read-only and *Step* is the precise tool instead. Browsing earlier
  moments properly means reading the run's stored results — the real engine writes
  every step to InfluxDB — through a "History" mode on the chart and table. That needs
  the store to exist in this stack first; it is the next item.
- **One step blob over HELICS, not one publication per variable.** The real engine
  publishes each variable separately (`building_federate.0/T_indoor`, typed, with units);
  the bridge will register those subscriptions from the scenario YAML. Its publishing
  code does not change — only where `values` come from.
- **The federation is static and heals in ~10 s.** Exactly two federates, no late joining.
  If the engine or bridge dies, the broker notices, tears the federation down and Docker
  restarts both. A restarted engine starts with no overrides while the last retained acks
  still say "applied" — re-apply from the dashboard.
- **`apply_control` is a legible stand-in**, not physics: it substitutes the commanded
  value and moves one coupled variable predictably. It exists so a dashboard action
  changes a chart line within one step.
- **Pace only slows.** The barrier can hold a federation back; it cannot make a slow model
  step faster. The dashboard shows the actual rate next to the requested one for that
  reason.
- **No retained telemetry snapshot.** A browser connecting mid-run fills in progressively
  rather than instantly.
- **Chart and table are hand-rolled SVG/DOM.** They are correct and parameterised for both
  tabs, but the table repaints every cell each frame and the chart carries a decimation
  cache it does not need. Dirty-cell painting and dropping the cache are straightforward;
  replacing the SVG with a canvas library such as uPlot (null-as-gap, time and numeric
  axes built in) is the larger option if the chart keeps growing.
- **Whole-file eager load.** Fine at 10 MB; a multi-GB run would need chunked reads.
- **The UrbanSim API is internal** to Politecnico di Torino; the 3D map needs network
  access to it. Everything else works without it.

## Author

David Shaterzadeh — COESI / Politecnico di Torino.
