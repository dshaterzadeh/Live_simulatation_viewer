FROM python:3.11-slim

WORKDIR /app

# Pinned in requirements.txt so the container matches the host .venv exactly.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Scripts only. The data file is bind-mounted by docker-compose.yml (HDF5_FILE)
# and every setting comes from the environment (config.py), so the image has
# no arguments and nothing deployment-specific baked in.
COPY config.py engine.py bridge.py federation.py helics_broker.py sensor_simulator.py mqtt_tester.py ./
COPY datasources/ ./datasources/
COPY replay/ ./replay/

# One image, four entrypoints: the `engine` service runs this default and
# docker-compose.yml overrides it for `bridge`, `helics-broker` and `sensors`.
ENTRYPOINT ["python3", "engine.py"]
