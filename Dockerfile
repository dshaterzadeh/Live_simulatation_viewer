FROM python:3.11-slim

WORKDIR /app

# Pinned in requirements.txt so the container matches the host .venv exactly.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts and data
COPY engine.py bridge.py federation.py helics_broker.py sensor_simulator.py mqtt_tester.py ./
COPY datasources/ ./datasources/
COPY replay/ ./replay/
COPY 20260623_baseline.hdf5 ./

# One image, four entrypoints: docker-compose.yml runs this default for the
# `engine` service and overrides it for `bridge`, `helics-broker` and `sensors`.
ENTRYPOINT ["python3", "engine.py"]
CMD ["--file", "20260623_baseline.hdf5", "--helics-broker", "tcp://helics-broker:23404", "--delay", "1.0", "--loop"]
