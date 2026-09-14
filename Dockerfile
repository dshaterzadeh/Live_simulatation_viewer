FROM python:3.11-slim

WORKDIR /app

# Pinned in requirements.txt so the container matches the host .venv exactly.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts and data
COPY hdf5_mqtt_publisher.py sensor_simulator.py mqtt_tester.py ./
COPY datasources/ ./datasources/
COPY replay/ ./replay/
COPY 20260623_baseline.hdf5 ./

# One image, two entrypoints: docker-compose.yml runs this default for the
# `publisher` service and overrides it with sensor_simulator.py for `sensors`.
ENTRYPOINT ["python3", "hdf5_mqtt_publisher.py"]
CMD ["--file", "20260623_baseline.hdf5", "--host", "mosquitto", "--port", "1883", "--topic", "sim/coesi5", "--delay", "1.0", "--loop"]
