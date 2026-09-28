FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY iot_fleet.py iot_producer.py finnhub_producer.py ./
# CMD is overridden per-service in docker-compose (one image, two entrypoints)
CMD ["python", "iot_producer.py"]
