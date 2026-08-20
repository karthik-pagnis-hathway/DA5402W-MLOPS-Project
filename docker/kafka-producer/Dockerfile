FROM python:3.11-slim

RUN pip install --no-cache-dir 'kafka-python==2.2.15' numpy pandas

COPY src/kafkaScripts/ /app/

WORKDIR /app

ENV KAFKA_BROKER=kafka:9092

CMD ["python", "launch_producer.py", \
     "--kafka-broker", "kafka:9092", \
     "--n-users", "1000", \
     "--n-products", "200", \
     "--batch-size", "500", \
     "--interval-seconds", "5", \
     "--run-forever", \
     "--out-dir", "/opt/airflow/outputs"]
