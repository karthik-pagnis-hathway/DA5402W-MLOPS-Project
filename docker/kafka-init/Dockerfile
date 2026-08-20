FROM python:3.11-slim

RUN pip install --no-cache-dir 'kafka-python==2.2.15'

COPY src/kafkaScripts/topicCreator.py /app/topicCreator.py

WORKDIR /app

ENV KAFKA_BROKER=kafka:9092

CMD ["python", "topicCreator.py"]
