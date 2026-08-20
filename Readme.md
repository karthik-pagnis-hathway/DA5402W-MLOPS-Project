producer ---> Model Building pipeline -> 
		 ---> Analytics Pipeline
		 ---> Prediction Pipeline --> model_prediction consumer -> Show to user
		 ---> Backend Process Triggers(yet to figure out)


=============================

# Clean up unwanted files and docker fresh start, Otherwise skip to step 6

1. List down the kafka topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

(Delete the topics one by one)
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic user-events

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic recommendation-actions

docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic notification-events

2. Remove the DB files, try docker command, if it doesn't work try sudo

docker exec -it spark-master bash -c "rm -rf /DB/consumed_events/* /DB/topic_dumps/* /DB/metrics/* /DB/analytics/*"
sudo rm -rf outputs/* checkpoints/* spark-warehouse .metastore_db
sudo rm -rf /DB/consumed_events/* /DB/topic_dumps/* /DB/metrics/*
sudo find . -name "*.crc" -delete
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

3. Down the container
docker compose -f docker-compose.yaml down

(OR, stop and delete everything related to the compose project)
docker compose -f docker-compose.yaml down --rmi all -v --remove-orphans

4. Full cleanup, Remove unused Docker resources, only if required !!...THIS WILL REMOVE ALL THE CONTAINERS PULLED...!!
docker system prune -a --volumes -f

------------------

# Fresh start using docker

6. Build fresh images, installations happen here
docker compose -f docker-compose.yaml build --no-cache

7. Fire up the services one by one, and check logs if needed

docker compose -f docker-compose.yaml up -d postgres
docker compose -f docker-compose.yaml logs postgres

(Create kafka topics)
docker compose -f docker-compose.yaml up -d topic-init
docker compose -f docker-compose.yaml logs topic-init

docker compose -f docker-compose.yaml up airflow-init
docker compose -f docker-compose.yaml logs airflow-init

(Start streaming consumer and event producer)
docker compose -f docker-compose.yaml up -d spark spark-consumer event-producer
docker compose -f docker-compose.yaml logs -f spark-consumer

(Wait for some time, Check if all the containers are running)
docker compose -f docker-compose.yaml ps

8. Fireup airflow webserver and verify UI at localhost:8080 and check logs
docker compose -f docker-compose.yaml up -d airflow-webserver airflow-scheduler
docker compose -f docker-compose.yaml logs airflow-scheduler

9. Fire up the ecomm backend + frontend
docker compose -f docker-compose.yaml up -d ecomm-postgres ecomm-backend frontend apriori-api churn-notifier

# Access points:
#   Angular UI:        http://localhost:4200
#   Flask backend:     http://localhost:5001/api/health
#   MLflow UI:         http://localhost:5000
#   Airflow UI:        http://localhost:8080  (admin/admin)
#   Apriori API:       http://localhost:5002/health
#   pgweb (DB UI):     http://localhost:8081
#   Kafka external:    localhost:9094

10. (Optional) Run the producer from host machine:
python ./src/kafkaScripts/launch_producer.py --n-users 1000 --n-products 200 --batch-size 500 --interval-seconds 5 --kafka-broker localhost:9094 --out-dir ./outputs/kafkaOutput --seed 42 --run-forever

------------------

# Prometheus + Grafana Monitoring

Prometheus and Grafana run inside the spark-master container (no separate containers needed).
They start automatically whenever any service is brought up (kafka, mlflow, airflow, etc.)

Ports:
- Prometheus UI  : http://localhost:9090
- Grafana UI     : http://localhost:3000  (login: admin / admin)
- Kafka JMX      : http://localhost:7071/metrics

Monitored services:
- Flask backend (ecomm-backend)   --> request rate, latency, error rate
- Apriori API                     --> prediction rate, latency
- Churn Notifier                  --> events processed, abandonments, notifications
- Kafka                           --> messages in/out, bytes, partitions (via JMX exporter)
- Airflow webserver + scheduler   --> task completions, executor status (via statsd-exporter)
- Spark                           --> JVM metrics (only when a spark-submit job is running)
- MLflow                          --> basic process metrics

------------------

# Running monitoring independently

10. Start ONLY Prometheus + Grafana (without kafka, airflow, etc.)
docker compose up spark --build -d

This gives you the monitoring stack at localhost:9090 and localhost:3000
All scrape targets will show "DOWN" until you bring up other services

11. Start monitoring + specific services only

(Monitoring + Kafka only)
docker compose up kafka -d

(Monitoring + MLflow only)
docker compose up mlflow -d

(Monitoring + Airflow only)
docker compose up airflow-webserver airflow-scheduler -d

(Everything)
docker compose up -d

Note: Starting kafka, mlflow, or airflow automatically starts the spark container
(which runs Prometheus + Grafana), so monitoring is always available.

12. Check Prometheus scrape targets
Open http://localhost:9090/targets
All active services should show "UP" within ~30 seconds
Spark target will show "DOWN" when no spark-submit job is running — this is expected

13. Access Grafana dashboard
Open http://localhost:3000
Login with admin / admin (change password on first login)
The "MLOps Pipeline Overview" dashboard is pre-loaded with panels for all services

14. Verify Flask backend metrics are being collected
curl http://localhost:5001/api/health
Then check in Prometheus: http://localhost:9090/graph
Query: flask_http_requests_total

15. Verify Kafka JMX metrics
curl http://localhost:7071/metrics
Should show kafka_server_brokertopicmetrics_* metrics

------------------
