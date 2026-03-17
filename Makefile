# Kafka to YugabyteDB Sync & Performance Toolkit Makefile

.PHONY: help setup infra-base init-db connector-up connector-status run run-small verify-db benchmark-5 benchmark-5-trigger benchmark-8 benchmark-8-trigger benchmark-32 benchmark-32-trigger clean

# Default: help
help:
	@echo "Available commands:"
	@echo "  make setup                - Install dependencies and set permissions"
	@echo "  make infra-base           - Start Kafka, YugabyteDB, Connect, Console"
	@echo "  make connector-up         - Manually create table and submit Sink Connector"
	@echo "  make connector-status     - Check health of the connector and tasks"
	@echo "  make run                  - Produce 100,000 messages (max speed)"
	@echo "  make run-small            - Produce 10 messages (for verification)"
	@echo "  make verify-db            - Robust row count check in YugabyteDB"
	@echo "  make benchmark-5          - Run benchmark: 5 tables, no triggers (20k/s)"
	@echo "  make benchmark-5-trigger  - Run benchmark: 5 tables, with triggers (20k/s)"
	@echo "  make benchmark-8          - Run benchmark: 8 tables, no triggers (20k/s)"
	@echo "  make benchmark-8-trigger  - Run benchmark: 8 tables, with triggers (20k/s)"
	@echo "  make benchmark-32         - Run benchmark: 32 tables, no triggers (20k/s)"
	@echo "  make benchmark-32-trigger - Run benchmark: 32 tables, with triggers (20k/s)"
	@echo "  make clean                - Stop and remove all containers and volumes"

# Setup: install dependencies
setup:
	pip install -r requirements.txt
	chmod +x init_db.sh

# Infrastructure: full cluster in KRaft mode
infra-base:
	docker compose up -d
	@echo "Waiting for services to be ready (45s)..."
	@sleep 45

# Database initialization (Manual table creation)
init-db:
	./init_db.sh

# Connector management
connector-up: init-db
	@echo "Submitting connector to Kafka Connect..."
	@curl -i -X POST -H "Content-Type: application/json" --data @connector.json http://localhost:8083/connectors

connector-status:
	@curl -s http://localhost:8083/connectors/iidr-jdbc-sink-yb/status | python3 -m json.tool

# Execution: performance tests
run:
	python3 producer.py --num-messages 100000 --message-file template.json

run-small:
	python3 producer.py --num-messages 10 --message-file template.json

# Default message count for benchmarks
MSG_COUNT ?= 200000

# Benchmarks based on TODO.md
# Note: 5 producers x 4000 msg/s = 20,000 msg/s
benchmark-5:
	python3 benchmark.py --num-tables 5 --num-producers 5 --rate-per-producer 4000 --messages-per-producer $(MSG_COUNT)

benchmark-5-trigger:
	python3 benchmark.py --num-tables 5 --num-producers 5 --rate-per-producer 4000 --messages-per-producer $(MSG_COUNT) --with-trigger

benchmark-8:
	python3 benchmark.py --num-tables 8 --num-producers 5 --rate-per-producer 4000 --messages-per-producer $(MSG_COUNT)

benchmark-8-trigger:
	python3 benchmark.py --num-tables 8 --num-producers 5 --rate-per-producer 4000 --messages-per-producer $(MSG_COUNT) --with-trigger

benchmark-32:
	python3 benchmark.py --num-tables 32 --num-producers 5 --rate-per-producer 4000 --messages-per-producer $(MSG_COUNT)

benchmark-32-trigger:
	python3 benchmark.py --num-tables 32 --num-producers 5 --rate-per-producer 4000 --messages-per-producer $(MSG_COUNT) --with-trigger

# Database verification (Dynamically resolves container IP)
verify-db:
	@echo "Checking YugabyteDB row count..."
	@HOST=$$(docker exec yugabyte hostname -i); \
	docker exec yugabyte bin/ysqlsh -h $$HOST -U yugabyte -d yugabyte -c "SELECT count(*) FROM test_orders;"

clean:
	docker compose down -v --remove-orphans
