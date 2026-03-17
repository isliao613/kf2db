# Kafka to YugabyteDB Sync & Performance Toolkit Makefile

.PHONY: help setup infra-base init-db connector-up connector-status run run-small verify-db clean

# Default: help
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies and set permissions"
	@echo "  make infra-base     - Start Kafka, YugabyteDB, Connect, Console"
	@echo "  make connector-up   - Manually create table and submit Sink Connector"
	@echo "  make connector-status - Check health of the connector and tasks"
	@echo "  make run            - Produce 100,000 messages (max speed)"
	@echo "  make run-small      - Produce 10 messages (for verification)"
	@echo "  make verify-db      - Robust row count check in YugabyteDB"
	@echo "  make clean          - Stop and remove all containers and volumes"

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

# Database verification (Dynamically resolves container IP)
verify-db:
	@echo "Checking YugabyteDB row count..."
	@HOST=$$(docker exec yugabyte hostname -i); \
	docker exec yugabyte bin/ysqlsh -h $$HOST -U yugabyte -d yugabyte -c "SELECT count(*) FROM test_orders;" \ 
	python3 analyze_latency.py

clean:
	docker compose down -v --remove-orphans
