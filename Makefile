# Kafka to YugabyteDB Sync Toolkit Makefile

.PHONY: help setup infra-base connector-up connector-status run run-small verify-db clean

# Default: help
help:
	@echo "Available commands:"
	@echo "  make infra-base     - Start Kafka, YugabyteDB, Connect, Console"
	@echo "  make connector-up   - Submit JDBC Sink Connector to Kafka Connect"
	@echo "  make connector-status - Check connector status (requires jq)"
	@echo "  make run            - Run test (100k msgs, max speed)"
	@echo "  make run-small      - Run test (10 msgs) for quick verification"
	@echo "  make verify-db      - Check row count in YugabyteDB"
	@echo "  make clean          - Stop and remove all containers and volumes"

# Setup: install dependencies
setup:
	pip install -r requirements.txt

# Infrastructure: full cluster in KRaft mode
infra-base:
	docker compose up -d
	@echo "Waiting for services to be ready (30s)..."
	@sleep 30

clean:
	docker compose down -v --remove-orphans

# Connector management
connector-up:
	curl -i -X POST -H "Content-Type: application/json" --data @connector.json http://localhost:8083/connectors

connector-status:
	@curl -s http://localhost:8083/connectors/iidr-jdbc-sink-yb/status | python3 -m json.tool

# Execution: performance tests
run:
	python3 producer.py --topic iidr.CDC.TEST_ORDERS --num-messages 100000 --message-file template.json

run-small:
	python3 producer.py --topic iidr.CDC.TEST_ORDERS --num-messages 10 --message-file template.json

# Database verification
verify-db:
	@echo "Checking YSQL connectivity..."
	@docker exec yugabyte bash -c "until bin/ysqlsh -h 127.0.0.1 -U yugabyte -c 'select 1' > /dev/null 2>&1; do echo 'Waiting for YSQL...'; sleep 2; done"
	@docker exec yugabyte bin/ysqlsh -h 127.0.0.1 -U yugabyte -d yugabyte -c "SELECT count(*) FROM test_orders;"
