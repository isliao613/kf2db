# Kafka to YugabyteDB Sync & Performance Toolkit Makefile

.PHONY: help setup infra-base init-db connector-up connector-multi-up connector-status reset-kafka run run-multi run-small verify-db verify-multi clean

# Default: help
help:
	@echo "Available commands:"
	@echo "  make setup          - Install dependencies and set permissions"
	@echo "  make infra-base     - Start Kafka, YugabyteDB, Connect, Console"
	@echo "  make init-db        - Initialize all required tables"
	@echo "  make connector-up   - Submit standard Sink Connector (connector.json)"
	@echo "  make connector-multi-up - Submit isolated paired connectors (conn1.json, conn2.json)"
	@echo "  make connector-status - Check health of all connectors"
	@echo "  make reset-kafka    - Delete and recreate topics to clear invalid data"
	@echo "  make run            - Produce 100,000 messages to 1 topic"
	@echo "  make run-multi      - Produce 10,000 messages per topic to 2 topics (isolated IDs)"
	@echo "  make run-small      - Produce 10 messages (for verification)"
	@echo "  make verify-db      - Row count & latency check (test_orders)"
	@echo "  make verify-multi   - Row count & latency check (test_orders, test_orders_2)"
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

connector-multi-up: init-db
	@echo "Submitting isolated connectors to Kafka Connect..."
	@curl -i -X POST -H "Content-Type: application/json" --data @conn1.json http://localhost:8083/connectors
	@curl -i -X POST -H "Content-Type: application/json" --data @conn2.json http://localhost:8083/connectors

connector-status:
	@echo "--- iidr-jdbc-sink-yb ---"
	@curl -s http://localhost:8083/connectors/iidr-jdbc-sink-yb/status | python3 -m json.tool || echo "Not found."
	@echo "--- iidr-sink-1 ---"
	@curl -s http://localhost:8083/connectors/iidr-sink-1/status | python3 -m json.tool || echo "Not found."
	@echo "--- iidr-sink-2 ---"
	@curl -s http://localhost:8083/connectors/iidr-sink-2/status | python3 -m json.tool || echo "Not found."

reset-kafka:
	@echo "Deleting and recreating topics..."
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic iidr.CDC.TEST_ORDERS || true
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic iidr.CDC.TEST_ORDERS_2 || true
	@sleep 2
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic iidr.CDC.TEST_ORDERS --partitions 32 --replication-factor 1
	@docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic iidr.CDC.TEST_ORDERS_2 --partitions 32 --replication-factor 1

# Execution: performance tests
run:
	python3 producer.py --num-messages 100000 --processes 4 --message-file template.json --truncate

run-multi:
	python3 producer.py --topic iidr.CDC.TEST_ORDERS iidr.CDC.TEST_ORDERS_2 --table test_orders test_orders_2 --num-messages 10000 --processes 4 --message-file template.json --truncate

run-small:
	python3 producer.py --num-messages 10 --message-file template.json

# Database verification (Dynamically resolves container IP)
verify-db:
	@echo "Checking YugabyteDB row count..."
	@HOST=$$(docker exec yugabyte hostname -i); \
	docker exec yugabyte bin/ysqlsh -h $$HOST -U yugabyte -d yugabyte -c "SELECT count(*) FROM test_orders;"
	@python3 analyze_latency.py --tables test_orders

verify-multi:
	@echo "Checking YugabyteDB row counts for multiple tables..."
	@HOST=$$(docker exec yugabyte hostname -i); \
	docker exec yugabyte bin/ysqlsh -h $$HOST -U yugabyte -d yugabyte -c "SELECT 'test_orders' as table_name, count(*) FROM test_orders UNION ALL SELECT 'test_orders_2' as table_name, count(*) FROM test_orders_2;"
	@python3 analyze_latency.py --tables test_orders test_orders_2

clean:
	docker compose down -v --remove-orphans
