# Kafka to YugabyteDB Sync & Performance Toolkit Makefile

.PHONY: help setup infra-base init-db connector-up connector-status run run-small verify-db clean

TABLES = orders products users customers inventory

# Default: help
help:
	@echo "Available commands (Supports 5 tables: $(TABLES)):"
	@echo "  make setup          - Install dependencies and set permissions"
	@echo "  make infra-base     - Start Kafka, YugabyteDB, Connect, Console"
	@echo "  make connector-up   - Manually create 5 tables and submit 5 Sink Connectors"
	@echo "  make connector-status - Check health of all 5 connectors"
	@echo "  make run            - Produce 100,000 messages for each of the 5 tables"
	@echo "  make run-small      - Produce 10 messages for each of the 5 tables"
	@echo "  make verify-db      - Row count check for all 5 tables in YugabyteDB"
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

# Database initialization (Manual table creation for all 5 tables)
init-db:
	@for table in $(TABLES); do \
		./init_db.sh test_$$table; \
	done

# Connector management (Generates and submits 5 connectors)
connector-up: init-db
	@echo "Submitting 5 connectors to Kafka Connect..."
	@for table in $(TABLES); do \
		UPPER_TABLE=$$(echo $$table | tr '[:lower:]' '[:upper:]'); \
		sed -e "s/iidr-jdbc-sink-yb/iidr-jdbc-sink-$$table/g" \
			-e "s/iidr\.CDC\.TEST_ORDERS/iidr.CDC.TEST_$$UPPER_TABLE/g" \
			-e "s/test_orders/test_$$table/g" \
			-e "s/TEST_ORDERS/TEST_$$UPPER_TABLE/g" \
			connector.json > connector_$$table.json; \
		curl -s -i -X POST -H "Content-Type: application/json" --data @connector_$$table.json http://localhost:8083/connectors > /dev/null; \
		rm connector_$$table.json; \
		echo "Connector for $$table submitted."; \
	done

connector-status:
	@for table in $(TABLES); do \
		echo "--- Status for iidr-jdbc-sink-$$table ---"; \
		curl -s http://localhost:8083/connectors/iidr-jdbc-sink-$$table/status | python3 -m json.tool || echo "Not found"; \
	done

# Execution: performance tests for all 5 tables
run:
	@for table in $(TABLES); do \
		UPPER_TABLE=$$(echo $$table | tr '[:lower:]' '[:upper:]'); \
		sed -e "s/TEST_ORDERS/TEST_$$UPPER_TABLE/g" \
			-e "s/item_{{id}}/$${table}_{{id}}/g" \
			template.json > template_$$table.json; \
		echo "Producing 100,000 messages to iidr.CDC.TEST_$$UPPER_TABLE..."; \
		python3 producer.py --num-messages 100000 --topic iidr.CDC.TEST_$$UPPER_TABLE --message-file template_$$table.json; \
		rm template_$$table.json; \
	done

run-small:
	@for table in $(TABLES); do \
		UPPER_TABLE=$$(echo $$table | tr '[:lower:]' '[:upper:]'); \
		sed -e "s/TEST_ORDERS/TEST_$$UPPER_TABLE/g" \
			-e "s/item_{{id}}/$${table}_{{id}}/g" \
			template.json > template_$$table.json; \
		echo "Producing 10 messages to iidr.CDC.TEST_$$UPPER_TABLE..."; \
		python3 producer.py --num-messages 10 --topic iidr.CDC.TEST_$$UPPER_TABLE --message-file template_$$table.json; \
		rm template_$$table.json; \
	done

# Database verification
verify-db:
	@echo "Checking YugabyteDB row counts..."
	@HOST=$$(docker exec yugabyte hostname -i); \
	for table in $(TABLES); do \
		echo "Table test_$$table:"; \
		docker exec yugabyte bin/ysqlsh -h $$HOST -U yugabyte -d yugabyte -c "SELECT count(*) FROM test_$$table;"; \
	done

clean:
	docker compose down -v --remove-orphans
	rm -f connector_*.json template_*.json
