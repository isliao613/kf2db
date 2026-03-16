# Kafka to YugabyteDB Sync & Performance Toolkit Makefile

.PHONY: help setup infra-base init-db connector-up connector-status run run-small verify-db clean

TABLES = orders products users customers inventory

# Individual targets for parallel execution
CONNECTOR_TARGETS = $(addprefix connector-, $(TABLES))
RUN_TARGETS = $(addprefix run-, $(TABLES))
RUN_SMALL_TARGETS = $(addprefix run-small-, $(TABLES))

# Default: help
help:
	@echo "Available commands (Supports 5 tables: $(TABLES)):"
	@echo "  make setup          - Install dependencies and set permissions"
	@echo "  make infra-base     - Start Kafka, YugabyteDB, Connect, Console"
	@echo "  make connector-up   - Create 5 tables (sequential) and submit 5 connectors (parallel)"
	@echo "  make connector-status - Check health of all 5 connectors"
	@echo "  make run            - Produce 100,000 msg/table (Parallel)"
	@echo "  make run-small      - Produce 10 msg/table (Parallel)"
	@echo "  make verify-db      - Row count check for all 5 tables"
	@echo "  make clean          - Stop and remove all containers and volumes"

# Setup: install dependencies
setup:
	pip install -r requirements.txt
	chmod +x init_db.sh

# Infrastructure
infra-base:
	docker compose up -d
	@echo "Waiting for services to be ready (45s)..."
	@sleep 45

# --- Execution Rules ---

# Database initialization (KEEP SEQUENTIAL to avoid Catalog Version Mismatch in YugabyteDB)
init-db:
	@for table in $(TABLES); do \
		./init_db.sh test_$$table; \
	done

# Connector management (Submission can be parallel)
connector-up: init-db
	@$(MAKE) -j 5 $(CONNECTOR_TARGETS)

connector-%:
	@UPPER_TABLE=$$(echo $* | tr '[:lower:]' '[:upper:]'); \
	sed -e "s/iidr-jdbc-sink-yb/iidr-jdbc-sink-$*/g" \
		-e "s/iidr\.CDC\.TEST_ORDERS/iidr.CDC.TEST_$$UPPER_TABLE/g" \
		-e "s/test_orders/test_$*/g" \
		-e "s/TEST_ORDERS/TEST_$$UPPER_TABLE/g" \
		connector.json > connector_$*.json; \
	curl -s -i -X POST -H "Content-Type: application/json" --data @connector_$*.json http://localhost:8083/connectors > /dev/null; \
	rm connector_$*.json; \
	echo "Connector for $* submitted."

connector-status:
	@for table in $(TABLES); do \
		echo "--- Status for iidr-jdbc-sink-$$table ---"; \
		curl -s http://localhost:8083/connectors/iidr-jdbc-sink-$$table/status | python3 -m json.tool || echo "Not found"; \
	done

# Performance tests (Parallel)
run:
	@$(MAKE) -j 5 $(RUN_TARGETS)

run-%:
	@UPPER_TABLE=$$(echo $* | tr '[:lower:]' '[:upper:]'); \
	sed -e "s/TEST_ORDERS/TEST_$$UPPER_TABLE/g" \
		-e "s/item_{{id}}/$*_{{id}}/g" \
		template.json > template_$*.json; \
	echo "Producing 100,000 messages to iidr.CDC.TEST_$$UPPER_TABLE..."; \
	python3 producer.py --num-messages 100000 --topic iidr.CDC.TEST_$$UPPER_TABLE --message-file template_$*.json; \
	rm template_$*.json

run-small:
	@$(MAKE) -j 5 $(RUN_SMALL_TARGETS)

run-small-%:
	@UPPER_TABLE=$$(echo $* | tr '[:lower:]' '[:upper:]'); \
	sed -e "s/TEST_ORDERS/TEST_$$UPPER_TABLE/g" \
		-e "s/item_{{id}}/$*_{{id}}/g" \
		template.json > template_$*.json; \
	echo "Producing 10 messages to iidr.CDC.TEST_$$UPPER_TABLE..."; \
	python3 producer.py --num-messages 10 --topic iidr.CDC.TEST_$$UPPER_TABLE --message-file template_$*.json; \
	rm template_$*.json

# --- Verification ---

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
