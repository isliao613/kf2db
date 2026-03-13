# Kafka Performance Testing Makefile

.PHONY: help setup infra-base infra-up infra-down infra-clean run run-rate run-partition run-small clean

# Show help by default
help:
	@echo "Available commands:"
	@echo "  make infra-base     - Start Kafka (KRaft) and Redpanda Console"
	@echo "  make run            - Run test (100k msgs, default keys)"
	@echo "  make run-rate       - Run test (1k msgs, 100 msg/sec)"
	@echo "  make run-partition  - Run test partitioning by nested field (value.payload.user_id)"
	@echo "  make run-small      - Run a quick test (100 msgs)"
	@echo "  make clean          - Stop containers and remove all volumes"

# Setup: install dependencies
setup:
	pip install -r requirements.txt

# Infrastructure management
infra-base:
	docker compose up -d
	@echo "Waiting for Kafka to be ready..."
	@sleep 10
	@echo "Infrastructure is up. UI: http://localhost:8080"

infra-up: infra-base

infra-down:
	docker compose stop

infra-clean:
	docker compose down -v --remove-orphans

clean: infra-clean

# Execution management
run:
	python3 producer.py --num-messages 100000 --message-file template.json

run-rate:
	python3 producer.py --num-messages 100000 --iterations 5 --rate 100000 --message-file template.json

run-partition:
	python3 producer.py --num-messages 100000 --rate 100000 --partition-key "value.payload.user_id" --message-file template.json

run-small:
	python3 producer.py --num-messages 100000 --message-file template.json --topic verify-topic

# Helper to remove local caches
local-clean:
	rm -rf __pycache__
