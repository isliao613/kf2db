# Kafka to YugabyteDB Sync & Performance Toolkit

This toolkit provides a high-performance pipeline to test data synchronization from Kafka into YugabyteDB using a customized Kafka Connect JDBC Sink.

## Architecture
- **Kafka**: KRaft mode (Single Broker).
- **YugabyteDB**: Distributed SQL database with UI enabled.
- **Kafka Connect**: Custom image `isliao613/kafka-connect:1.0.6` with IIDR SMT.
- **Redpanda Console**: v3.3.2 with full Kafka Connect visibility.

---

## 1. Installation & Infrastructure

### Setup Environment
Install Python dependencies and set script permissions:
```bash
make setup
```

### Start Services
Launch the full cluster:
```bash
make infra-base
```

### Dashboard Access
- **Redpanda Console**: [http://localhost:8080](http://localhost:8080) (Inspect topics and connectors)
- **YugabyteDB UI**: [http://localhost:15433](http://localhost:15433) (Monitor database metrics)

---

## 2. Synchronization Setup

### Initialize Table & Connector
This command manually creates the `test_orders` table in YugabyteDB and submits the JDBC Sink configuration:
```bash
make connector-up
```

### Verify Health
Ensure the connector and tasks are in the `RUNNING` state:
```bash
make connector-status
```

---

## 3. Performance Testing

### Run Standard Benchmark
Produces 100,000 structured CDC-style messages to Kafka:
```bash
make run
```

### Customizable Production
Use `producer.py` for specific rate limiting or iterations:
```bash
python3 producer.py --topic iidr.CDC.TEST_ORDERS --num-messages 10000 --rate 500 --message-file template.json
```

---

## 4. Verification

### Check Database Sync
Count the rows in the YugabyteDB `test_orders` table:
```bash
make verify-db
```

---

## Configuration Reference

- **`template.json`**: Defines the structured message (Key, Value, Headers). Uses `{{id}}` and `{{timestamp}}` placeholders.
- **`connector.json`**: JDBC Sink configuration with specific SMT field mappings and overrides.
- **`producer.py`**: High-performance producer optimized for raw JSON strings and clean Kafka headers.
- **`init_db.sh`**: Manual DDL script for database preparation.

---

## Cleanup
Stop and remove all containers, volumes, and networks:
```bash
make clean
```
