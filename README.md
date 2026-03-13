# Kafka to YugabyteDB Sync & Performance Toolkit

This toolkit provides a complete pipeline to test high-performance data ingestion from Kafka into YugabyteDB using a customized Kafka Connect JDBC Sink.

## Architecture
- **Kafka**: KRaft mode (no Zookeeper).
- **YugabyteDB**: High-performance distributed SQL database.
- **Kafka Connect**: Using `isliao613/kafka-connect:1.0.6` with custom SMTs.
- **Redpanda Console**: Web UI for monitoring topics, messages, and connectors.

---

## 1. Setup & Infrastructure

### Start All Services
```bash
make infra-base
```
*Note: Wait ~30-45 seconds for YugabyteDB and Kafka Connect to initialize.*

### Install Python Dependencies
```bash
make setup
```

---

## 2. Configure the Sink Connector

Submit the JDBC Sink connector to Kafka Connect. This will target the `iidr.CDC.TEST_ORDERS` topic and sync it to the `test_orders` table in YugabyteDB.

```bash
make connector-up
```

### Check Status
```bash
make connector-status
```
Ensure the connector and tasks are in the `RUNNING` state.

---

## 3. Run Performance Tests

The producer uses a structured `template.json` to simulate CDC (Change Data Capture) messages.

### Produce 100,000 Messages
```bash
make run
```

### Produce with Specific Rate (e.g., 500 msg/sec)
```bash
python3 producer.py --topic iidr.CDC.TEST_ORDERS --rate 500 --num-messages 10000 --message-file template.json
```

---

## 4. Verify Data in YugabyteDB

Check the row count in the `test_orders` table to confirm synchronization:
```bash
make verify-db
```

Access the YugabyteDB UI at [http://localhost:15433](http://localhost:15433) or the Redpanda Console at [http://localhost:8080](http://localhost:8080).

---

## 5. Cleanup

Stop and remove all containers, volumes, and networks:
```bash
make clean
```

---

## Configuration Reference

| File | Description |
| :--- | :--- |
| `docker-compose.yml` | Full infrastructure definition. |
| `connector.json` | Sink connector config (Dialect, Table Format, SMTs). |
| `template.json` | Structured message template (Key, Value, Headers). |
| `producer.py` | High-performance Python producer with rate limiting. |
