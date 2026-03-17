# Kafka to YugabyteDB Sync & Performance Toolkit

This toolkit provides a high-performance pipeline to test data synchronization from Kafka into YugabyteDB using a customized Kafka Connect JDBC Sink. It is specifically designed for benchmarking latency and throughput across multiple tables with high concurrency.

## Architecture
- **Kafka**: KRaft mode (Single Broker) with **32 partitions** per topic by default.
- **YugabyteDB**: Distributed SQL database with UI enabled.
- **Kafka Connect**: Custom image `isliao613/kafka-connect:1.0.6` with IIDR SMT.
- **Redpanda Console**: v3.3.2 for monitoring topics and connectors.
- **Python Drivers**: `kafka-python-ng` and `lz4` for high-performance production.

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
- **Redpanda Console**: [http://localhost:8080](http://localhost:8080)
- **YugabyteDB UI**: [http://localhost:15433](http://localhost:15433)

---

## 2. Synchronization Setup

### Initialize Table & Connector
Create the default `test_orders` table and submit the JDBC Sink configuration:
```bash
make connector-up
```

---

## 3. Performance Benchmarking

The toolkit includes a `benchmark.py` script to automate complex test scenarios involving multiple tables and parallel producers.

### Benchmarking Targets
All benchmarks target an aggregate ingress rate of **20,000 messages/second** using 5 parallel producer processes (4,000 msg/s each).

| Command | Tables | Triggers | Goal |
| :--- | :--- | :--- | :--- |
| `make benchmark-5` | 5 | No | Standard baseline |
| `make benchmark-5-trigger` | 5 | Yes | Standard with triggers |
| `make benchmark-8` | 8 | No | Extended baseline |
| `make benchmark-8-trigger` | 8 | Yes | Extended with triggers |
| `make benchmark-32` | 32 | No | High-concurrency baseline |
| `make benchmark-32-trigger` | 32 | Yes | High-concurrency with triggers |

### Performance Report
After each benchmark run, a `performance_report.md` is automatically generated. It contains:
- **Test Configuration**: Date, table count, producer count, and target rate.
- **Producer Statistics**: Exact start and end times for each producer process.
- **Detailed Table Metrics**: Comprehensive latency analysis for every table using full descriptive names (e.g., `MAX(yb_at-kafka_at)`).
- **System Summary**: Total messages synchronized and global average end-to-end latency.

### Latency Metrics Tracked
- **`kafka_at`**: Set by the producer at the moment of dispatch (UTC).
- **`consumer_at`**: Simulated processing time (UTC).
- **`yb_at`**: Database ingestion time (automatically set by YugabyteDB).

---

## 4. Manual Production

Use `producer.py` for fine-grained control:
```bash
python3 producer.py \
  --topic iidr.CDC.TEST_ORDERS \
  --num-messages 100000 \
  --rate 4000 \
  --message-file template.json \
  --table-name TEST_ORDERS
```

---

## Configuration Reference

- **`template.json`**: Message structure with placeholders for `id`, `timestamp`, `kafka_at`, `consumer_at`, and `table_name`.
- **`connector.json`**: JDBC Sink configuration with IIDR SMT field mappings.
- **`benchmark.py`**: Orchestrator for table creation, connector submission, and result analysis.
- **`producer.py`**: Performance-optimized producer with high-resolution rate limiting and UTC timestamping.

---

## Cleanup
```bash
make clean
```
