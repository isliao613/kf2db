# Kafka Performance Testing Toolkit

This toolkit provides a high-performance Kafka environment (KRaft mode) and a customizable Python producer script designed for benchmarking and throughput testing.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.7+
- (Optional) A Python virtual environment

## 1. Start the Kafka Environment

Deploy the Kafka cluster (KRaft Broker and Redpanda Console) using Docker Compose:

```bash
make infra-base
```

### Accessing the UI
Redpanda Console provides a web interface to visualize topics and messages.
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Features**: Monitor partition distribution and message headers in real-time.

## 2. Install Dependencies

```bash
make setup
```

## 3. Run Performance Tests

### Basic Usage (Max Speed)
```bash
make run
```

### Advanced Usage (Rate Limiting & Partitioning)
To test a specific throughput and force partitioning based on a nested field (e.g., `user_id` inside the `value` object):
```bash
python3 producer.py \
  --num-messages 10000 \
  --rate 500 \
  --partition-key "value.payload.user_id" \
  --message-file template.json
```

## CLI Arguments

| Argument | Default | Description |
| :--- | :--- | :--- |
| `--bootstrap-servers` | `localhost:9092` | Kafka broker addresses. |
| `--topic` | `perf-test` | The Kafka topic to produce to. |
| `--num-messages` | `100000` | Messages produced **per iteration**. |
| `--iterations` | `1` | Number of times to repeat the production cycle. |
| `--rate` | `0` | Target messages per second (**0 = max speed**). |
| `--partition-key` | `None` | Dot-separated path to a field in the template to use as the Kafka Key (e.g. `value.user_id`). |
| `--message-file` | `None` | Path to a structured JSON template file. |
| `--batch-size` | `16384` | Maximum number of messages per batch. |
| `--linger-ms` | `10` | Milliseconds to wait before sending a batch. |

## 4. Customizing the Message Template

The `template.json` file allows you to define the **Key**, **Value**, and **Headers** for each message. Use `{{id}}` and `{{timestamp}}` as dynamic placeholders.

```json
{
  "key": { "user_id": {{id}}, "type": "USER_ACTION" },
  "headers": { "source": "perf-test", "trace_id": "trace-{{id}}" },
  "value": {
    "id": {{id}},
    "ts": "{{timestamp}}",
    "payload": { "user_id": {{id}}, "msg": "Structured test message" }
  }
}
```

## 5. Cleanup

```bash
make clean
```
