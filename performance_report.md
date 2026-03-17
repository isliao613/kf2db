# Performance Benchmark Report
**Date:** 2026-03-17 15:42:08
**Target Tables:** 2
**Producers:** 5
**Target Rate:** 20000 msg/s
**Triggers Enabled:** False

## Producer Statistics (Client -> Kafka)
| Producer ID | Start Time | End Time |
| --- | --- | --- |
| 0 | 2026-03-17T15:42:07.358697 | 2026-03-17T15:42:07.781690 |
| 1 | 2026-03-17T15:42:07.360777 | 2026-03-17T15:42:07.754929 |
| 2 | 2026-03-17T15:42:07.361111 | 2026-03-17T15:42:07.732469 |
| 3 | 2026-03-17T15:42:07.363111 | 2026-03-17T15:42:07.795313 |
| 4 | 2026-03-17T15:42:07.363699 | 2026-03-17T15:42:07.789690 |

## Table Metrics (Kafka -> YugabyteDB)
| Table | MIN(yb_at) | MAX(yb_at) | MAX-MIN(yb_at) | MAX(yb_at-kafka_at) | AVG(yb_at-kafka_at) | MAX(yb_at-consumer_at) | AVG(yb_at-consumer_at) | MAX(consumer_at-kafka_at) | AVG(consumer_at-kafka_at) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| test_orders_0 | 2026-03-17 07:42:05.716787+00 | 2026-03-17 07:42:07.68556+00 | 1.968773 | 1284.716787 | 1280.891982 | 1284.716787 | 1280.891982 | 0.000000 | 0.000000 |
| test_orders_1 | 2026-03-17 07:42:05.821356+00 | 2026-03-17 07:42:07.802248+00 | 1.980892 | 1284.821356 | 1281.766211 | 1284.821356 | 1281.766211 | 0.000000 | 0.000000 |

## Summary
- **Total Messages Synchronized:** 1107
- **Global Average End-to-End Latency:** 1281.3291 s