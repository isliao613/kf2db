# Kafka to YugabyteDB Synchronization Tuning Summary

This document summarizes the performance tuning and latency analysis performed on the Kafka to YugabyteDB synchronization pipeline.

---

## 1. Methodology: Observability & Calibration
To enable precise tuning, we established segment-level observability by injecting timestamps at every stage of the data lifecycle:
- **T1 (Producer)**: `created_at` (When the message was generated).
- **T2 (Kafka)**: `kafka_at` (When the message was appended to the Kafka log).
- **T4 (Database)**: `db_at` (When the row was committed to YugabyteDB).

**Key Insight**: A **Clock Calibration** step was implemented in the analysis script to compensate for clock drift between the Host (Producer) and the Docker containers (Kafka/DB), ensuring accurate latency measurements across distributed components.

---

## 2. Tuning Results & Observations

| Configuration | Tasks | Batch Size | Kafka-to-DB Latency (Avg) | Analysis |
| :--- | :--- | :--- | :--- | :--- |
| **Baseline** | 32 | 5000 | **~1.6s** | High latency due to large batching delay. |
| **Aggressive Eager** | 1 | 100 | **~1.8s** | Bottlenecked by serial processing (1 task). |
| **Overwhelmed** | 32 | 50 | **~3.6s** | High context switching & lock contention. |
| **Optimized Balanced**| 4 | 500 | **~0.8s** | **The "Sweet Spot" for this environment.** |

---

## 3. Key Tuning Levers Applied

### A. Database Schema: Tablet Splitting
- **Action**: Added `SPLIT INTO 8 TABLETS` to the `CREATE TABLE` statement.
- **Why**: This pre-shards the table into 8 parallel storage buckets (tablets). It allows YugabyteDB to use multiple CPU cores simultaneously and avoids "hotspots" where multiple Kafka tasks compete for the same internal lock.

### B. JDBC Driver: Batch Rewriting
- **Action**: Added `reWriteBatchedInserts=true` to the JDBC connection URL.
- **Why**: This critical YugabyteDB/PostgreSQL optimization collapses hundreds of individual `INSERT` statements into a single multi-row `INSERT` command. This reduces network round-trips and transaction overhead by 99%.

### C. Connector: Balanced Parallelism
- **Action**: Reduced `tasks.max` from 32 to 4 and adjusted `batch.size` to 500.
- **Why**: In a single-node setup, 32 tasks create excessive overhead. 4 tasks provide enough parallelism to saturate the database without causing high context switching. A 500-message batch ensures enough data is available for `reWriteBatchedInserts` to be effective.

### D. Operation Mode: `Insert` vs `Upsert`
- **Action**: Changed `insert.mode` to `insert`.
- **Why**: `Upsert` (Insert or Update) requires a "Read-before-Write" check to verify if the ID exists. Pure `insert` skips this check and is significantly faster for fresh data ingestion.

---

## 4. Final Recommendations for Low Latency
To maintain the lowest possible end-to-end delay:
1.  **Align Parallelism**: Set `tasks.max` to match the number of CPU cores or the number of table tablets (whichever is lower).
2.  **Enable Batch Rewriting**: Always use `reWriteBatchedInserts=true` for JDBC-based sinks.
3.  **Pre-split Tables**: Always use `SPLIT INTO` for new tables to ensure distributed performance from the start.
4.  **Monitor Clock Drift**: In distributed testing, always use UTC and calibrate offsets between the producer and the database.
