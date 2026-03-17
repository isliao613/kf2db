import psycopg2
import pandas as pd
from datetime import datetime

def analyze():
    try:
        conn = psycopg2.connect("host=localhost port=5433 dbname=yugabyte user=yugabyte password=yugabyte")
        
        # 1. Calibrate Clock Offset
        host_now = datetime.utcnow()
        with conn.cursor() as cur:
            cur.execute("SELECT (clock_timestamp() AT TIME ZONE 'UTC')")
            db_now = cur.fetchone()[0]
        
        offset_ms = (db_now.replace(tzinfo=None) - host_now).total_seconds() * 1000
        print(f"--- Calibration ---")
        print(f"Detected Clock Offset (DB - Host): {offset_ms:.2f} ms\n")

        # 2. Analyze with Offset Correction
        # Client -> Kafka (T1 to T2): Both come from Host-synced clocks usually
        # Kafka -> DB (T2 to T4): T4 comes from DB clock, T2 from Connect (Host-synced)
        # Total E2E (T1 to T4): T4 comes from DB clock, T1 from Host
        
        t1_to_t2 = "EXTRACT(EPOCH FROM (kafka_at - created_at)) * 1000"
        t2_to_t4 = f"(EXTRACT(EPOCH FROM (db_at - kafka_at)) * 1000) - {offset_ms}"
        t1_to_t4 = f"(EXTRACT(EPOCH FROM (db_at - created_at)) * 1000) - {offset_ms}"

        summary_query = f"""
        SELECT 
            MIN({t1_to_t2}) AS min_client_to_kafka,
            AVG({t1_to_t2}) AS avg_client_to_kafka,
            MAX({t1_to_t2}) AS max_client_to_kafka,
            
            MIN({t2_to_t4}) AS min_kafka_to_db,
            AVG({t2_to_t4}) AS avg_kafka_to_db,
            MAX({t2_to_t4}) AS max_kafka_to_db,
            
            MIN({t1_to_t4}) AS min_total_e2e,
            AVG({t1_to_t4}) AS avg_total_e2e,
            MAX({t1_to_t4}) AS max_total_e2e
        FROM test_orders;
        """
        
        summary = pd.read_sql(summary_query, conn)
        
        print("--- Detailed Latency Metrics (Adjusted) ---")
        report = pd.DataFrame({
            'Metric Segment': ['Client to Kafka', 'Kafka to DB', 'Total E2E'],
            'Min (ms)': [summary['min_client_to_kafka'][0], summary['min_kafka_to_db'][0], summary['min_total_e2e'][0]],
            'Avg (ms)': [summary['avg_client_to_kafka'][0], summary['avg_kafka_to_db'][0], summary['avg_total_e2e'][0]],
            'Max (ms)': [summary['max_client_to_kafka'][0], summary['max_kafka_to_db'][0], summary['max_total_e2e'][0]]
        })
        print(report.to_string(index=False))
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    analyze()
