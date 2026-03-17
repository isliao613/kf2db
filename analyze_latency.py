import psycopg2
import pandas as pd
from datetime import datetime

def analyze():
    try:
        conn = psycopg2.connect("host=localhost port=5433 dbname=yugabyte user=yugabyte password=yugabyte")
        
        # 1. Calibrate Clock Offset
        # Get host time and DB time as close as possible
        host_now = datetime.utcnow()
        with conn.cursor() as cur:
            cur.execute("SELECT (clock_timestamp() AT TIME ZONE 'UTC')")
            db_now = cur.fetchone()[0]
        
        # Offset in milliseconds: (DB_Clock - Host_Clock)
        # If DB is 20s behind, offset will be approx -20000ms
        offset_ms = (db_now.replace(tzinfo=None) - host_now).total_seconds() * 1000
        print(f"--- Calibration ---")
        print(f"Host UTC: {host_now}")
        print(f"DB UTC:   {db_now}")
        print(f"Detected Clock Offset (DB - Host): {offset_ms:.2f} ms\n")

        # 2. Analyze with Offset Correction
        # Note: connector_received_at comes from the Connect container (usually synced with Host)
        # created_at comes from the Host
        # db_received_at comes from the DB container
        
        query = f"""
        SELECT 
            id,
            EXTRACT(EPOCH FROM (connector_received_at - created_at)) * 1000 AS client_to_connector_ms,
            (EXTRACT(EPOCH FROM (db_received_at - connector_received_at)) * 1000) - {offset_ms} AS connector_to_db_ms,
            (EXTRACT(EPOCH FROM (db_received_at - created_at)) * 1000) - {offset_ms} AS total_e2e_ms
        FROM test_orders
        ORDER BY id DESC
        LIMIT 10;
        """
        df = pd.read_sql(query, conn)
        print("--- Latency Analysis (Last 10 Records, Adjusted) ---")
        print(df.to_string(index=False))
        
        summary_query = f"""
        SELECT 
            AVG(EXTRACT(EPOCH FROM (connector_received_at - created_at)) * 1000) AS avg_client_to_connector_ms,
            AVG((EXTRACT(EPOCH FROM (db_received_at - connector_received_at)) * 1000) - {offset_ms}) AS avg_connector_to_db_ms,
            AVG((EXTRACT(EPOCH FROM (db_received_at - created_at)) * 1000) - {offset_ms}) AS avg_total_e2e_ms
        FROM test_orders;
        """
        summary = pd.read_sql(summary_query, conn)
        print("\n--- Average Latency Metrics (Adjusted) ---")
        print(summary.to_string(index=False))
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    analyze()
