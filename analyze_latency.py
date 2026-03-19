import psycopg2
import argparse
from datetime import datetime

def analyze(tables, host, port, dbname, user, password):
    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
        
        # 1. Calibrate Clock Offset
        host_now = datetime.utcnow()
        with conn.cursor() as cur:
            cur.execute("SELECT (clock_timestamp() AT TIME ZONE 'UTC')")
            db_now = cur.fetchone()[0]
        
        offset_ms = (db_now.replace(tzinfo=None) - host_now).total_seconds() * 1000
        print(f"--- Calibration ---")
        print(f"Detected Clock Offset (DB - Host): {offset_ms:.2f} ms\n")

        for table in tables:
            print(f"--- Analysis for Table: {table} ---")
            # 2. Define Latency Expressions
            t1_to_t2 = "EXTRACT(EPOCH FROM (kafka_at - created_at)) * 1000"
            t2_to_t4 = f"(EXTRACT(EPOCH FROM (db_at - kafka_at)) * 1000) - {offset_ms}"
            t1_to_t4 = f"(EXTRACT(EPOCH FROM (db_at - created_at)) * 1000) - {offset_ms}"

            # 3. Fetch Summary Metrics
            summary_query = f"""
            SELECT 
                MIN({t1_to_t2}), AVG({t1_to_t2}), MAX({t1_to_t2}),
                MIN({t2_to_t4}), AVG({t2_to_t4}), MAX({t2_to_t4}),
                MIN({t1_to_t4}), AVG({t1_to_t4}), MAX({t1_to_t4}),
                COUNT(*)
            FROM {table};
            """
            
            with conn.cursor() as cur:
                try:
                    cur.execute(summary_query)
                    row = cur.fetchone()
                except Exception as table_err:
                    print(f"Error reading table {table}: {table_err}")
                    conn.rollback()
                    continue
                
                if not row or row[0] is None or row[9] == 0:
                    print(f"No data found in {table} table.\n")
                    continue

                metrics = [
                    ("Client to Kafka", row[0], row[1], row[2]),
                    ("Kafka to DB",     row[3], row[4], row[5]),
                    ("Total E2E",      row[6], row[7], row[8])
                ]

                print(f"Total Rows: {row[9]}")
                print(f"{'Metric Segment':<20} | {'Min (ms)':>10} | {'Avg (ms)':>10} | {'Max (ms)':>10}")
                print("-" * 60)
                for label, min_val, avg_val, max_val in metrics:
                    print(f"{label:<20} | {min_val:>10.3f} | {avg_val:>10.3f} | {max_val:>10.3f}")
                print("\n")
        
    except Exception as e:
        print(f"Global Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", nargs='+', default=["test_orders"], help="List of tables to analyze")
    parser.add_argument("--db-host", default="localhost")
    parser.add_argument("--db-port", type=int, default=5433)
    parser.add_argument("--db-name", default="yugabyte")
    parser.add_argument("--db-user", default="yugabyte")
    parser.add_argument("--db-password", default="yugabyte")
    
    args = parser.parse_args()
    analyze(args.tables, args.db_host, args.db_port, args.db_name, args.db_user, args.db_password)
