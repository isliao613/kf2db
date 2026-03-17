import argparse
import subprocess
import time
import json
import sys
import os
from datetime import datetime
import multiprocessing

def run_command(cmd, shell=True):
    result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error executing: {cmd}")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
    return result.stdout, result.returncode

def get_yugabyte_ip():
    stdout, _ = run_command("docker exec yugabyte hostname -i")
    return stdout.strip()

def setup_db(num_tables, with_trigger):
    ip = get_yugabyte_ip()
    print(f"Setting up {num_tables} tables in YugabyteDB (trigger={with_trigger})...")
    
    sql = ""
    for i in range(num_tables):
        table_name = f"test_orders_{i}"
        sql += f"DROP TABLE IF EXISTS {table_name}; "
        sql += f"""
CREATE TABLE {table_name} (
    id BIGINT PRIMARY KEY,
    order_name TEXT,
    amount DECIMAL,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_date DATE,
    order_time TIME,
    kafka_at TIMESTAMP,
    consumer_at TIMESTAMP,
    yb_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
); """
        if with_trigger:
            sql += f"""
CREATE OR REPLACE FUNCTION update_timestamp_{i}() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trigger_update_{i} BEFORE INSERT OR UPDATE ON {table_name}
FOR EACH ROW EXECUTE FUNCTION update_timestamp_{i}();
"""
    
    # Run SQL via ysqlsh
    cmd = f"docker exec yugabyte bin/ysqlsh -h {ip} -U yugabyte -d yugabyte -c \"{sql}\""
    run_command(cmd)
    print("Database setup complete.")

def setup_connectors(num_tables):
    print(f"Setting up connectors for {num_tables} topics...")
    # Delete existing connectors first to be clean
    stdout, _ = run_command("curl -s http://localhost:8083/connectors")
    try:
        connectors = json.loads(stdout)
        for c in connectors:
            run_command(f"curl -s -X DELETE http://localhost:8083/connectors/{c}")
    except:
        pass

    # Use a single connector for all topics or one per topic? 
    # Usually one per topic is easier to manage for benchmarks.
    with open("connector.json", "r") as f:
        base_config = json.load(f)

    for i in range(num_tables):
        table_name = f"test_orders_{i}"
        topic_name = f"iidr.CDC.TEST_ORDERS_{i}"
        
        config = base_config.copy()
        config["name"] = f"jdbc-sink-{table_name}"
        config["config"] = base_config["config"].copy()
        config["config"]["topics"] = topic_name
        config["config"]["table.name.format"] = table_name
        config["config"]["tasks.max"] = "32"
        config["config"]["transforms.iidrToJdbc.table.name.filter"] = f"TEST_ORDERS_{i}"
        
        print(f"DEBUG: Submitting connector {config['name']} with tasks.max={config['config']['tasks.max']}")
        
        # Submit to Kafka Connect
        cmd = f"curl -s -X POST -H \"Content-Type: application/json\" --data '{json.dumps(config)}' http://localhost:8083/connectors"
        run_command(cmd)
    
    print(f"Connector setup complete for {num_tables} tables.")

def run_single_producer(producer_id, topic, table_name, num_messages, rate, results_dict):
    start_time = datetime.now().isoformat()
    cmd = [
        "python3", "producer.py",
        "--topic", topic,
        "--table-name", table_name,
        "--num-messages", str(num_messages),
        "--rate", str(rate),
        "--message-file", "template.json",
        "--producer-id", str(producer_id)
    ]
    print(f"Starting Producer {producer_id} for topic {topic} (rate={rate})...")
    subprocess.run(cmd)
    end_time = datetime.now().isoformat()
    results_dict[producer_id] = {"start": start_time, "end": end_time}

def analyze_results(num_tables, args, producers_summary):
    ip = get_yugabyte_ip()
    print("\n--- Benchmark Analysis ---")
    
    report_lines = []
    report_lines.append("# Performance Benchmark Report")
    report_lines.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"**Target Tables:** {num_tables}")
    report_lines.append(f"**Producers:** {args.num_producers}")
    report_lines.append(f"**Target Rate:** {args.num_producers * args.rate_per_producer} msg/s")
    report_lines.append(f"**Triggers Enabled:** {args.with_trigger}")
    
    report_lines.append("\n## Producer Statistics (Client -> Kafka)")
    report_lines.append("| Producer ID | Start Time | End Time |")
    report_lines.append("| --- | --- | --- |")
    for pid in sorted(producers_summary.keys()):
        s = producers_summary[pid]
        report_lines.append(f"| {pid} | {s['start']} | {s['end']} |")

    report_lines.append("\n## Table Metrics (Kafka -> YugabyteDB)")
    
    table_headers = [
        "Table", "MIN(yb_at)", "MAX(yb_at)", "MAX-MIN(yb_at)", 
        "MAX(yb_at-kafka_at)", "AVG(yb_at-kafka_at)", 
        "MAX(yb_at-consumer_at)", "AVG(yb_at-consumer_at)", 
        "MAX(consumer_at-kafka_at)", "AVG(consumer_at-kafka_at)"
    ]
    report_lines.append("| " + " | ".join(table_headers) + " |")
    report_lines.append("| " + " | ".join(["---"] * len(table_headers)) + " |")

    metrics_sql = """
    SELECT 
        MIN(yb_at AT TIME ZONE 'UTC') as min_yb,
        MAX(yb_at AT TIME ZONE 'UTC') as max_yb,
        EXTRACT(EPOCH FROM (MAX(yb_at) - MIN(yb_at))) as duration,
        EXTRACT(EPOCH FROM MAX((yb_at AT TIME ZONE 'UTC') - kafka_at)) as max_yb_ka,
        EXTRACT(EPOCH FROM AVG((yb_at AT TIME ZONE 'UTC') - kafka_at)) as avg_yb_ka,
        EXTRACT(EPOCH FROM MAX((yb_at AT TIME ZONE 'UTC') - consumer_at)) as max_yb_co,
        EXTRACT(EPOCH FROM AVG((yb_at AT TIME ZONE 'UTC') - consumer_at)) as avg_yb_co,
        EXTRACT(EPOCH FROM MAX(consumer_at - kafka_at)) as max_co_ka,
        EXTRACT(EPOCH FROM AVG(consumer_at - kafka_at)) as avg_co_ka
    FROM {table_name};
    """
    
    total_messages = 0
    all_avg_yb_ka = []
    
    for i in range(num_tables):
        table_name = f"test_orders_{i}"
        
        # Get count
        count_out, _ = run_command(f"docker exec yugabyte bin/ysqlsh -h {ip} -U yugabyte -d yugabyte -t -c \"SELECT count(*) FROM {table_name}\"")
        count = int(count_out.strip()) if count_out.strip() else 0
        total_messages += count
        
        # Get metrics
        sql = metrics_sql.format(table_name=table_name)
        stdout, _ = run_command(f"docker exec yugabyte bin/ysqlsh -h {ip} -U yugabyte -d yugabyte -t -A -F ',' -c \"{sql}\"")
        
        if stdout.strip():
            parts = stdout.strip().split(',')
            if len(parts) >= 9:
                min_yb, max_yb, dur, m_yb_ka, a_yb_ka, m_yb_co, a_yb_co, m_co_ka, a_co_ka = parts
                row = [table_name, min_yb, max_yb, dur, m_yb_ka, a_yb_ka, m_yb_co, a_yb_co, m_co_ka, a_co_ka]
                report_lines.append("| " + " | ".join(row) + " |")
                if a_yb_ka: all_avg_yb_ka.append(float(a_yb_ka))

    report_lines.append("\n## Summary")
    overall_avg = sum(all_avg_yb_ka) / len(all_avg_yb_ka) if all_avg_yb_ka else 0
    report_lines.append(f"- **Total Messages Synchronized:** {total_messages}")
    report_lines.append(f"- **Global Average End-to-End Latency:** {overall_avg:.4f} s")
    
    report_content = "\n".join(report_lines)
    with open("performance_report.md", "w") as f:
        f.write(report_content)
    
    print(report_content)
    print("\nReport saved to performance_report.md")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-tables", type=int, default=5)
    parser.add_argument("--with-trigger", action="store_true")
    parser.add_argument("--num-producers", type=int, default=5)
    parser.add_argument("--rate-per-producer", type=int, default=4000, help="Rate per producer instance (total target is 20000)")
    parser.add_argument("--messages-per-producer", type=int, default=100000)
    args = parser.parse_args()

    # 1. Setup DB
    setup_db(args.num_tables, args.with_trigger)
    
    # 2. Setup Connectors
    setup_connectors(args.num_tables)
    
    # 3. Wait for connectors to be ready
    print("Waiting 10s for connectors to initialize...")
    time.sleep(10)
    
    # 4. Run Producers in parallel
    manager = multiprocessing.Manager()
    producers_summary = manager.dict()
    processes = []
    for i in range(args.num_producers):
        table_idx = i % args.num_tables
        topic = f"iidr.CDC.TEST_ORDERS_{table_idx}"
        table_name = f"TEST_ORDERS_{table_idx}"
        p = multiprocessing.Process(target=run_single_producer, args=(i, topic, table_name, args.messages_per_producer, args.rate_per_producer, producers_summary))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()
    
    print("All producers finished. Waiting 15s for sink to catch up...")
    time.sleep(15)
    
    # 5. Analyze and Generate Report
    analyze_results(args.num_tables, args, producers_summary)

if __name__ == "__main__":
    main()
