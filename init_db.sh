#!/bin/bash
set -e

echo "Initializing YugabyteDB tables with lowercase columns..."

# Connect via container IP
HOST=$(docker exec yugabyte hostname -i)
echo "Resolved Yugabyte IP: $HOST"

# Wait for YSQL to be ready
docker exec yugabyte bash -c "until bin/ysqlsh -h $HOST -U yugabyte -c 'select 1' > /dev/null 2>&1; do echo 'Waiting for YSQL...'; sleep 2; done"

# Define the table schema once to avoid duplication
SCHEMA="
    id BIGINT PRIMARY KEY,
    order_name TEXT,
    amount DECIMAL,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    order_date DATE,
    order_time TIME,
    kafka_at TIMESTAMPTZ,
    db_at TIMESTAMPTZ DEFAULT (clock_timestamp() AT TIME ZONE 'UTC')
"

# Create both tables
for TABLE in test_orders test_orders_2; do
    echo "Creating/Resetting table: $TABLE..."
    docker exec yugabyte bin/ysqlsh -h $HOST -U yugabyte -d yugabyte -c "
    DROP TABLE IF EXISTS $TABLE;
    CREATE TABLE $TABLE ($SCHEMA);"
done

echo "Tables initialized successfully."
