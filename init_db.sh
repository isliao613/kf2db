#!/bin/bash
set -e

TABLE_NAME=${1:-test_orders}

echo "Initializing YugabyteDB table '$TABLE_NAME' with lowercase columns..."

# Connect via container IP
HOST=$(docker exec yugabyte hostname -i)
echo "Resolved Yugabyte IP: $HOST"

# Wait for YSQL to be ready
docker exec yugabyte bash -c "until bin/ysqlsh -h $HOST -U yugabyte -c 'select 1' > /dev/null 2>&1; do echo 'Waiting for YSQL...'; sleep 2; done"

# Create table with lowercase columns
docker exec yugabyte bin/ysqlsh -h $HOST -U yugabyte -d yugabyte -c "
DROP TABLE IF EXISTS $TABLE_NAME;
CREATE TABLE $TABLE_NAME (
    id BIGINT PRIMARY KEY,
    name TEXT,
    amount DECIMAL,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_date DATE,
    order_time TIME
);"

echo "Table '$TABLE_NAME' created successfully."
