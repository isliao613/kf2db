#!/bin/bash
set -e

echo "Initializing YugabyteDB table with lowercase columns..."

# Connect via container IP
HOST=$(docker exec yugabyte hostname -i)
echo "Resolved Yugabyte IP: $HOST"

# Wait for YSQL to be ready
docker exec yugabyte bash -c "until bin/ysqlsh -h $HOST -U yugabyte -c 'select 1' > /dev/null 2>&1; do echo 'Waiting for YSQL...'; sleep 2; done"

# Create table with lowercase columns
docker exec yugabyte bin/ysqlsh -h $HOST -U yugabyte -d yugabyte -c "
DROP TABLE IF EXISTS test_orders;
CREATE TABLE test_orders (
    id BIGINT PRIMARY KEY,
    order_name TEXT,
    amount DECIMAL,
    status TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_date DATE,
    order_time TIME
);"

echo "Table 'test_orders' created successfully with lowercase columns."
