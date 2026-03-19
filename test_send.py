from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

msg1 = {
    "key": {"ID": 999},
    "value": {
        "ORDER_NAME": "test_1",
        "AMOUNT": 100.0,
        "STATUS": "OK",
        "CREATED_AT": "2026-03-19 10:00:00",
        "UPDATED_AT": "2026-03-19 10:00:00",
        "ORDER_DATE": "2026-03-19",
        "ORDER_TIME": "10:00:00"
    }
}

producer.send('iidr.CDC.TEST_ORDERS', key=json.dumps(msg1["key"]).encode('utf-8'), value=json.dumps(msg1["value"]).encode('utf-8'))

msg2 = {
    "key": {"ID": 888},
    "value": {
        "ORDER_NAME": "test_2",
        "AMOUNT": 200.0,
        "STATUS": "OK",
        "CREATED_AT": "2026-03-19 10:00:00",
        "UPDATED_AT": "2026-03-19 10:00:00",
        "ORDER_DATE": "2026-03-19",
        "ORDER_TIME": "10:00:00"
    }
}
producer.send('iidr.CDC.TEST_ORDERS_2', key=json.dumps(msg2["key"]).encode('utf-8'), value=json.dumps(msg2["value"]).encode('utf-8'))

producer.flush()
print("Sent 2 test messages.")
