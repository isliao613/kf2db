import argparse
import time
import random
import string
import json
import re
from datetime import datetime
from kafka import KafkaProducer

def generate_random_string(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

class FastProducer:
    def __init__(self, args):
        self.args = args
        self.raw_template = self._load_template()
        self.is_structured = False
        self.key_tmpl = '{"ID": {{id}}}'
        self.val_tmpl = self.raw_template
        self.headers_tmpl = None
        
        self._prepare_templates()

    def _load_template(self):
        if self.args.message_file:
            with open(self.args.message_file, 'r') as f:
                return f.read()
        return self.args.message or generate_random_string(self.args.message_size)

    def _prepare_templates(self):
        ID_VAL = "123456789"
        TS_VAL = "2099-01-01 00:00:00.000000000000"
        
        safe_json = self.raw_template.replace("{{id}}", ID_VAL).replace("{{timestamp}}", TS_VAL)
        
        try:
            full_data = json.loads(safe_json)
            if isinstance(full_data, dict) and any(k in full_data for k in ["key", "value", "headers"]):
                self.is_structured = True
                
                def to_tmpl(obj, force_json=False):
                    if force_json:
                        s = json.dumps(obj)
                    elif isinstance(obj, (dict, list)):
                        s = json.dumps(obj)
                    else:
                        s = str(obj)
                    return s.replace(TS_VAL, '{{timestamp}}').replace(ID_VAL, '{{id}}')

                if "key" in full_data:
                    self.key_tmpl = to_tmpl(full_data["key"], force_json=True)
                
                if "value" in full_data:
                    self.val_tmpl = to_tmpl(full_data["value"], force_json=True)
                else:
                    self.val_tmpl = self.raw_template
                
                if "headers" in full_data:
                    self.headers_tmpl = {k: to_tmpl(v, force_json=False) for k, v in full_data["headers"].items()}
        except json.JSONDecodeError:
            self.is_structured = False

    def get_data(self, index):
        now = datetime.now()
        ts_str = now.strftime('%Y-%m-%d %H:%M:%S.%f') + '000000'
        idx = str(index)
        
        key = self.key_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts_str).encode('utf-8')
        value = self.val_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts_str).encode('utf-8')
        
        headers = []
        if self.headers_tmpl:
            headers = [(k, v.replace("{{id}}", idx).replace("{{timestamp}}", ts_str).encode('utf-8')) 
                       for k, v in self.headers_tmpl.items()]
        
        return key, value, headers

def run_producer(args):
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        client_id='perf-producer',
        batch_size=args.batch_size,
        linger_ms=args.linger_ms,
        compression_type='lz4',
        acks=1
    )
    fast_data = FastProducer(args)
    
    print(f"Starting production to topic '{args.topic}'...")
    total_start = time.time()
    for iteration in range(args.iterations):
        iter_start = time.time()
        for i in range(args.num_messages):
            key, value, headers = fast_data.get_data(i + (iteration * args.num_messages))
            producer.send(args.topic, key=key, value=value, headers=headers)
        producer.flush()
        print(f"Iteration complete. Rate: {args.num_messages / (time.time() - iter_start):.2f} msg/sec")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="iidr.CDC.TEST_ORDERS")
    parser.add_argument("--num-messages", type=int, default=100000)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--rate", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=100000)
    parser.add_argument("--linger-ms", type=int, default=50)
    parser.add_argument("--message-file", type=str)
    parser.add_argument("--message", type=str)
    parser.add_argument("--message-size", type=int, default=1024)
    args = parser.parse_args()
    run_producer(args)
