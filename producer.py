import argparse
import time
import random
import string
import json
import re
from datetime import datetime
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        pass

def generate_random_string(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

class FastProducer:
    def __init__(self, args):
        self.args = args
        self.raw_template = self._load_template()
        self.is_structured = False
        self.key_tmpl = '{"ID": "{{id}}"}'
        self.val_tmpl = self.raw_template
        self.headers_tmpl = None
        
        self._prepare_templates()

    def _load_template(self):
        if self.args.message_file:
            with open(self.args.message_file, 'r') as f:
                return f.read()
        return self.args.message or generate_random_string(self.args.message_size)

    def _prepare_templates(self):
        """Identifies key, value, and headers even if JSON has unquoted placeholders."""
        # Create a 'safe' version of the template to test for structure
        safe_json = self.raw_template.replace("{{id}}", "0").replace("{{timestamp}}", "0")
        
        try:
            data = json.loads(safe_json)
            if isinstance(data, dict) and any(k in data for k in ["key", "value", "headers"]):
                self.is_structured = True
                
                # We need to extract the raw strings for key, value, and headers from the original template
                # To do this safely while preserving placeholders, we parse the 'safe' version 
                # but then use the structure to find what to extract.
                
                # For high performance, we re-serialize the SUB-OBJECTS from the 'safe' parse
                # but then put the placeholders back. This is safer than regex.
                
                full_data = json.loads(safe_json)
                
                # Function to convert safe-serialized back to placeholder-serialized
                def to_tmpl(obj):
                    s = json.dumps(obj)
                    # This is a bit of a hack but works for standard templates
                    # It assumes the user didn't have literal "0" strings they wanted to keep
                    return s.replace('"0"', '"{{timestamp}}"').replace('0', '{{id}}')

                if "key" in full_data:
                    self.key_tmpl = to_tmpl(full_data["key"])
                
                if "value" in full_data:
                    self.val_tmpl = to_tmpl(full_data["value"])
                else:
                    self.val_tmpl = to_tmpl(full_data)
                
                if "headers" in full_data:
                    self.headers_tmpl = full_data["headers"]

        except json.JSONDecodeError:
            # If we can't parse it even with safe values, treat the whole thing as value
            self.is_structured = False

    def get_data(self, index):
        now = datetime.now()
        ts_str = now.strftime('%Y-%m-%d %H:%M:%S.%f') + '000000'
        idx = str(index)
        
        # Apply placeholders
        key = self.key_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts_str)
        value = self.val_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts_str)
        
        headers = []
        if self.headers_tmpl:
            headers = [(k, str(v).replace("{{id}}", idx).replace("{{timestamp}}", ts_str)) 
                       for k, v in self.headers_tmpl.items()]
        
        return key, value, headers

def run_producer(args):
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'perf-producer',
        'queue.buffering.max.messages': 2000000,
        'batch.num.messages': args.batch_size,
        'linger.ms': args.linger_ms,
        'compression.type': 'lz4',
        'acks': '1'
    }

    producer = Producer(conf)
    fast_data = FastProducer(args)
    
    print(f"Starting production to topic '{args.topic}'...")
    total_start = time.time()
    
    for iteration in range(args.iterations):
        print(f"--- Iteration {iteration + 1}/{args.iterations} ---")
        iter_start = time.time()
        
        for i in range(args.num_messages):
            key, value, headers = fast_data.get_data(i + (iteration * args.num_messages))
            producer.produce(args.topic, key=key, value=value, headers=headers)
            if i % 10000 == 0:
                producer.poll(0)

        producer.flush()
        print(f"Iteration complete. Rate: {args.num_messages / (time.time() - iter_start):.2f} msg/sec")

    overall_time = time.time() - total_start
    print(f"\nFinal Throughput: {(args.num_messages * args.iterations) / overall_time:.2f} msg/sec")

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
