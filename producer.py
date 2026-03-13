import argparse
import time
import random
import string
import json
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        pass # Printing here is a bottleneck at 100k msg/s

def generate_random_string(size):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

class FastProducer:
    def __init__(self, args):
        self.args = args
        self.raw_template = self._load_template()
        self.is_structured = False
        self.key_tmpl = None
        self.val_tmpl = None
        self.headers_tmpl = None
        
        self._prepare_templates()

    def _load_template(self):
        if self.args.message_file:
            with open(self.args.message_file, 'r') as f:
                return f.read()
        return self.args.message or generate_random_string(self.args.message_size)

    def _prepare_templates(self):
        """Pre-processes the template to move JSON logic out of the hot loop."""
        try:
            data = json.loads(self.raw_template)
            if isinstance(data, dict) and any(k in data for k in ["key", "value", "headers"]):
                self.is_structured = True
                
                # Pre-serialize sub-components so we can use string replacement
                key_obj = data.get("key", "{{id}}")
                self.key_tmpl = json.dumps(key_obj) if isinstance(key_obj, (dict, list)) else str(key_obj)
                
                val_obj = data.get("value", "")
                self.val_tmpl = json.dumps(val_obj) if isinstance(val_obj, (dict, list)) else str(val_obj)
                
                self.headers_tmpl = data.get("headers", {})
        except json.JSONDecodeError:
            self.val_tmpl = self.raw_template

    def get_data(self, index):
        ts = str(time.time())
        idx = str(index)
        
        if not self.is_structured:
            return idx, self.val_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts), None

        key = self.key_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts)
        value = self.val_tmpl.replace("{{id}}", idx).replace("{{timestamp}}", ts)
        
        # Headers are still slightly expensive, but usually small
        headers = None
        if self.headers_tmpl:
            headers = [(k, str(v).replace("{{id}}", idx).replace("{{timestamp}}", ts)) 
                       for k, v in self.headers_tmpl.items()]
        
        return key, value, headers

def run_producer(args):
    # High-performance config
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'perf-producer',
        'queue.buffering.max.messages': 2000000,
        'queue.buffering.max.kbytes': 1048576, # 1GB
        'batch.num.messages': args.batch_size,
        'linger.ms': args.linger_ms,
        'compression.type': 'lz4', # Faster than snappy for high throughput
        'acks': '1',
        'sticky.partitioning.linger.ms': 10 # Improve partition distribution
    }

    producer = Producer(conf)
    fast_data = FastProducer(args)
    
    print(f"Starting production to topic '{args.topic}'...")
    total_start = time.time()
    
    for iteration in range(args.iterations):
        print(f"--- Iteration {iteration + 1}/{args.iterations} ---")
        iter_start = time.time()
        
        # High-precision rate limiting
        rate = args.rate
        if rate > 0:
            for i in range(args.num_messages):
                key, value, headers = fast_data.get_data(i + (iteration * args.num_messages))
                producer.produce(args.topic, key=key, value=value, headers=headers)
                
                # Periodic polling
                if i % 10000 == 0:
                    producer.poll(0)
                
                # Minimal sleep for rate limiting
                expected_time = (i + 1) / rate
                actual_time = time.time() - iter_start
                if actual_time < expected_time:
                    time.sleep(expected_time - actual_time)
        else:
            # Max speed path
            for i in range(args.num_messages):
                key, value, headers = fast_data.get_data(i + (iteration * args.num_messages))
                producer.produce(args.topic, key=key, value=value, headers=headers)
                if i % 10000 == 0:
                    producer.poll(0)

        print(f"Flushing iteration {iteration + 1}...")
        producer.flush()
        iter_duration = time.time() - iter_start
        print(f"Iteration complete. Rate: {args.num_messages / iter_duration:.2f} msg/sec")

    overall_time = time.time() - total_start
    total_msgs = args.num_messages * args.iterations
    print(f"\nFinal Throughput: {total_msgs / overall_time:.2f} msg/sec")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="perf-test")
    parser.add_argument("--num-messages", type=int, default=100000)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument("--rate", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=100000)
    parser.add_argument("--linger-ms", type=int, default=50)
    parser.add_argument("--message-file", type=str)
    parser.add_argument("--message", type=str)
    parser.add_argument("--message-size", type=int, default=1024)
    parser.add_argument("--partition-key", type=str) # Legacy arg kept for compat
    args = parser.parse_args()
    run_producer(args)
