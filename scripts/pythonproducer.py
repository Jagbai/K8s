from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("producer")

# Change this depending on where you're running:
# - Local machine → localhost:31092
# - Inside Kubernetes → redpanda:9093
BROKER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    retries=5
)

def on_send_success(record_metadata):
    log.info(f"✅ Sent to topic={record_metadata.topic}, partition={record_metadata.partition}, offset={record_metadata.offset}")

def on_send_error(excp):
    log.error("❌ Error sending message", exc_info=excp)

pages = ["/home", "/menu", "/checkout", "/order"]
users = [f"user_{i}" for i in range(1, 6)]

try:
    while True:
        event = {
            "user": random.choice(users),
            "page": random.choice(pages),
            "timestamp": time.time()
        }
        future = producer.send("clickstream", value=event)
        future.add_callback(on_send_success).add_errback(on_send_error)
        producer.flush()
        time.sleep(1)
except KeyboardInterrupt:
    log.info("Stopping producer.")
finally:
    producer.close()
