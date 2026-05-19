import time
import uuid
import random
import json
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration parameters
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "web_events"

# Load Avro Schema
schema_str = """{
  "type": "record",
  "name": "WebEvent",
  "namespace": "com.ecommerce.web",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "user_id", "type": ["null", "string"], "default": null},
    {"name": "event_type", "type": "string"},
    {"name": "product_id", "type": ["null", "string"], "default": null},
    {"name": "category", "type": ["null", "string"], "default": null},
    {"name": "timestamp", "type": "long"},
    {"name": "ip_address", "type": "string"},
    {"name": "device_type", "type": "string"}
  ]
}"""

def create_event():
    event_types = ["view_item", "add_to_cart", "remove_from_cart", "checkout", "search"]
    categories = ["Electronics", "Apparel", "Home", "Books", "Sports"]
    devices = ["mobile", "desktop", "tablet"]
    
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1000, 9999)),
        "event_type": random.choice(event_types),
        "product_id": f"PROD-{random.randint(100, 999)}",
        "category": random.choice(categories),
        "timestamp": int(time.time() * 1000),
        "ip_address": f"192.168.1.{random.randint(1, 254)}",
        "device_type": random.choice(devices)
    }

def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, schema_str)
    
    producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "value.serializer": avro_serializer
    })
    
    print(f"Starting simulated web events stream to Kafka topic: {TOPIC_NAME}")
    try:
        while True:
            event = create_event()
            producer.produce(topic=TOPIC_NAME, value=event)
            producer.flush()
            print(f"Sent event: {event['event_id']} - {event['event_type']}")
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print("Stopping web events producer.")

if __name__ == "__main__":
    main()
