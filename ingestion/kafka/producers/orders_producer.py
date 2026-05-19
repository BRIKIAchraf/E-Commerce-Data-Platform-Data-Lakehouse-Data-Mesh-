import time
import uuid
import random
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration parameters
BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "orders_cdc"

schema_str = """{
  "type": "record",
  "name": "Order",
  "namespace": "com.ecommerce.orders",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "total_amount", "type": "double"},
    {"name": "status", "type": "string"},
    {"name": "payment_method", "type": "string"},
    {"name": "shipping_address", "type": "string"},
    {"name": "created_at", "type": "long"},
    {"name": "updated_at", "type": "long"}
  ]
}"""

def create_order():
    payment_methods = ["credit_card", "paypal", "apple_pay", "stripe"]
    statuses = ["created", "processing", "completed", "cancelled"]
    
    return {
        "order_id": str(uuid.uuid4()),
        "user_id": f"USER-{random.randint(1000, 9999)}",
        "total_amount": round(random.uniform(10.0, 1500.0), 2),
        "status": random.choice(statuses),
        "payment_method": random.choice(payment_methods),
        "shipping_address": f"{random.randint(1, 999)} Rue de Rivoli, Paris, France",
        "created_at": int(time.time() * 1000),
        "updated_at": int(time.time() * 1000)
    }

def main():
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr_client, schema_str)
    
    producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "value.serializer": avro_serializer
    })
    
    print(f"Starting simulated orders CDC stream to Kafka topic: {TOPIC_NAME}")
    try:
        while True:
            order = create_order()
            producer.produce(topic=TOPIC_NAME, value=order)
            producer.flush()
            print(f"Sent Order: {order['order_id']} - EUR {order['total_amount']}")
            time.sleep(random.uniform(1.0, 5.0))
    except KeyboardInterrupt:
        print("Stopping orders producer.")

if __name__ == "__main__":
    main()
