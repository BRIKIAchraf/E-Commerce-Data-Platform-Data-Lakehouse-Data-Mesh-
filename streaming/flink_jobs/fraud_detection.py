import json

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors import FlinkKafkaConsumer
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common import WatermarkStrategy
except ImportError:
    StreamExecutionEnvironment = None
    FlinkKafkaConsumer = None
    SimpleStringSchema = None
    WatermarkStrategy = None

# Minimal simulated fraud logic on continuous orders stream
def detect_fraud(order_event_str):
    try:
        order = json.loads(order_event_str)
        # Rule 1: High transaction amount > $1200
        # Rule 2: Potential rapid transaction (can be complex, kept simple here)
        if order["total_amount"] > 1200.0:
            return json.dumps({
                "alert_type": "HIGH_VALUE_TRANSACTION",
                "order_id": order["order_id"],
                "user_id": order["user_id"],
                "total_amount": order["total_amount"],
                "severity": "CRITICAL"
            })
    except Exception as e:
        return None
    return None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "flink-fraud-detector"
    }

    consumer = FlinkKafkaConsumer(
        "orders_cdc",
        SimpleStringSchema(),
        kafka_props
    )

    orders_stream = env.add_source(consumer)
    
    fraud_stream = orders_stream \
        .map(detect_fraud) \
        .filter(lambda x: x is not None)

    # Output to stdout/console (easily connected to s3/slack sinks)
    print("Streaming Fraud Detection job started...")
    fraud_stream.print()

    env.execute("Real-time Fraud Detection")

if __name__ == "__main__":
    main()
