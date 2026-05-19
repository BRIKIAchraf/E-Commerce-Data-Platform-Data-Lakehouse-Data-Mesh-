import json

try:
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors import FlinkKafkaConsumer
    from pyflink.common.serialization import SimpleStringSchema
except ImportError:
    StreamExecutionEnvironment = None
    FlinkKafkaConsumer = None
    SimpleStringSchema = None

def process_stock_event(event_str):
    try:
        event = json.loads(event_str)
        if event.get("event_type") == "add_to_cart":
            # Live deduction simulation of stock
            return json.dumps({
                "product_id": event["product_id"],
                "quantity_deducted": 1,
                "timestamp": event["timestamp"]
            })
    except Exception:
        return None
    return None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_props = {"bootstrap.servers": "localhost:9092", "group.id": "flink-stock-updater"}
    consumer = FlinkKafkaConsumer("web_events", SimpleStringSchema(), kafka_props)
    
    stream = env.add_source(consumer)
    
    stock_deductions = stream \
        .map(process_stock_event) \
        .filter(lambda x: x is not None)
        
    print("Real-time Stock Updater Flink Job started...")
    stock_deductions.print()
    env.execute("Live Stock Updates Engine")

if __name__ == "__main__":
    main()
