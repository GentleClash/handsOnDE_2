import json
import time
from datetime import datetime
from confluent_kafka import Producer
from synthetic_data_generator import SyntheticDataGenerator
from typing import Dict
import uuid


conf: Dict[str, str] = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf) #type: ignore

print("Initializing synthetic data generator...")
data_generator = SyntheticDataGenerator(output_dir='data')
data_generator.initialize()


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


print("Starting data production loop...")

try:
    while True:
        print(f"Generating batch for {datetime.now()}...")
        orders, riders = data_generator.generate_events(
            datetime.now(),
            duration_minutes=1,
            output_file=None
        )

        for order_item in orders:
            key_bytes, value_bytes = None, None
            try:
                if isinstance(order_item, str):
                    value_bytes = order_item.encode('utf-8')
                    order_dict = json.loads(order_item)
                    key_bytes = order_dict.get('order_id', str(uuid.uuid4())).encode('utf-8')
                else:
                    value_bytes = json.dumps(order_item).encode('utf-8')
                    key_bytes = order_item.get('order_id', str(uuid.uuid4())).encode('utf-8')

            except (json.decoder.JSONDecodeError, AttributeError) as e:
                    print(f"Error processing order item: {e}")
                    key_bytes = str(uuid.uuid4()).encode('utf-8')
                    if value_bytes is None:
                        value_bytes = str(order_item).encode('utf-8')

            producer.produce(
                topic='orders.events',
                key = key_bytes,
                value = value_bytes, 
                on_delivery = delivery_report,
            )
        
        for rider_item in riders:
            key_bytes, value_bytes = None, None
            try:
                if isinstance(rider_item, str):
                    value_bytes = rider_item.encode('utf-8')
                    rider_dict = json.loads(rider_item)
                    key_bytes = rider_dict.get('rider_id', str(uuid.uuid4())).encode('utf-8')
                else:
                    value_bytes = json.dumps(rider_item).encode('utf-8')
                    key_bytes = rider_item.get('rider_id', str(uuid.uuid4())).encode('utf-8')

            except (json.decoder.JSONDecodeError, AttributeError) as e:
                    print(f"Error processing rider item: {e}")
                    key_bytes = str(uuid.uuid4()).encode('utf-8')
                    if value_bytes is None:
                        value_bytes = str(rider_item).encode('utf-8')

            producer.produce(
                topic='riders.events',
                key = key_bytes,
                value = value_bytes, 
                on_delivery = delivery_report,
            )
        
        producer.poll(0)
        producer.flush()
        time.sleep(10)

except KeyboardInterrupt:
    print("Data production interrupted. Exiting...")
