import json
import os
import time
from datetime import datetime
from confluent_kafka import Producer
from synthetic_data_generator import SyntheticDataGenerator
from typing import Dict
import uuid
from dotenv import load_dotenv
load_dotenv()


# There is no __main__ guard here as this script is intended to be run directly.

def get_config() -> dict:
    """Check for config in env otherwise use defaults."""

    BOOTSTRAP_SERVERS = os.getenv('KAFKA_PRODUCER_BOOTSTRAP_SERVERS', 'localhost:9092')

    conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS
    }

    BASE_DIR = os.getenv('SYNTHETIC_DATA_OUTPUT_DIR', 'data')

    return {
        'kafka-conf': conf,
        'base_dir': BASE_DIR
    }


conf: Dict[str, str] = get_config()

producer = Producer(conf['kafka-conf']) #type: ignore

print("Initializing synthetic data generator...")
data_generator = SyntheticDataGenerator(output_dir=conf['base_dir'], csv_flush_interval=60)
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
        orders, riders = data_generator.generate_events_for_minute(
            datetime.now()
        )

        for order_item in orders:
            key_bytes, value_bytes = None, None
            try:
                if isinstance(order_item, str): # Broken json string
                    value_bytes = order_item.encode('utf-8')
                    order_dict = json.loads(order_item)
                    key_bytes = order_dict.get('order_id', str(uuid.uuid4())).encode('utf-8') #Use random UUID if missing
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
