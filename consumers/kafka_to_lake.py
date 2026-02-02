import json
import os
from datetime import datetime
from confluent_kafka import Consumer
from argparse import ArgumentParser
from dotenv import load_dotenv
load_dotenv()



def get_config() -> dict:
    """Check for config in env otherwise use defaults"""

    BOOTSTRAP_SERVERS = os.getenv('KAFKA_CONSUMER_BOOTSTRAP_SERVERS', 'localhost:9092')
    GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP', 'raw-data-archive')
    OFFSET_RESET = os.getenv('KAFKA_CONSUMER_AUTO_OFFSET_RESET', 'earliest')
    bootstrap_conf = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': OFFSET_RESET
    }

    TOPICS = os.getenv('KAFKA_CONSUMER_TOPICS', 'orders.events,riders.events').split(',')
    BASE_DIR = os.getenv('DATA_LAKE_BASE_DIR', 'data_lake/raw')

    return {
        'conf': bootstrap_conf,
        'topics': TOPICS,
        'base_dir': BASE_DIR
    }


def get_file_path(topic: str, msg_value: bytes, base_dir: str) -> str:
    """
    returns datalake/raw/{topic}/region={region}/date={YYYY-MM-DD}/hour={HH}/chunk_{timestamp}.ndjson
    """
    
    
    try:
        data = json.loads(msg_value)
        region = data.get('region', 'unknown')
        event_ts = data.get('event_timestamp')

        dt = datetime.fromisoformat(event_ts)
    
    except (json.decoder.JSONDecodeError, ValueError, TypeError, AttributeError) as e:
        region = 'unknown'
        dt = datetime.now()

    date_str = dt.strftime('%Y-%m-%d')
    hour_str = dt.strftime('%H')

    # Topic -> region -> date -> hour
    dir_path = os.path.join(
                    base_dir, 
                    topic, 
                    f'region={region}', 
                    f'date={date_str}', 
                    f'hour={hour_str}')
    
    os.makedirs(dir_path, exist_ok=True)

    return os.path.join(dir_path, f'chunk_{int(datetime.now().timestamp())}.ndjson')

def flush_buffer(buffer: list, base_dir: str) -> None:
    if not buffer:
        return
    
    print(f"Flushing {len(buffer)} messages to disk...")
    
    files_to_write = {}

    for topic, msg_value in buffer:
        file_path = get_file_path(topic, msg_value, base_dir = base_dir)
        if file_path not in files_to_write:
            files_to_write[file_path] = []
        files_to_write[file_path].append(msg_value)
    
    for file_path, messages in files_to_write.items():
        with open(file_path, 'a') as f:
            for msg in messages:
                f.write(msg + '\n')
    
    print(f"Written {len(files_to_write)} files.")
    #buffer.clear()

def main(conf: dict, topics: list, base_dir: str, batch_size: int, flush_interval: int) -> None:
    consumer = Consumer(conf) # type: ignore
    consumer.subscribe(topics)

    buffer = []
    last_flush_time = datetime.now()

    try:
        while True:
            msg = consumer.poll(1.0)

            if (datetime.now() - last_flush_time).seconds >= flush_interval and buffer:
                flush_buffer(buffer, base_dir)
                buffer.clear()
                last_flush_time = datetime.now()

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            
            par = msg.partition()
            print(f"Consumed message from topic {msg.topic()}, partition {par}")

            val = msg.value().decode('utf-8') #type: ignore
            topic = msg.topic()
            buffer.append((topic, val))

            if len(buffer) >= batch_size:
                flush_buffer(buffer, base_dir)
                buffer.clear()
                last_flush_time = datetime.now()

    except KeyboardInterrupt:
        pass
    finally:
        if buffer: flush_buffer(buffer, base_dir)
        consumer.close()

if __name__ == "__main__":

    # cli
    parser = ArgumentParser()
    parser.add_argument('--batch-size', type=int, default=200)
    parser.add_argument('--flush-interval', type=int, default=10)
    args = parser.parse_args()

    config_dict = get_config()
    conf = config_dict['conf']
    topics = config_dict['topics']
    base_dir = config_dict['base_dir']


    batch_size = args.batch_size
    flush_interval = args.flush_interval
    main(conf, topics, base_dir, batch_size, flush_interval)