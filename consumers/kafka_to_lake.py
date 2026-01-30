import json
import os
from datetime import datetime
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'raw-data-archive',
    'auto.offset.reset': 'earliest'
}

TOPICS = ['orders.events', 'riders.events']


BASE_DIR = 'data_lake/raw' #todo: use gcs
BATCH_SIZE = 200  
FLUSH_INTERVAL = 10  

def get_file_path(topic: str, msg_value: bytes) -> str:
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
                    BASE_DIR, 
                    topic, 
                    f'region={region}', 
                    f'date={date_str}', 
                    f'hour={hour_str}')
    
    os.makedirs(dir_path, exist_ok=True)

    return os.path.join(dir_path, f'chunk_{int(datetime.now().timestamp())}.ndjson')

def flush_buffer(buffer: list) -> None:
    if not buffer:
        return
    
    print(f"Flushing {len(buffer)} messages to disk...")
    
    files_to_write = {}

    for topic, msg_value in buffer:
        file_path = get_file_path(topic, msg_value)
        if file_path not in files_to_write:
            files_to_write[file_path] = []
        files_to_write[file_path].append(msg_value)
    
    for file_path, messages in files_to_write.items():
        with open(file_path, 'a') as f:
            for msg in messages:
                f.write(msg + '\n')
    
    print(f"Written {len(files_to_write)} files.")
    #buffer.clear()

def main() -> None:
    consumer = Consumer(conf) # type: ignore
    consumer.subscribe(TOPICS)

    buffer = []
    last_flush_time = datetime.now()

    try:
        while True:
            msg = consumer.poll(1.0)

            if (datetime.now() - last_flush_time).seconds >= FLUSH_INTERVAL and buffer:
                flush_buffer(buffer)
                buffer.clear()
                last_flush_time = datetime.now()

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            val = msg.value().decode('utf-8') #type: ignore
            topic = msg.topic()
            buffer.append((topic, val))

            if len(buffer) >= BATCH_SIZE:
                flush_buffer(buffer)
                buffer.clear()
                last_flush_time = datetime.now()

    except KeyboardInterrupt:
        pass
    finally:
        if buffer: flush_buffer(buffer)
        consumer.close()

if __name__ == "__main__":
    main()