# Real-Time Event Ingestion Project: A Dockerized Kafka streaming pipeline for decoupling microservices.

## Tech Stack
- Kafka
- Docker
- Python
- Airflow

## Architecture / What It Does
This learning project is designed to handle high-throughput telemetry and transaction data, capable of processing 20,000+ events/min. The custom Python producers generate and manage realistic streaming data using custom batching mechanisms to optimize network I/O and latency. The consumer applications then dump these events into a data lake organized with Hive-style partitioning, preparing it for downstream analytics or batch processing.

## Directory Structure / Where to Look
- **`consumers/`**: Logic for reading off topics and landing data into the lake.
- **`dags/`**: Airflow DAGs for orchestrating batch pipeline components. 
- **`data_lake/`**: The target landing zone for events, structured via Hive-style partitioning.
- **`producer/`**: High-throughput synthetic data generation and streaming logic.

Note: Detailed documentation on the producer logic, architecture, and mathematical models can be found in the [/producer/producer_README.md](./producer/producer_README.md).

## How to Run It

**1. Start the Data Pipelines** (this automatically starts the Kafka brokers):
```bash
docker-compose up producer consumer_1 consumer_2
```
The producers will start generating events and the consumers will begin processing them, landing the data into the `data_lake/` directory.

**2. Start Airflow (UI + Scheduler)** (this automatically starts `airflow-db`, `airflow-init`, and `airflow-dag-processor` via dependencies):
```bash
docker-compose up airflow-scheduler
```
The service will be available at `http://localhost:8080` with the default credentials (username: `admin`, password: `admin`).
