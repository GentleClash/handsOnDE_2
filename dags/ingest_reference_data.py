from airflow.sdk import dag, task
import shutil
import os
from pathlib import Path
import pendulum

SOURCE_BASE_PATH = Path(os.getenv("SOURCE_BASE_PATH", "/opt/airflow/data/reference"))
DESTINATION_BASE_PATH = Path(os.getenv("DESTINATION_BASE_PATH", "/opt/airflow/data_lake/raw/reference/"))


@dag(
    dag_id="ingest_reference_data",
    start_date=pendulum.datetime(2026, 2, 5, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "local"],
)
def ingest_reference_data():
    """
    DAG to move reference CSV files from source to data lake with date partitioning.
    """

    @task
    def move_reference_csv(**context) -> dict:
        """
        Move CSV files from source to destination with date-based partitioning.
        """
        execution_date_str = context['ds']
        print(f"Processing for date: {execution_date_str}")

        src = SOURCE_BASE_PATH
        dest_base = DESTINATION_BASE_PATH

        if not src.exists():
            print(f"Source path {src} does not exist. Skipping.")
            return {"status": "skipped", "reason": "source_not_found"}

        destination_dir = dest_base / f"date={execution_date_str}"
        destination_dir.mkdir(parents=True, exist_ok=True)

        files_to_move = [f for f in os.listdir(src) if f.endswith(".csv")]
        
        if not files_to_move:
            print(f"No CSV files found in {src}")
            return {"status": "skipped", "reason": "no_files", "count": 0}

        moved_count = 0
        for file_name in files_to_move:
            source_file = src / file_name
            destination_file = destination_dir / file_name
            
            should_move = False

            if not destination_file.exists():
                should_move = True
            else:
                source_size = source_file.stat().st_size
                dest_size = destination_file.stat().st_size
                
                if source_size != dest_size:
                    should_move = True
                    print(f"File {file_name} size mismatch - will overwrite")
                else:
                    print(f"Skipping {file_name}: Already exists with same size.")

            if should_move:
                shutil.copy2(str(source_file), str(destination_file))
                print(f"Copied {file_name} to {destination_file}")
                moved_count += 1
        
        print(f"Total files copied: {moved_count}")
        return {"status": "success", "count": moved_count, "files": files_to_move}

    move_reference_csv()


_ = ingest_reference_data()