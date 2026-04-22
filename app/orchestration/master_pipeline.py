import subprocess
import sys
import time
from pathlib import Path

from prefect import flow, task

from app.ingestion.batch.batch_pipeline import run_batch_pipeline
from app.transformation.run_batch_silver import run_batch_silver
from app.transformation.final_silver_pipeline import build_final_silver
from app.gold.gold_aggregations import run_gold_aggregations
from app.ingestion.streaming.adzuna_stream_producer import fetch_and_publish_adzuna_jobs


CONSUMER_MODULE = "app.ingestion.streaming.kafka_consumer"
CSV_SAMPLE_PATH = "data/sample/ai_jobs.csv"


@task(name="Run batch ingestion")
def batch_ingestion_task():
    return run_batch_pipeline(csv_path=CSV_SAMPLE_PATH)


@task(name="Run batch silver transformation")
def batch_silver_task():
    return run_batch_silver()


@task(name="Publish stream jobs to Kafka")
def stream_producer_task():
    return fetch_and_publish_adzuna_jobs()


@task(name="Wait for stream processing")
def wait_for_stream_processing(seconds: int = 20):
    print(f"Waiting {seconds} seconds for Kafka consumer to process stream jobs...")
    time.sleep(seconds)


@task(name="Build final silver")
def final_silver_task():
    return build_final_silver()


@task(name="Build gold aggregations")
def gold_task():
    return run_gold_aggregations()


def start_kafka_consumer() -> subprocess.Popen:
    print("Starting Kafka consumer in background...")
    process = subprocess.Popen(
        [sys.executable, "-m", CONSUMER_MODULE],
        stdout=None,
        stderr=None,
    )
    return process


def stop_kafka_consumer(process: subprocess.Popen):
    if process and process.poll() is None:
        print("Stopping Kafka consumer...")
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


@flow(name="master-project-pipeline")
def master_project_pipeline():
    consumer_process = None

    try:
        # 1. Start consumer
        consumer_process = start_kafka_consumer()

        # give consumer a few seconds to connect
        time.sleep(5)

        # 2. Batch side
        batch_ingestion_task()
        batch_silver_task()

        # 3. Stream side
        stream_producer_task()

        # 4. Wait for consumer to process published jobs
        wait_for_stream_processing()

        # 5. Consolidation
        final_silver_task()

        # 6. Gold refresh
        gold_task()

        print("Master project pipeline completed successfully.")

    finally:
        stop_kafka_consumer(consumer_process)


if __name__ == "__main__":
    master_project_pipeline()