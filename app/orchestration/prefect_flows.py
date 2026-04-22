from prefect import flow, task

from app.ingestion.batch.batch_pipeline import run_batch_pipeline
from app.transformation.run_batch_silver import run_batch_silver
from app.transformation.final_silver_pipeline import build_final_silver
from app.gold.gold_aggregations import run_gold_aggregations
from app.ingestion.streaming.adzuna_stream_producer import fetch_and_publish_adzuna_jobs


@task(name="Run batch ingestion")
def batch_ingestion_task():
    return run_batch_pipeline(csv_path="data/sample/ai_jobs.csv")


@task(name="Run batch silver transformation")
def batch_silver_task():
    return run_batch_silver()


@task(name="Build final silver")
def final_silver_task():
    return build_final_silver()


@task(name="Build gold aggregations")
def gold_task():
    return run_gold_aggregations()


@task(name="Publish fresh Adzuna jobs to Kafka")
def hourly_stream_producer_task():
    return fetch_and_publish_adzuna_jobs()


@flow(name="batch-refresh-flow")
def batch_refresh_flow():
    batch_ingestion_task()
    batch_silver_task()
    final_silver_task()
    gold_task()


@flow(name="hourly-stream-producer-flow")
def hourly_stream_producer_flow():
    hourly_stream_producer_task()


