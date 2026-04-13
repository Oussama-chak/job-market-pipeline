from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.ingestion.batch.adzuna_client import (
    
    fetch_target_jobs_stream,
    map_adzuna_job_to_schema,
)
from app.ingestion.streaming.kafka_producer import create_kafka_producer


TOPIC_NAME = "jobs_topic"


def enrich_stream_record(record: dict[str, Any]) -> dict[str, Any]:
    record = record.copy()
    record["source"] = "adzuna"
    record["ingested_at"] = datetime.now(timezone.utc).isoformat()
    return record


def fetch_and_publish_adzuna_jobs() -> int:
    producer = create_kafka_producer()
    raw_jobs = fetch_target_jobs_stream()

    sent_count = 0

    for raw_job in raw_jobs:
        mapped_job = map_adzuna_job_to_schema(raw_job)
        stream_record = enrich_stream_record(mapped_job)

        producer.send(TOPIC_NAME, value=stream_record)
        sent_count += 1

    producer.flush()
    print(f"Published {sent_count} Adzuna jobs to Kafka topic '{TOPIC_NAME}'.")
    return sent_count


if __name__ == "__main__":
    fetch_and_publish_adzuna_jobs()