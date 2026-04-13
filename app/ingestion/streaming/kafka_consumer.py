import json
import pandas as pd
from kafka import KafkaConsumer

from app.ingestion.streaming.stream_pipeline import (
    save_raw_record,
    append_processed_record,
)
from app.transformation.transform_pipeline import transform_jobs_dataframe


def create_kafka_consumer(
    topic: str = "jobs_topic",
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "jobs-consumer-group",
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


if __name__ == "__main__":
    consumer = create_kafka_consumer()

    print("Listening to Kafka topic and processing incoming jobs...")

    for message in consumer:
        record = message.value

        print("Received:", record)

        # Step 1: save raw Bronze record
        save_raw_record(record)

        # Step 2: convert one Kafka message into a one-row DataFrame
        df = pd.DataFrame([record])

        # Step 3: run your transformation pipeline
        transformed_df = transform_jobs_dataframe(df)

        # Step 4: save cleaned Silver row
        append_processed_record(transformed_df)

        print("Processed and saved:", record.get("job_title"))