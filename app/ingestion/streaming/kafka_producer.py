import json
from kafka import KafkaProducer


def create_kafka_producer(
    bootstrap_servers: str = "localhost:9092",
) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def send_test_jobs():
    producer = create_kafka_producer()

    jobs = [
        {
            "job_title": "Data Engineer",
            "description": "Remote full-time role using Python, SQL and Airflow",
            "country": "Germany",
            "city": None,
            "remote_type": None,
            "experience_level": "Mid-level",
            "employment_type": None,
            "salary_min_usd": 50000,
            "salary_max_usd": 70000,
            "posted_year": 2026,
        },
        {
            "job_title": "ML Engineer",
            "description": "Hybrid contract role in Berlin using Python and AWS",
            "country": "Germany",
            "city": None,
            "remote_type": None,
            "experience_level": "Senior",
            "employment_type": None,
            "salary_min_usd": 65000,
            "salary_max_usd": 85000,
            "posted_year": 2026,
        },
    ]

    for job in jobs:
        producer.send("jobs_topic", value=job)

    producer.flush()
    print("Test jobs sent to Kafka.")


if __name__ == "__main__":
    send_test_jobs()