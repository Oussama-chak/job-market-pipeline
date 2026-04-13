import json
from pathlib import Path
import pandas as pd


RAW_STREAM_PATH = Path("data/raw/stream_jobs.jsonl")
PROCESSED_STREAM_PATH = Path("data/processed/stream_transformed_jobs.csv")


def save_raw_record(record: dict) -> None:
    RAW_STREAM_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(RAW_STREAM_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def append_processed_record(df: pd.DataFrame) -> None:
    PROCESSED_STREAM_PATH.parent.mkdir(parents=True, exist_ok=True)

    if PROCESSED_STREAM_PATH.exists():
        df.to_csv(PROCESSED_STREAM_PATH, mode="a", header=False, index=False)
    else:
        df.to_csv(PROCESSED_STREAM_PATH, index=False)   