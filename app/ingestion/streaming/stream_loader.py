import pandas as pd
from pathlib import Path


RAW_STREAM_PATH = Path("data/raw/stream_jobs.jsonl")


def load_stream_jobs() -> pd.DataFrame:
    if not RAW_STREAM_PATH.exists():
        return pd.DataFrame()

    return pd.read_json(RAW_STREAM_PATH, lines=True)