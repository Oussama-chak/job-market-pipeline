import pandas as pd
from pathlib import Path

from app.ingestion.streaming.stream_loader import load_stream_jobs


BATCH_PATH = Path("data/processed/unified_jobs.csv")


def load_batch_jobs() -> pd.DataFrame:
    if not BATCH_PATH.exists():
        return pd.DataFrame()
    return pd.read_csv(BATCH_PATH)


def merge_batch_and_stream() -> pd.DataFrame:
    batch_df = load_batch_jobs()
    stream_df = load_stream_jobs()

    if batch_df.empty and stream_df.empty:
        return pd.DataFrame()

    if batch_df.empty:
        return stream_df

    if stream_df.empty:
        return batch_df

    return pd.concat([batch_df, stream_df], ignore_index=True)