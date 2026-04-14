from pathlib import Path
import pandas as pd


BATCH_SILVER_PATH = Path("data/processed/batch_transformed_jobs.csv")
STREAM_SILVER_PATH = Path("data/processed/stream_transformed_jobs.csv")
FINAL_SILVER_PATH = Path("data/processed/final_silver_jobs.csv")


def _load_csv_if_exists(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"File not found: {path}")
        return pd.DataFrame()
    return pd.read_csv(path)


def _align_columns(df1: pd.DataFrame, df2: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_cols = sorted(set(df1.columns).union(set(df2.columns)))

    for col in all_cols:
        if col not in df1.columns:
            df1[col] = pd.NA
        if col not in df2.columns:
            df2[col] = pd.NA

    return df1[all_cols], df2[all_cols]


def build_final_silver() -> pd.DataFrame:
    batch_df = _load_csv_if_exists(BATCH_SILVER_PATH)
    stream_df = _load_csv_if_exists(STREAM_SILVER_PATH)

    if batch_df.empty and stream_df.empty:
        print("No batch or stream Silver data found.")
        return pd.DataFrame()

    if batch_df.empty:
        final_df = stream_df.copy()
    elif stream_df.empty:
        final_df = batch_df.copy()
    else:
        batch_df, stream_df = _align_columns(batch_df, stream_df)
        final_df = pd.concat([batch_df, stream_df], ignore_index=True)

    if "job_id" in final_df.columns:
        final_df = final_df.drop_duplicates(subset=["job_id"], keep="first")
    else:
        final_df = final_df.drop_duplicates()

    FINAL_SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(FINAL_SILVER_PATH, index=False)

    print(f"Final Silver dataset saved to: {FINAL_SILVER_PATH}")
    print(f"Shape: {final_df.shape}")

    return final_df


if __name__ == "__main__":
    build_final_silver()