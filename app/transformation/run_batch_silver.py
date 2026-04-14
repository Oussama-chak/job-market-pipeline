from pathlib import Path
import pandas as pd

from app.transformation.transform_pipeline import transform_jobs_dataframe



BATCH_BRONZE_PATH = Path("data/processed/batch_unified_jobs.csv")
BATCH_SILVER_PATH = Path("data/processed/batch_transformed_jobs.csv")



def run_batch_silver() -> pd.DataFrame:
    if not BATCH_BRONZE_PATH.exists():
        print(f"Batch bronze file not found: {BATCH_BRONZE_PATH}")
        return pd.DataFrame()

    df = pd.read_csv(BATCH_BRONZE_PATH)

    if df.empty:
        print("Batch bronze dataset is empty.")
        return df

    transformed_df = transform_jobs_dataframe(df)

    BATCH_SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    transformed_df.to_csv(BATCH_SILVER_PATH, index=False)

    print(f"Batch Silver dataset saved to: {BATCH_SILVER_PATH}")
    print(f"Shape: {transformed_df.shape}")

    return transformed_df


if __name__ == "__main__":
    run_batch_silver()