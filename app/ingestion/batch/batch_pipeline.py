from pathlib import Path
import pandas as pd

from app.ingestion.batch.csv_loader import (
    load_csv_dataset,
    filter_csv_countries,
    map_csv_to_schema,
)
from app.ingestion.batch.adzuna_client import (
    fetch_target_jobs,
    map_adzuna_job_to_schema,
)
from app.schema.unified_schema import UNIFIED_JOB_FIELDS


def build_csv_dataframe(csv_path: str) -> pd.DataFrame:
    raw_csv_df = load_csv_dataset(csv_path)
    filtered_csv_df = filter_csv_countries(raw_csv_df)
    mapped_csv_df = map_csv_to_schema(filtered_csv_df)
    return mapped_csv_df


def build_adzuna_dataframe(page: int = 10, results_per_page: int = 30) -> pd.DataFrame:
    jobs = fetch_target_jobs()
    mapped_jobs = [map_adzuna_job_to_schema(job) for job in jobs]

    if not mapped_jobs:
        return pd.DataFrame(columns=UNIFIED_JOB_FIELDS)

    adzuna_df = pd.DataFrame(mapped_jobs)
    adzuna_df = adzuna_df[UNIFIED_JOB_FIELDS]
    return adzuna_df


def run_batch_pipeline(
    csv_path: str,
    adzuna_page: int = 10,
    adzuna_results_per_page: int = 30,
) -> pd.DataFrame:
    csv_df = build_csv_dataframe(csv_path)
    adzuna_df = build_adzuna_dataframe(
        page=adzuna_page,
        results_per_page=adzuna_results_per_page,
    )

    unified_df = pd.concat([csv_df, adzuna_df], ignore_index=True)
    return unified_df


if __name__ == "__main__":
    csv_file = Path("data/sample/ai_jobs.csv")

    unified_df = run_batch_pipeline(
        csv_path=csv_file,
        adzuna_page=3,
        adzuna_results_per_page=30,
    )

    print("\nUnified dataset shape:", unified_df.shape)
    print("\nUnified dataset columns:")
    print(unified_df.columns.tolist())

    print("\nCountry distribution:")
    print(unified_df["country"].value_counts(dropna=False))

    print("\nJob title sample:")
    print(unified_df["job_title"].head(10))

    output_path = Path("data/processed/batch_unified_jobs.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    unified_df.to_csv(output_path, index=False)

    print(f"\nUnified dataset saved to: {output_path}")