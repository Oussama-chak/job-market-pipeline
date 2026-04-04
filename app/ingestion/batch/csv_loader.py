from pathlib import Path
import pandas as pd

from app.schema.unified_schema import UNIFIED_JOB_FIELDS


TARGET_CSV_COUNTRIES = {"Germany", "Canada", "Canada ", "USA", "UK"}


def load_csv_dataset(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df


def filter_csv_countries(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["country"] = df["country"].astype(str).str.strip()
    filtered_df = df[df["country"].isin({"Germany", "Canada", "USA", "UK"})].copy()
    return filtered_df


def map_csv_to_schema(df: pd.DataFrame) -> pd.DataFrame:
    mapped_df = pd.DataFrame()

    mapped_df["job_id"] = df["job_id"].astype(str)
    mapped_df["job_title"] = df["job_title"]
    if "description" in df.columns:
        mapped_df["description"] = df["description"]
    elif "job_description" in df.columns:
        mapped_df["description"] = df["job_description"]
    else:
        mapped_df["description"] = pd.NA
    mapped_df["country"] = df["country"]
    mapped_df["city"] = df["city"]
    mapped_df["remote_type"] = df["remote_type"]
    mapped_df["experience_level"] = df["experience_level"]
    mapped_df["salary_min_usd"] = pd.to_numeric(df["salary_min_usd"], errors="coerce")
    mapped_df["salary_max_usd"] = pd.to_numeric(df["salary_max_usd"], errors="coerce")
    mapped_df["employment_type"] = df["employment_type"]
    mapped_df["posted_year"] = pd.to_numeric(df["posted_year"], errors="coerce").astype("Int64")

    mapped_df = mapped_df[UNIFIED_JOB_FIELDS]
    return mapped_df


def inspect_dataframe(df: pd.DataFrame, name: str = "DataFrame") -> None:
    print(f"\n{name} shape: {df.shape}")
    print(f"\n{name} columns:")
    print(df.columns.tolist())

    print(f"\n{name} null counts:")
    print(df.isnull().sum())

    print(f"\n{name} first 5 rows:")
    print(df.head())


if __name__ == "__main__":
    csv_file = Path("data/sample/ai_jobs.csv")

    raw_df = load_csv_dataset(csv_file)
    inspect_dataframe(raw_df, "Raw CSV")

    filtered_df = filter_csv_countries(raw_df)
    inspect_dataframe(filtered_df, "Filtered CSV")

    mapped_df = map_csv_to_schema(filtered_df)
    inspect_dataframe(mapped_df, "Mapped CSV")

    print("\nUnified schema columns check:")
    print(mapped_df.columns.tolist() == UNIFIED_JOB_FIELDS)