from pathlib import Path
import pandas as pd

from app.ingestion.batch.batch_pipeline import run_batch_pipeline
from app.schema.unified_schema import UNIFIED_JOB_FIELDS
from app.transformation.cleaning import (
    strip_text_columns,
    replace_empty_strings_with_null,
    clean_job_title_and_city_with_ftfy,
    clean_salary_columns,
    impute_salary_by_country_mean,
    fill_missing_city,
    clean_posted_year,
    fill_missing_employment_type,
    fill_missing_remote_type,
    drop_duplicates,
)
from app.transformation.normalization import (
    normalize_country,
    normalize_remote_type,
    normalize_experience_level,
    normalize_employment_type,
)
from app.transformation.validation import (
    validate_required_columns,
    summarize_nulls,
    summarize_shape,
)


def transform_jobs_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    validate_required_columns(df, UNIFIED_JOB_FIELDS)

    transformed_df = df.copy()

    transformed_df = strip_text_columns(transformed_df)
    transformed_df = replace_empty_strings_with_null(transformed_df)
    transformed_df = clean_job_title_and_city_with_ftfy(transformed_df)

    transformed_df = normalize_country(transformed_df)
    transformed_df = normalize_remote_type(transformed_df)
    transformed_df = normalize_experience_level(transformed_df)
    transformed_df = normalize_employment_type(transformed_df)
    transformed_df = fill_missing_city(transformed_df)
    transformed_df = clean_salary_columns(transformed_df)
    transformed_df = impute_salary_by_country_mean(transformed_df)
    transformed_df = fill_missing_employment_type(transformed_df)
    transformed_df = fill_missing_remote_type(transformed_df)
    transformed_df = clean_posted_year(transformed_df)

    transformed_df = drop_duplicates(transformed_df)

    return transformed_df


if __name__ == "__main__":
    csv_file = Path("data/processed/unified_jobs.csv")

    batch_df = run_batch_pipeline(
        csv_path=csv_file,
        adzuna_page=1,
        adzuna_results_per_page=10,
    )

    print("\nBatch dataframe shape before transform:")
    print(summarize_shape(batch_df))

    print("\nMissing salary values before transformation:")
    print(batch_df[["salary_min_usd", "salary_max_usd"]].isnull().sum())

    transformed_df = transform_jobs_dataframe(batch_df)
    print("\nUnique countries before transformation:")
    print(sorted(batch_df["country"].dropna().astype(str).unique()))

    print("\nTransformed dataframe shape:")
    print(summarize_shape(transformed_df))

    print("\nMissing salary values after transformation:")
    print(transformed_df[["salary_min_usd", "salary_max_usd"]].isnull().sum())


    print("\nNull summary after transformation:")
    print(summarize_nulls(transformed_df))
    print("\nUnique countries after transformation:")
    print(sorted(transformed_df["country"].dropna().astype(str).unique()))

    print("\nSample transformed rows:")
    print(transformed_df.head())

    output_path = Path("data/processed/transformed_jobs.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    transformed_df.to_csv(output_path, index=False)

    print(f"\nTransformed dataset saved to: {output_path}")