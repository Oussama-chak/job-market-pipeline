from app.ingestion.streaming.merge_sources import merge_batch_and_stream
from app.transformation.transform_pipeline import transform_jobs_dataframe


if __name__ == "__main__":
    df = merge_batch_and_stream()

    if df.empty:
        print("No data found.")
    else:
        transformed_df = transform_jobs_dataframe(df)
        transformed_df.to_csv("data/processed/transformed_jobs.csv", index=False)
        print("Unified transformed dataset saved.")