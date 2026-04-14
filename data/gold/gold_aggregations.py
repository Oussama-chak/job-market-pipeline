from pathlib import Path
import pandas as pd


SILVER_PATH = Path("data/processed/final_silver_jobs.csv")
GOLD_DIR = Path("data/gold")


def load_silver_data() -> pd.DataFrame:
    if not SILVER_PATH.exists():
        raise FileNotFoundError(f"Missing file: {SILVER_PATH}")

    df = pd.read_csv(SILVER_PATH)
    return df


def add_salary_avg(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "salary_min_usd" in df.columns and "salary_max_usd" in df.columns:
        df["salary_avg_usd"] = (
            pd.to_numeric(df["salary_min_usd"], errors="coerce") +
            pd.to_numeric(df["salary_max_usd"], errors="coerce")
        ) / 2

    return df


def build_overview_metrics(df: pd.DataFrame) -> pd.DataFrame:
    metrics = {
        "total_jobs": len(df),
        "total_countries": df["country"].nunique(dropna=True) if "country" in df.columns else 0,
        "total_cities": df["city"].nunique(dropna=True) if "city" in df.columns else 0,
        "remote_jobs": (df["remote_type"] == "Remote").sum() if "remote_type" in df.columns else 0,
        "hybrid_jobs": (df["remote_type"] == "Hybrid").sum() if "remote_type" in df.columns else 0,
        "onsite_jobs": (df["remote_type"] == "Onsite").sum() if "remote_type" in df.columns else 0,
        "average_salary_usd": round(df["salary_avg_usd"].mean(), 2) if "salary_avg_usd" in df.columns else None,
    }

    return pd.DataFrame([metrics])


def build_jobs_by_country(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("country", dropna=False)
        .size()
        .reset_index(name="job_count")
        .sort_values("job_count", ascending=False)
    )


def build_jobs_by_city(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["country", "city"], dropna=False)
        .size()
        .reset_index(name="job_count")
        .sort_values("job_count", ascending=False)
    )


def build_jobs_by_remote_type(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("remote_type", dropna=False)
        .size()
        .reset_index(name="job_count")
        .sort_values("job_count", ascending=False)
    )


def build_remote_type_by_country(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["country", "remote_type"], dropna=False)
        .size()
        .reset_index(name="job_count")
        .sort_values(["country", "job_count"], ascending=[True, False])
    )


def build_salary_by_country(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("country", dropna=False)["salary_avg_usd"]
        .mean()
        .reset_index(name="avg_salary_usd")
        .sort_values("avg_salary_usd", ascending=False)
    )


def build_salary_by_remote_type(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("remote_type", dropna=False)["salary_avg_usd"]
        .mean()
        .reset_index(name="avg_salary_usd")
        .sort_values("avg_salary_usd", ascending=False)
    )


def build_salary_distribution(df: pd.DataFrame) -> pd.DataFrame:
    salary_df = df[["salary_avg_usd"]].copy()
    salary_df = salary_df.dropna()
    return salary_df


def save_table(df: pd.DataFrame, filename: str) -> None:
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(GOLD_DIR / filename, index=False)


def run_gold_aggregations() -> None:
    df = load_silver_data()
    df = add_salary_avg(df)

    save_table(build_overview_metrics(df), "overview_metrics.csv")
    save_table(build_jobs_by_country(df), "jobs_by_country.csv")
    save_table(build_jobs_by_city(df), "jobs_by_city.csv")
    save_table(build_jobs_by_remote_type(df), "jobs_by_remote_type.csv")
    save_table(build_remote_type_by_country(df), "remote_type_by_country.csv")
    save_table(build_salary_by_country(df), "salary_by_country.csv")
    save_table(build_salary_by_remote_type(df), "salary_by_remote_type.csv")
    save_table(build_salary_distribution(df), "salary_distribution.csv")

    print("Gold tables generated successfully.")


if __name__ == "__main__":
    run_gold_aggregations()