from pathlib import Path
import duckdb


DB_PATH = Path("data/job_market.duckdb")

BATCH_SILVER_PATH = Path("data/processed/batch_transformed_jobs.csv")
STREAM_SILVER_PATH = Path("data/processed/stream_transformed_jobs.csv")
FINAL_SILVER_PATH = Path("data/processed/final_silver_jobs.csv")

GOLD_DIR = Path("data/gold")


def connect_duckdb():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def create_table_from_csv(con, table_name: str, csv_path: Path):
    if not csv_path.exists():
        print(f"Skipping missing file: {csv_path}")
        return

    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto(
            '{csv_path.as_posix()}',
            header=True,
            all_varchar=true,
            sample_size=-1,
            ignore_errors=true
        )
        """
    )
    print(f"Loaded table: {table_name} from {csv_path}")


def create_or_replace_view(con, view_name: str, query: str):
    con.execute(f"DROP VIEW IF EXISTS {view_name}")
    con.execute(f"CREATE VIEW {view_name} AS {query}")
    print(f"Created view: {view_name}")


def load_silver_tables(con):
    create_table_from_csv(con, "batch_transformed_jobs", BATCH_SILVER_PATH)
    create_table_from_csv(con, "stream_transformed_jobs", STREAM_SILVER_PATH)
    create_table_from_csv(con, "final_silver_jobs", FINAL_SILVER_PATH)


def load_gold_tables(con):
    gold_files = {
        "overview_metrics": GOLD_DIR / "overview_metrics.csv",
        "jobs_by_country": GOLD_DIR / "jobs_by_country.csv",
        "jobs_by_city": GOLD_DIR / "jobs_by_city.csv",
        "jobs_by_remote_type": GOLD_DIR / "jobs_by_remote_type.csv",
        "remote_type_by_country": GOLD_DIR / "remote_type_by_country.csv",
        "salary_by_country": GOLD_DIR / "salary_by_country.csv",
        "salary_by_remote_type": GOLD_DIR / "salary_by_remote_type.csv",
        "salary_distribution": GOLD_DIR / "salary_distribution.csv",
    }

    for table_name, file_path in gold_files.items():
        create_table_from_csv(con, table_name, file_path)


def create_views(con):
    create_or_replace_view(
        con,
        "top_10_countries_by_jobs",
        """
        SELECT *
        FROM jobs_by_country
        ORDER BY CAST(job_count AS BIGINT) DESC
        LIMIT 10
        """,
    )

    create_or_replace_view(
        con,
        "top_10_cities_by_jobs",
        """
        SELECT *
        FROM jobs_by_city
        ORDER BY CAST(job_count AS BIGINT) DESC
        LIMIT 10
        """,
    )

    create_or_replace_view(
        con,
        "top_salary_countries",
        """
        SELECT *
        FROM salary_by_country
        WHERE avg_salary_usd IS NOT NULL AND avg_salary_usd <> ''
        ORDER BY CAST(avg_salary_usd AS DOUBLE) DESC
        LIMIT 10
        """,
    )

    create_or_replace_view(
        con,
        "final_silver_jobs_typed",
        """
        SELECT
            job_id,
            job_title,
            description,
            country,
            city,
            remote_type,
            experience_level,
            TRY_CAST(salary_min_usd AS DOUBLE) AS salary_min_usd,
            TRY_CAST(salary_max_usd AS DOUBLE) AS salary_max_usd,
            employment_type,
            TRY_CAST(posted_year AS INTEGER) AS posted_year,
            job_description,
            source,
            ingested_at
        FROM final_silver_jobs
        """,
    )


def load_all_to_duckdb():
    con = connect_duckdb()

    try:
        load_silver_tables(con)
        load_gold_tables(con)
        create_views(con)
        print(f"DuckDB loaded successfully at: {DB_PATH}")
    finally:
        con.close()


if __name__ == "__main__":
    load_all_to_duckdb()