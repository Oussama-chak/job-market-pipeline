import re
import pandas as pd
from ftfy import fix_text


TEXT_COLUMNS = [
    "job_title",
    "country",
    "city",
    "remote_type",
    "experience_level",
    "employment_type",
]

TEXT_CLEAN_COLUMNS = ["job_title", "city"]


def _normalize_whitespace(value: str) -> str:
    if not isinstance(value, str):
        return value

    value = value.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def fix_text_with_ftfy(value: str) -> str:
    """
    Fix mojibake / encoding issues while preserving valid Unicode text (ftfy).
    Examples: KÃ¶ln -> Köln, DÃ©veloppeur -> Développeur
    """
    if not isinstance(value, str):
        return value
    try:
        return fix_text(value)
    except Exception:
        return value


def _ftfy_then_normalize_whitespace(value):
    if pd.isna(value) or not isinstance(value, str):
        return value
    return _normalize_whitespace(fix_text_with_ftfy(value))


def clean_text_columns_with_ftfy(
    df: pd.DataFrame,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Run ftfy + whitespace normalization on each cell of the given columns.
    """
    cols = TEXT_CLEAN_COLUMNS if columns is None else columns
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[col] = df[col].apply(_ftfy_then_normalize_whitespace)
    return df


def clean_job_title_and_city_with_ftfy(df: pd.DataFrame) -> pd.DataFrame:
    """ftfy + whitespace for job_title and city."""
    return clean_text_columns_with_ftfy(df, TEXT_CLEAN_COLUMNS)


def strip_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    return df


def replace_empty_strings_with_null(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        if df[col].dtype == "object" or str(df[col].dtype) == "string":
            df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)

    return df


def clean_salary_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["salary_min_usd"] = pd.to_numeric(df["salary_min_usd"], errors="coerce")
    df["salary_max_usd"] = pd.to_numeric(df["salary_max_usd"], errors="coerce")

    df.loc[df["salary_min_usd"] < 0, "salary_min_usd"] = pd.NA
    df.loc[df["salary_max_usd"] < 0, "salary_max_usd"] = pd.NA

    swap_mask = (
        df["salary_min_usd"].notna()
        & df["salary_max_usd"].notna()
        & (df["salary_min_usd"] > df["salary_max_usd"])
    )

    df.loc[swap_mask, ["salary_min_usd", "salary_max_usd"]] = df.loc[
        swap_mask, ["salary_max_usd", "salary_min_usd"]
    ].values

    return df


def impute_salary_by_country_mean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["salary_min_usd"] = pd.to_numeric(df["salary_min_usd"], errors="coerce")
    df["salary_max_usd"] = pd.to_numeric(df["salary_max_usd"], errors="coerce")

    country_salary_min_mean = df.groupby("country")["salary_min_usd"].transform("mean")
    country_salary_max_mean = df.groupby("country")["salary_max_usd"].transform("mean")

    df["salary_min_usd"] = df["salary_min_usd"].fillna(country_salary_min_mean)
    df["salary_max_usd"] = df["salary_max_usd"].fillna(country_salary_max_mean)

    return df


def clean_posted_year(df: pd.DataFrame, min_year: int = 2020, max_year: int = 2026) -> pd.DataFrame:
    df = df.copy()

    df["posted_year"] = pd.to_numeric(df["posted_year"], errors="coerce")
    invalid_mask = (df["posted_year"] < min_year) | (df["posted_year"] > max_year)
    df.loc[invalid_mask, "posted_year"] = pd.NA
    df["posted_year"] = df["posted_year"].astype("Int64")

    return df


def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.drop_duplicates(
        subset=[
            "job_title",
            "country",
            "city",
            "remote_type",
            "experience_level",
            "salary_min_usd",
            "salary_max_usd",
            "employment_type",
            "posted_year",
        ]
    )

    return df