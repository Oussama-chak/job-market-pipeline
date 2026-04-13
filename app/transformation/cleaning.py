import re
import pandas as pd
from ftfy import fix_text


TEXT_COLUMNS = [
    "job_title",
    "description",
    "country",
    "city",
    "remote_type",
    "experience_level",
    "employment_type",
]

TEXT_CLEAN_COLUMNS = ["job_title", "city","description"]


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


def sanitize_extracted_city(candidate, country=None):
    """
    Only normalize:
    - Remote
    - Hybrid
    - Onsite

    Otherwise return cleaned candidate as-is.
    """

    if pd.isna(candidate):
        return None

    text = str(candidate).strip()
    if not text:
        return None

    text_lower = text.lower()

    if "remote" in text_lower:
        return "Remote"

    if "hybrid" in text_lower:
        return "Hybrid"

    if "onsite" in text_lower or "on-site" in text_lower or "on site" in text_lower:
        return country if pd.notna(country) else None

    return text

def fill_missing_city(df: pd.DataFrame) -> pd.DataFrame:
    """
    Improved strategy:

    Step 1: if city exists -> keep it
    Step 2: if city missing, use remote_type FIRST:
        - Remote -> "Remote"
        - Hybrid -> country
        - Onsite -> country
    Step 3: if still missing -> extract from description
    Step 4: final fallback -> country
    """
    df = df.copy()

    if "city" not in df.columns:
        return df

    if "country" not in df.columns:
        df["country"] = pd.NA

    if "remote_type" not in df.columns:
        df["remote_type"] = pd.NA

    if "description" not in df.columns:
        df["description"] = pd.NA

    def extract_city_from_description(description, country):
        if pd.isna(description):
            return None

        text = str(description).strip()
        if not text:
            return None

        patterns = [
            r"location\s*:\s*([A-ZÀ-ÖØ-Ý][A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+)",
            r"city\s*:\s*([A-ZÀ-ÖØ-Ý][A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+)",
            r"based in\s+([A-ZÀ-ÖØ-Ý][A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+)",
            r"located in\s+([A-ZÀ-ÖØ-Ý][A-Za-zÀ-ÖØ-öø-ÿ'`\- ]+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                candidate = match.group(1).strip()
                candidate = re.sub(r"\s+", " ", candidate)
                candidate = candidate.strip(" ,.;:-")
                return sanitize_extracted_city(candidate, country)

        return None

    # Identify missing city
    city_series = df["city"].astype("string")
    missing_city_mask = city_series.isna() | (city_series.str.strip() == "")

    remote_series = df["remote_type"].astype("string").str.strip()

    # ✅ Step 2: PRIORITY → remote_type
    remote_mask = missing_city_mask & (remote_series == "Remote")
    hybrid_mask = missing_city_mask & (remote_series == "Hybrid")
    onsite_mask = missing_city_mask & (remote_series == "Onsite")

    df.loc[remote_mask, "city"] = "Remote"
    df.loc[hybrid_mask, "city"] = df.loc[hybrid_mask, "country"]
    df.loc[onsite_mask, "city"] = df.loc[onsite_mask, "country"]

    # Recompute missing after remote_type
    city_series = df["city"].astype("string")
    missing_city_mask = city_series.isna() | (city_series.str.strip() == "")

    # ✅ Step 3: extract from description ONLY if still missing
    inferred_city = df.apply(
        lambda row: extract_city_from_description(row["description"], row["country"]),
        axis=1
    )
    fill_from_desc_mask = missing_city_mask & inferred_city.notna()
    df.loc[fill_from_desc_mask, "city"] = inferred_city[fill_from_desc_mask]

    # Recompute again
    city_series = df["city"].astype("string")
    missing_city_mask = city_series.isna() | (city_series.str.strip() == "")

    # ✅ Step 4: final fallback → country
    df.loc[missing_city_mask, "city"] = df.loc[missing_city_mask, "country"]

    return df


def fill_missing_employment_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "employment_type" not in df.columns:
        return df

    if "description" not in df.columns:
        df["description"] = pd.NA

    def infer_from_description(text):
        if pd.isna(text):
            return None

        text = str(text).lower()

        if "full time" in text or "full-time" in text:
            return "Full-time"

        if "part time" in text or "part-time" in text:
            return "Part-time"

        if "contract" in text or "contractor" in text:
            return "Contract"

        if "intern" in text or "internship" in text:
            return "Internship"

        if "temporary" in text or "temp" in text:
            return "Temporary"

        return None

    missing_mask = (
        df["employment_type"].isna()
        | (df["employment_type"].astype("string").str.strip() == "")
    )

    inferred = df.loc[missing_mask, "description"].apply(infer_from_description)

    df.loc[missing_mask, "employment_type"] = inferred

    # fallback
    still_missing = (
        df["employment_type"].isna()
        | (df["employment_type"].astype("string").str.strip() == "")
    )

    df.loc[still_missing, "employment_type"] = "Full-time"

    return df



def fill_missing_remote_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "remote_type" not in df.columns:
        return df

    if "description" not in df.columns:
        df["description"] = pd.NA

    def infer_remote(text):
        if pd.isna(text):
            return None

        text = str(text).lower()

        if "remote" in text or "work from home" in text or "wfh" in text:
            return "Remote"

        if "hybrid" in text:
            return "Hybrid"

        if "on-site" in text or "onsite" in text or "on site" in text:
            return "Onsite"

        return None

    missing_mask = (
        df["remote_type"].isna()
        | (df["remote_type"].astype("string").str.strip() == "")
    )

    inferred = df.loc[missing_mask, "description"].apply(infer_remote)

    df.loc[missing_mask, "remote_type"] = inferred

    # fallback
    still_missing = (
        df["remote_type"].isna()
        | (df["remote_type"].astype("string").str.strip() == "")
    )

    df.loc[still_missing, "remote_type"] = "Onsite"

    return df