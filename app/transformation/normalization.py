import pandas as pd


COUNTRY_MAPPING = {
    "US": "USA",
    

    "DE": "Germany",
    "Germany": "Germany",
    "Deutschland": "Germany",

}


def normalize_country(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["country"] = (
        df["country"]
        .astype("string")
        .str.strip()
        .map(COUNTRY_MAPPING)
        .fillna(df["country"])
    )

    return df


def normalize_remote_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    mapping = {
        "remote": "Remote",
        "hybrid": "Hybrid",
        "onsite": "Onsite",
        "on-site": "Onsite",
        "on site": "Onsite",
    }

    normalized = (
        df["remote_type"]
        .astype("string")
        .str.strip()
        .str.lower()
        .map(mapping)
    )

    df["remote_type"] = normalized.fillna(df["remote_type"])

    return df


def normalize_experience_level(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    mapping = {
        "entry": "Entry",
        "junior": "Entry",
        "jr": "Entry",
        "intern": "Entry",
        "internship": "Entry",
        "mid": "Mid",
        "intermediate": "Mid",
        "senior": "Senior",
        "sr": "Senior",
        "lead": "Senior",
        "principal": "Senior",
        "director": "Senior",
        "architect": "Senior",
        "head": "Senior",


    }

    normalized = (
        df["experience_level"]
        .astype("string")
        .str.strip()
        .str.lower()
        .map(mapping)
    )

    df["experience_level"] = normalized.fillna(df["experience_level"])

    return df


def normalize_employment_type(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    mapping = {
        "full-time": "Full-time",
        "full time": "Full-time",
        "full_time": "Full-time",
        "part-time": "Part-time",
        "part time": "Part-time",
        "part_time": "Part-time",
        "contract": "Contract",
        "permanent": "Full-time",
    }

    normalized = (
        df["employment_type"]
        .astype("string")
        .str.strip()
        .str.lower()
        .map(mapping)
    )

    df["employment_type"] = normalized.fillna(df["employment_type"])

    return df