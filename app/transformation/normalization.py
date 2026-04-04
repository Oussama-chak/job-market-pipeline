import re

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


_EXPERIENCE_LEVEL_MAPPING = {
    "entry": "Entry",
    "entry level": "Entry",
    "entry-level": "Entry",
    "junior": "Entry",
    "jr": "Entry",
    "jr.": "Entry",
    "intern": "Entry",
    "internship": "Entry",
    "graduate": "Entry",
    "grad": "Entry",
    "trainee": "Entry",
    "apprentice": "Entry",
    "student": "Entry",
    "mid": "Mid",
    "mid-level": "Mid",
    "mid level": "Mid",
    "intermediate": "Mid",
    "medium": "Mid",
    "senior": "Senior",
    "sr": "Senior",
    "researcher": "Senior",
    "chercheur": "Senior",
    "sr.": "Senior",
    "lead": "Senior",
    "principal": "Senior",
    "director": "Senior",
    "architect": "Senior",
    "head": "Senior",
    "staff": "Senior",
    "vp": "Senior",
    "vice president": "Senior",
    "expert": "Senior",
    "executive": "Senior",
    "chief": "Senior",
    "fellow": "Senior",
    "distinguished": "Senior",
}

_ENTRY_IN_TEXT = re.compile(
    r"\b(junior|jr\.?|intern(ship)?|graduate|entry[\s-]?level|trainee|apprentice)\b",
    re.IGNORECASE,
)
_MID_IN_TEXT = re.compile(
    r"\b(mid[\s-]?level|intermediate|middle)\b|\bmid\b",
    re.IGNORECASE,
)
_SENIOR_IN_TEXT = re.compile(
    r"\b(senior|sr\.?|lead|principal|director|head\s+of|vice[\s-]president|"
    r"v\.?p\.?|chief|executive|distinguished|fellow|staff)\b",
    re.IGNORECASE,
)
# Role titles that imply senior band when the source leaves experience_level blank
_ARCHITECT_OR_SIMILAR_ROLE = re.compile(
    r"\b(data|solutions?|solution|cloud|enterprise|technical|software|systems?|ai|"
    r"security|infrastructure|application|it|platform|domain)\s+architect\b|\barchitect\b",
    re.IGNORECASE,
)
_ENTRY_YEARS_HINT = re.compile(
    r"\b(0[-–]?[12]\+?\s+years?|0[-–]?[12]\s+yrs?|1[-–]?[23]\+?\s+years?)\b",
    re.IGNORECASE,
)

_MID_YEARS_HINT = re.compile(
    r"\b([3-5][-–]?[5]?\+?\s+years?|3\+?\s+years?|4\+?\s+years?|5\+?\s+years?)\b",
    re.IGNORECASE,
)

_SENIOR_YEARS_HINT = re.compile(
    r"\b([6-9]\+?\s+years?|1[0-9]\+?\s+years?|7\+?\s+years?|8\+?\s+years?|10\+?\s+years?)\b",
    re.IGNORECASE,
)


def _coerce_experience_cell(raw) -> str | None:
    """Return canonical level or None if unknown / missing."""
    if pd.isna(raw):
        return None
    s = str(raw).strip().lower()
    if not s or s in ("nan", "none", "n/a", "na", "-", "--"):
        return None
    if s in _EXPERIENCE_LEVEL_MAPPING:
        return _EXPERIENCE_LEVEL_MAPPING[s]
    if _ENTRY_IN_TEXT.search(s):
        return "Entry"
    if _MID_IN_TEXT.search(s):
        return "Mid"
    if _SENIOR_IN_TEXT.search(s):
        return "Senior"
    return None


def infer_experience_level_from_title(title) -> str | None:
    """Infer Entry / Mid / Senior from job title when API experience_level is empty."""
    if pd.isna(title):
        return None
    t = str(title).strip()
    if not t:
        return None
    if _ENTRY_IN_TEXT.search(t):
        return "Entry"
    if _MID_IN_TEXT.search(t):
        return "Mid"
    if _SENIOR_IN_TEXT.search(t):
        return "Senior"
    if _ARCHITECT_OR_SIMILAR_ROLE.search(t):
        return "Senior"
    return None


def normalize_experience_level(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Step 1: normalize existing values
    if "experience_level" in df.columns:
        coerced = df["experience_level"].astype("string").apply(_coerce_experience_cell)
        df["experience_level"] = coerced.where(coerced.notna(), df["experience_level"])

    # Step 2: infer from title
    if "job_title" in df.columns:
        current = df["experience_level"].astype("string")
        missing_mask = current.isna() | (current.str.strip() == "")

        inferred_from_title = df["job_title"].apply(infer_experience_level_from_title)

        fill_mask = missing_mask & inferred_from_title.notna()
        df.loc[fill_mask, "experience_level"] = inferred_from_title[fill_mask]

    # Step 3: infer from description
    if "description" in df.columns:
        current = df["experience_level"].astype("string")
        missing_mask = current.isna() | (current.str.strip() == "")

        inferred_from_desc = df["description"].apply(infer_experience_level_from_description)

        fill_mask = missing_mask & inferred_from_desc.notna()
        df.loc[fill_mask, "experience_level"] = inferred_from_desc[fill_mask]

    # -----------------------------
    # Step 4: hierarchical fallback
    # -----------------------------

    current = df["experience_level"].astype("string")
    missing_mask = current.isna() | (current.str.strip() == "")

    if missing_mask.any():

        # Level 1: job_title + country
        mode_job_country = (
            df.groupby(["job_title", "country"])["experience_level"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        )

        # Level 2: job_title
        mode_job = (
            df.groupby("job_title")["experience_level"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        )

        # Level 3: country
        mode_country = (
            df.groupby("country")["experience_level"]
            .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else None)
        )

        # Level 4: global
        global_mode = df["experience_level"].mode()
        global_mode = global_mode.iloc[0] if not global_mode.empty else None

        def fill_row(row):
            if pd.notna(row["experience_level"]) and str(row["experience_level"]).strip() != "":
                return row["experience_level"]

            jt = row["job_title"]
            c = row["country"]

            if (jt, c) in mode_job_country and pd.notna(mode_job_country.get((jt, c))):
                return mode_job_country[(jt, c)]

            if jt in mode_job and pd.notna(mode_job.get(jt)):
                return mode_job[jt]

            if c in mode_country and pd.notna(mode_country.get(c)):
                return mode_country[c]

            return global_mode

        df.loc[missing_mask, "experience_level"] = df.loc[missing_mask].apply(fill_row, axis=1)

    return df

def infer_experience_level_from_description(description) -> str | None:
    if pd.isna(description):
        return None

    d = str(description).strip()
    if not d:
        return None

    # strongest direct keyword signals first
    if _ENTRY_IN_TEXT.search(d):
        return "Entry"
    if _SENIOR_IN_TEXT.search(d):
        return "Senior"
    if _MID_IN_TEXT.search(d):
        return "Mid"

    # then years-of-experience hints
    if _SENIOR_YEARS_HINT.search(d):
        return "Senior"
    if _MID_YEARS_HINT.search(d):
        return "Mid"
    if _ENTRY_YEARS_HINT.search(d):
        return "Entry"

    return None


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