from __future__ import annotations

from datetime import datetime
from typing import Any
import os

import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.adzuna.com/v1/api"

TARGET_COUNTRIES = {
    "Germany": "de",
    "Canada": "ca",
    "USA": "us",
    "UK": "gb",
}

TARGET_ROLE_QUERIES = [
    "MLOps Engineer",
    "Data Analyst",
    "Applied Scientist",
    "Machine Learning Engineer",
    "AI Researcher",
    "Data Scientist",
]

FALLBACK_QUERIES = {
    "MLOps Engineer": ["Machine Learning","MLOps", "Data Engineer", "Data"],
    "Data Analyst": ["Analytics", "Business Intelligence", "Data"],
    "Applied Scientist": ["Machine Learning", "AI", "Data Science"],
    "Machine Learning Engineer": ["Machine Learning", "ML Engineer", "AI"],
    "AI Researcher": ["Artificial Intelligence", "Machine Learning", "AI"],
    "Data Scientist": ["Data Science", "Machine Learning", "Data"],
}

COUNTRY_TARGETS = {
    "de": 150,
    "ca": 150,
    "us": 200,
    "gb": 200,
}


def get_adzuna_credentials() -> tuple[str, str]:
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    if not app_id or not app_key:
        raise ValueError("Missing ADZUNA_APP_ID or ADZUNA_APP_KEY in environment variables.")
    return app_id, app_key


def build_adzuna_search_url(country: str, page: int = 1) -> str:
    return f"{BASE_URL}/jobs/{country}/search/{page}"


def build_adzuna_params(
    app_id: str,
    app_key: str,
    what: str,
    results_per_page: int = 25,
    sort_by: str = "date",
    use_category: bool = True,
) -> dict[str, Any]:
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": what,
        "results_per_page": results_per_page,
        "sort_by": sort_by,
        "content-type": "application/json",
    }
    if use_category:
        params["category"] = "it-jobs"
    return params


def fetch_adzuna_jobs(
    country: str,
    what: str,
    page: int = 1,
    results_per_page: int = 25,
    use_category: bool = True,
) -> list[dict[str, Any]]:
    app_id, app_key = get_adzuna_credentials()
    url = build_adzuna_search_url(country=country, page=page)
    params = build_adzuna_params(
        app_id=app_id,
        app_key=app_key,
        what=what,
        results_per_page=results_per_page,
        use_category=use_category,
    )

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    return data.get("results", [])
def fetch_page_window_for_query(
    country: str,
    what: str,
    start_page: int = 1,
    end_page: int = 10,
    results_per_page: int = 25,
    use_category: bool = True,
) -> list[dict[str, Any]]:
    collected = []
    seen_ids = set()

    for page in range(start_page, end_page + 1):
        jobs = fetch_adzuna_jobs(
            country=country,
            what=what,
            page=page,
            results_per_page=results_per_page,
            use_category=use_category,
        )

        print(f"country={country} query='{what}' page={page} fetched={len(jobs)}")

        if not jobs:
            break

        new_count = 0
        for job in jobs:
            job_id = str(job.get("id")) if job.get("id") is not None else None
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                collected.append(job)
                new_count += 1

        if new_count == 0:
            break

    return collected


def fetch_all_pages_for_query(
    country: str,
    what: str,
    results_per_page: int = 25,
    max_pages: int = 10,
    use_category: bool = True,
) -> list[dict[str, Any]]:
    collected = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        jobs = fetch_adzuna_jobs(
            country=country,
            what=what,
            page=page,
            results_per_page=results_per_page,
            use_category=use_category,
        )

        print(f"country={country} query='{what}' page={page} fetched={len(jobs)}")

        if not jobs:
            break

        new_count = 0
        for job in jobs:
            job_id = str(job.get("id")) if job.get("id") is not None else None
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                collected.append(job)
                new_count += 1

        if new_count == 0:
            break

    return collected


def fetch_country_job_pool(
    country: str,
    target_rows: int,
    results_per_page: int = 25,
    start_page: int = 1,
    end_page: int = 10,
    use_fallbacks: bool = True,
) -> list[dict[str, Any]]:
    pool = []
    seen_ids = set()

    for role in TARGET_ROLE_QUERIES:
        role_jobs = fetch_page_window_for_query(
            country=country,
            what=role,
            start_page=start_page,
            end_page=end_page,
            results_per_page=results_per_page,
            use_category=True,
        )

        for job in role_jobs:
            job_id = str(job.get("id")) if job.get("id") is not None else None
            if job_id and job_id not in seen_ids:
                seen_ids.add(job_id)
                pool.append(job)

        if len(pool) >= target_rows:
            return pool[:target_rows]

        if use_fallbacks:
            for fallback in FALLBACK_QUERIES.get(role, []):
                fallback_jobs = fetch_page_window_for_query(
                    country=country,
                    what=fallback,
                    start_page=start_page,
                    end_page=end_page,
                    results_per_page=results_per_page,
                    use_category=False,
                )

                for job in fallback_jobs:
                    job_id = str(job.get("id")) if job.get("id") is not None else None
                    if job_id and job_id not in seen_ids:
                        seen_ids.add(job_id)
                        pool.append(job)

                if len(pool) >= target_rows:
                    return pool[:target_rows]

    return pool


def fetch_target_jobs() -> list[dict[str, Any]]:
    all_jobs = []

    for country_name, country_code in TARGET_COUNTRIES.items():
        target_rows = COUNTRY_TARGETS[country_code]
        country_jobs = fetch_country_job_pool(
    country=country_code,
    target_rows=target_rows,
    results_per_page=25,
    start_page=4,
    end_page=12,
    use_fallbacks=True,
)
        print(f"{country_name} ({country_code}) final collected={len(country_jobs)}")
        all_jobs.extend(country_jobs)

    return all_jobs




def fetch_target_jobs_recent_batch() -> list[dict[str, Any]]:
    all_jobs = []

    for country_name, country_code in TARGET_COUNTRIES.items():
        target_rows = COUNTRY_TARGETS[country_code]

        country_jobs = fetch_country_job_pool(
            country=country_code,
            target_rows=target_rows,
            results_per_page=25,
            start_page=1,
            end_page=5,
            use_fallbacks=True,
        )

        print(f"{country_name} ({country_code}) recent batch collected={len(country_jobs)}")
        all_jobs.extend(country_jobs)

    return all_jobs


def fetch_target_jobs_deep_batch() -> list[dict[str, Any]]:
    all_jobs = []

    for country_name, country_code in TARGET_COUNTRIES.items():
        target_rows = COUNTRY_TARGETS[country_code] * 2

        country_jobs = fetch_country_job_pool(
            country=country_code,
            target_rows=target_rows,
            results_per_page=25,
            start_page=6,
            end_page=20,
            use_fallbacks=True,
        )

        print(f"{country_name} ({country_code}) deep batch collected={len(country_jobs)}")
        all_jobs.extend(country_jobs)

    return all_jobs

def fetch_target_jobs_stream() -> list[dict[str, Any]]:
    all_jobs = []

    stream_country_targets = {
        "de": 40,
        "ca": 40,
        "us": 50,
        "gb": 50,
    }

    for country_name, country_code in TARGET_COUNTRIES.items():
        target_rows = stream_country_targets[country_code]

        country_jobs = fetch_country_job_pool(
            country=country_code,
            target_rows=target_rows,
            results_per_page=25,
            start_page=1,
            end_page=2,
            use_fallbacks=False,
        )

        print(f"{country_name} ({country_code}) stream collected={len(country_jobs)}")
        all_jobs.extend(country_jobs)

    return all_jobs


def map_adzuna_job_to_schema(job: dict[str, Any]) -> dict[str, Any]:
    location = job.get("location") or {}
    area = location.get("area", [])

    title = (job.get("title") or "").strip()
    description = (job.get("description") or "").strip()

    title_lower = title.lower()
    description_lower = description.lower()

    country = area[0] if len(area) > 0 else None
    city = area[-1] if len(area) > 1 else None

    remote_type = None
    if "remote" in title_lower or "remote" in description_lower:
        remote_type = "Remote"
    elif "hybrid" in title_lower or "hybrid" in description_lower:
        remote_type = "Hybrid"
    elif (
        "onsite" in title_lower or "on-site" in title_lower or "on site" in title_lower
        or "onsite" in description_lower or "on-site" in description_lower or "on site" in description_lower
    ):
        remote_type = "Onsite"

    experience_level = None
    if any(word in title_lower or word in description_lower for word in ["senior", "sr.", "lead", "principal"]):
        experience_level = "Senior"
    elif any(word in title_lower or word in description_lower for word in ["junior", "jr.", "entry", "graduate", "intern"]):
        experience_level = "Entry"
    elif any(word in title_lower or word in description_lower for word in ["mid", "intermediate"]):
        experience_level = "Mid"

    created = job.get("created")
    posted_year = None
    if created:
        posted_year = datetime.fromisoformat(created.replace("Z", "+00:00")).year

    contract_time = job.get("contract_time")
    contract_type = job.get("contract_type")

    employment_type = None
    if contract_time == "full_time":
        employment_type = "Full-time"
    elif contract_time == "part_time":
        employment_type = "Part-time"
    elif contract_type == "contract":
        employment_type = "Contract"
    elif contract_type == "permanent":
        employment_type = "Full-time"

    return {
        "job_id": str(job.get("id")) if job.get("id") is not None else None,
        "job_title": job.get("title"),
        "description": job.get("description"),
        "country": country,
        "city": city,
        "remote_type": remote_type,
        "experience_level": experience_level,
        "salary_min_usd": job.get("salary_min"),
        "salary_max_usd": job.get("salary_max"),
        "employment_type": employment_type,
        "posted_year": posted_year,
        "job_description": job.get("description"),
    }


if __name__ == "__main__":
    jobs = fetch_target_jobs_stream()
    print(f"Total jobs fetched: {len(jobs)}")