from __future__ import annotations

from datetime import datetime
from typing import Any
import os

import requests
from dotenv import load_dotenv

from app.utils.config import TARGET_ADZUNA_COUNTRIES, TARGET_JOB_TITLES

load_dotenv()

BASE_URL = "https://api.adzuna.com/v1/api"


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
    results_per_page: int = 20,
    sort_by: str = "date",
) -> dict[str, Any]:
    return {
        "app_id": app_id,
        "app_key": app_key,
        "what": what,
        "category": "it-jobs",
        "results_per_page": results_per_page,
        "sort_by": sort_by,
        "content-type": "application/json",
    }


def fetch_adzuna_jobs(
    country: str,
    what: str,
    page: int = 1,
    results_per_page: int = 20,
) -> list[dict[str, Any]]:
    app_id, app_key = get_adzuna_credentials()
    url = build_adzuna_search_url(country=country, page=page)
    params = build_adzuna_params(
        app_id=app_id,
        app_key=app_key,
        what=what,
        results_per_page=results_per_page,
    )

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    return data.get("results", [])


def fetch_target_jobs(
    page: int = 1,
    results_per_page: int = 20,
) -> list[dict[str, Any]]:
    all_jobs: list[dict[str, Any]] = []

    for country_name, country_code in TARGET_ADZUNA_COUNTRIES.items():
        for job_title in TARGET_JOB_TITLES:
            try:
                jobs = fetch_adzuna_jobs(
                    country=country_code,
                    what=job_title,
                    page=page,
                    results_per_page=results_per_page,
                )
                print(
                    f"Fetched {len(jobs)} jobs | country={country_name} ({country_code}) | query='{job_title}'"
                )
                all_jobs.extend(jobs)
            except Exception as e:
                print(
                    f"Error fetching jobs | country={country_name} ({country_code}) | query='{job_title}' | error={e}"
                )

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
        "onsite" in title_lower
        or "on-site" in title_lower
        or "on site" in title_lower
        or "onsite" in description_lower
        or "on-site" in description_lower
        or "on site" in description_lower
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
        "country": country,
        "city": city,
        "remote_type": remote_type,
        "experience_level": experience_level,
        "salary_min_usd": job.get("salary_min"),
        "salary_max_usd": job.get("salary_max"),
        "employment_type": employment_type,
        "posted_year": posted_year,
    }


if __name__ == "__main__":
    jobs = fetch_target_jobs(page=1, results_per_page=10)
    print(f"\nTotal jobs fetched: {len(jobs)}")

    if jobs:
        mapped = map_adzuna_job_to_schema(jobs[0])
        print("\nFirst mapped job:")
        print(mapped)