CSV_MAPPING = {
    "job_id": "job_id",
    "job_title": "job_title",
    "country": "country",
    "city": "city",
    "remote_type": "remote_type",
    "experience_level": "experience_level",
    "salary_min_usd": "salary_min_usd",
    "salary_max_usd": "salary_max_usd",
    "employment_type": "employment_type",
    "posted_year": "posted_year",
}

AADZUNA_MAPPING = {
    "job_id": "id",
    "job_title": "title",
    "country": "location.area[0]",
    "city": "location.area[-1]",
    "remote_type": None,
    "experience_level": None,
    "salary_min_usd": "salary_min",
    "salary_max_usd": "salary_max",
    "employment_type": "contract_time",
    "posted_year": "created",
}