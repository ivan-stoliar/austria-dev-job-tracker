import requests
from dotenv import load_dotenv
import os
import json
import logging
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
    )

load_dotenv()
adzuna_id_secret = os.environ["ADZUNA_ID"]
adzuna_api_secret = os.environ["ADZUNA_API"]
jooble_api_secret = os.environ["JOOBLE_API"]

@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.RequestException)
)

def fetch_data_with_retry(session, url, payload=None, is_post=False):
    """
    A network request function that automatically retries on failure.
    """

    if is_post:
        response = session.post(url, json=payload, timeout=10)
    else:
        response = session.get(url, params=payload, timeout=10)

    response.raise_for_status()

    return response

def save_raw_json(wrapped_data, source_name, country, keyword):
    """
    Saves a JSON payload using Data Lake partitioning:
    data/raw/source/country/YYYY/MM/DD/keyword_jobs.json
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    folder_path = os.path.abspath(os.path.join(
        script_dir, "..", "data", "raw", source_name, country, year, month, day
    ))

    os.makedirs(folder_path, exist_ok=True)

    safe_keyword = keyword.lower().replace(" ", "_")
    file_path = os.path.join(folder_path, f"{safe_keyword}_jobs.json")

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(wrapped_data, file, indent=4, ensure_ascii=False)

    logging.info(f"Successfully saved partitioned file to: {file_path}")


def extract_job_adzuna():
    """
    Extracts job data from the Adzuna API.
    """

    logging.info("Starting Adzuna extraction...")
    job_keywords = ["Data Engineer", "Data", "Backend", "Cloud", "Devops"]
    target_country = "austria"
    MAX_PAGES = 10

    seen_adzuna_ids = set()

    with requests.Session() as session:
        for role in job_keywords:
            page = 1
            keyword_jobs = []

            while page <= MAX_PAGES:
                logging.info(f"Fetching {role} - Page {page}...")

                payload = {
                    "app_id": adzuna_id_secret,
                    "app_key": adzuna_api_secret,
                    "results_per_page": 50,
                    "what": role
                }

                url = f'https://api.adzuna.com/v1/api/jobs/at/search/{page}'

                try:
                    r = fetch_data_with_retry(session, url, payload=payload, is_post=False)

                except requests.exceptions.RequestException as e:
                    logging.error(f"FATAL: Adzuna API totally failed on {role} page {page} after 4 attempts.")
                    logging.error(f"Error details: {e}")
                    break

                data = r.json().get('results', [])

                if not data:
                    logging.info(f"End of jobs for {role} reached at page {page}.")
                    break

                duplicates_prevented = 0
                for job in data:

                    job_id = str(job.get("id"))

                    if job_id and job_id not in seen_adzuna_ids:
                        seen_adzuna_ids.add(job_id)
                        keyword_jobs.append(job)
                    else:
                        duplicates_prevented += 1

                if duplicates_prevented > 0:
                    logging.info(f"Skipped {duplicates_prevented} duplicate jobs on this page.")

                logging.info(f"Accumulated {len(keyword_jobs)} total {role} jobs so far...")

                page += 1

                time.sleep(5)

            if keyword_jobs:

                wrapped_payload = {
                    "extracted_at": datetime.now().isoformat(),
                    "source": "adzuna",
                    "country": target_country,
                    "keyword": role,
                    "total_records": len(keyword_jobs),
                    "jobs": keyword_jobs
                }

                save_raw_json(wrapped_payload, "adzuna", target_country, role)
                logging.info(f"Extraction complete! Saved {len(keyword_jobs)} jobs for {role} to the file.")
            else:
                logging.warning("No jobs were found today across any keywords.")

    logging.info(f"Adzuna pipeline finished. Saved {len(seen_adzuna_ids)} Adzuna unique jobs today.")


def extract_job_jooble():
    """
    Extracts job data from the Jooble API.
    """
    logging.info("Starting Jooble extraction...")

    url = f"https://jooble.org/api/{jooble_api_secret}"

    job_keywords = ["Data Engineer", "Data", "Backend", "Cloud", "Devops"]
    target_country = "austria"
    MAX_PAGES = 10

    seen_jooble_ids = set()


    with requests.Session() as session:
        for role in job_keywords:
            page = 1
            keyword_jobs = []
            while page <= MAX_PAGES:
                logging.info(f"Fetching Jooble: {role} - Page {page}...")

                payload = {
                    "keywords": role,
                    "location": "Austria",
                    "page": str(page)
                }

                try:
                    r = fetch_data_with_retry(session, url, payload=payload, is_post=True)

                except requests.exceptions.RequestException as e:
                    logging.error(f"FATAL: Jooble API totally failed on {role} page {page} after 4 attempts.")
                    logging.error(f"Error details: {e}")
                    break

                data = r.json().get('jobs', [])

                if not data:
                    logging.info(f"End of Jooble jobs for {role} reached at page {page}.")
                    break
                duplicates_prevented = 0

                for job in data:

                    job_id = str(job.get("id"))

                    if job_id and job_id not in seen_jooble_ids:
                        seen_jooble_ids.add(job_id)
                        keyword_jobs.append(job)
                    else:
                        duplicates_prevented += 1

                if duplicates_prevented > 0:
                    logging.info(f"Skipped {duplicates_prevented} duplicate jobs on this page.")

                logging.info(f"Accumulated {len(keyword_jobs)} total {role} jobs so far...")

                page += 1

                time.sleep(5)

        if keyword_jobs:

                wrapped_payload = {
                    "extracted_at": datetime.now().isoformat(),
                    "source": "jooble",
                    "country": target_country,
                    "keyword": role,
                    "total_records": len(keyword_jobs),
                    "jobs": keyword_jobs
                }

                save_raw_json(wrapped_payload, "jooble", target_country, role)
                logging.info(f"Extraction complete! Saved {len(keyword_jobs)} jobs for {role} to the file.")
        else:
                logging.warning("No jobs were found today across any keywords.")

    logging.info(f"Jooble pipeline finished. Saved {len(seen_jooble_ids)} Jooble unique jobs today.")

def extract_job_arbeitnow():

    """
    Extracts job data from the Arbeitnow API.
    """

    logging.info("Starting Arbeitnow extraction...")
    url = "https://www.arbeitnow.com/api/job-board-api"
    r = requests.get(url, timeout=10)

    if r.status_code == 200:
        data = r.json().get('data', [])
        save_raw_json(data, "arbeitnow")
    else:
        logging.error(f"Arbeitnow API failed with status code: {r.status_code}")


if __name__ == "__main__":
    logging.info("Starting Daily Data Extraction Pipeline")

    # extract_job_arbeitnow()
    extract_job_adzuna()
    # extract_job_jooble()

    logging.info("Pipeline Finished")
