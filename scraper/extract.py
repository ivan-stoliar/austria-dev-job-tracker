import requests
from dotenv import load_dotenv
import os
import pandas as pd
import json
import logging
from datetime import datetime
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
    )

load_dotenv()
adzuna_id_secret = os.environ.get("ADZUNA_ID")
adzuna_api_secret = os.environ.get("ADZUNA_API")

def save_raw_json(data, source_name):
    """
    Saves a JSON payload to the correct local folder based on the source name.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))

    folder_path = os.path.abspath(os.path.join(script_dir, "..", "data", "raw", source_name))

    os.makedirs(folder_path, exist_ok=True)

    today = datetime.now().strftime("%Y-%m-%d")
    file_path = os.path.join(folder_path, f"{today}.json")

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    logging.info(f"Successfully saved {len(data)} records to {file_path}")

def extract_job_adzuna():
    """
    Extracts job data from the Adzuna API.
    """

    logging.info("Starting Adzuna extraction...")
    job_keywords = ["Data Engineer", "Data", "Backend", "Cloud", "Devops"]
    MAX_PAGES = 10

    all_adzuna_jobs = []

    for role in job_keywords:
        page = 1
        while page <= MAX_PAGES:
            logging.info(f"Fetching {role} - Page {page}...")

            payload = {
                "app_id": adzuna_id_secret,
                "app_key": adzuna_api_secret,
                "results_per_page": 50,
                "what": role
            }

            url = f'https://api.adzuna.com/v1/api/jobs/at/search/{page}'
            r = requests.get(url, params=payload)


            if r.status_code != 200:
                logging.error(f"API crashed on {role} page {page}. Code: {r.status_code}")
                logging.error(f"Adzuna Error Message: {r.text}")
                break

            data = r.json().get('results', [])

            if not data:
                logging.info(f"End of jobs for {role} reached at page {page}.")
                break

            all_adzuna_jobs.extend(data)

            logging.info(f"Accumulated {len(all_adzuna_jobs)} total jobs so far...")

            page += 1

            time.sleep(5)

    if all_adzuna_jobs:
        save_raw_json(all_adzuna_jobs, "adzuna")
        logging.info(f"Extraction complete! Saved {len(all_adzuna_jobs)} total jobs to today's file.")
    else:
        logging.warning("No jobs were found today across any keywords.")



def extract_job_jooble():

    pass

def extract_job_arbeitnow():

    """
    Extracts job data from the Arbeitnow API.
    """

    logging.info("Starting Arbeitnow extraction...")
    url = "https://www.arbeitnow.com/api/job-board-api"
    r = requests.get(url)

    if r.status_code == 200:
        data = r.json().get('data', [])
        save_raw_json(data, "arbeitnow")
    else:
        logging.error(f"Arbeitnow API failed with status code: {r.status_code}")


# def extract_jobs():

    # job_description = []
    # lowered_job_description = []
    # job_description.append(data["results"][0]["description"])

    # for word in job_description:
    #     lowered_job_description.append(word.lower())

    # print(lowered_job_description)

    # target_skills = ["python", "sql", "aws", "azure", "kafka", "java", "spark", "austria", "wien", "wir", "vollzeit", "teilzeit", "österreich"]
    # found_skills = []
    # for skill in target_skills:
    #     if skill in str(lowered_job_description):
    #         found_skills.append(skill)

    # print(lowered_job_description)
    # print(lowered_job_description[0])
    # print(found_skills)

    # titles = []
    # companies = []
    # job_descriptions = []
    # locations = []
    # dates = []



    # for job in data["results"]:
    #     titles.append(job["title"])
    #     companies.append(job["company"]["display_name"])


    # # print(titles)
    # # print(companies)

    # dataset = {
    #     "job_title":titles,
    #     "company_name": companies
    # }

    # df = pd.DataFrame(dataset)
    # print(df)


if __name__ == "__main__":
    logging.info("Starting Daily Data Extraction Pipeline")

    extract_job_arbeitnow()
    extract_job_adzuna()
    # extract_jooble()

    logging.info("Pipeline Finished")
