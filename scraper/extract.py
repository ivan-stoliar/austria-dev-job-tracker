import requests
from dotenv import load_dotenv
import os
import pandas as pd
import json

load_dotenv()

def extract_job_adzuna():

    pass

def extract_job_jooble():

    pass

def extract_job_arbeitnow():

    base_url = 'https://arbeitnow.com/api/job-board-api'
    r = requests.get(base_url)
    data = r.json()
    jobs_list = data['data']

    with open("arbeitnow_raw.json", "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

def extract_jobs():



    job_keyword = "Data Engineer"
    job_keywords = ["Data Enginner", "Data Analyst", "Backend Developer"]
    results_per_page = 5
    job_location = "Vienna"

    adzuna_id_secret = os.environ.get("ADZUNA_ID")
    adzuna_api_secret = os.environ.get("ADZUNA_API")

    base_url = 'https://api.adzuna.com/v1/api/jobs/at/search/1'

    payload = {
        "app_id": adzuna_id_secret,
        "app_key": adzuna_api_secret,
        "results_per_page": results_per_page,
        "what": job_keyword,
        "where": job_location
    }

    r = requests.get(base_url, params=payload)
    #r = requests.get('https://api.adzuna.com/v1/api/jobs/at/search/1?app_id={0}&app_key={1}&results_per_page={2}&what={3}&where={4}'.format(adzuna_id_secret, adzuna_api_secret, results_per_page,job_keyword,job_location))

    #print(r.text)

    data = r.json()

   # print(data["results"][0])


    job_description = []
    lowered_job_description = []
    job_description.append(data["results"][0]["description"])

    for word in job_description:
        lowered_job_description.append(word.lower())

    # print(lowered_job_description)

    target_skills = ["python", "sql", "aws", "azure", "kafka", "java", "spark", "austria", "wien", "wir", "vollzeit", "teilzeit", "österreich"]
    found_skills = []
    for skill in target_skills:
        if skill in str(lowered_job_description):
            found_skills.append(skill)

    # print(lowered_job_description)
    # print(lowered_job_description[0])
    # print(found_skills)

    titles = []
    companies = []
    job_descriptions = []
    locations = []
    dates = []



    for job in data["results"]:
        titles.append(job["title"])
        companies.append(job["company"]["display_name"])


    # print(titles)
    # print(companies)

    dataset = {
        "job_title":titles,
        "company_name": companies
    }

    df = pd.DataFrame(dataset)
    print(df)


if __name__ == "__main__":
    # extract_jobs()
    extract_job_arbeitnow()
