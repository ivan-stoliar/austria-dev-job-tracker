import os
import json
import logging
import psycopg2
import glob
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_db_connection():

    """Establishes a connection to the PostgreSQL database."""

    try:
        conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"]
        )
        return conn
    except Exception as e:
        logging.error(f"FATAL: Could not connect to the database. Error: {e}")
        raise

def setup_bronze_table(conn):

    """Creates the schema and raw ingestion table."""

    query = """
    CREATE SCHEMA IF NOT EXISTS bronze;

    CREATE TABLE IF NOT EXISTS bronze.raw_jobs (
        id SERIAL PRIMARY KEY,
        job_id VARCHAR NOT NULL,
        source VARCHAR NOT NULL,
        keyword VARCHAR,
        country VARCHAR,
        extracted_at TIMESTAMP WITH TIME ZONE,
        raw_data JSONB NOT NULL,
        ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (job_id, source)
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    logging.info("Verified 'bronze.raw_jobs' table exists in the database.")

def extract_todays_payloads():
    """Reads ONLY today's JSON files from the partitioned Data Lake."""

    script_dir = os.path.dirname(os.path.abspath(__file__))

    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")

    search_pattern = os.path.abspath(os.path.join(
        script_dir, "..", "data", "raw", "**", year, month, day, "*.json"
    ))

    todays_files = glob.glob(search_pattern, recursive=True)

    if not todays_files:
        logging.warning("No JSON files found for today's date. Nothing to load.")
        return []

    records_to_insert = []

    for file_path in todays_files:
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                wrapped_data = json.load(f)

                source = wrapped_data.get("source")
                keyword = wrapped_data.get("keyword")
                country = wrapped_data.get("country")
                extracted_at = wrapped_data.get("extracted_at")
                jobs_list = wrapped_data.get("jobs", [])

                for job in jobs_list:

                    job_id = job.get("id") or job.get("slug")

                    if not job_id:
                        logging.warning(f"Skipping job with no ID from source: {source}")
                        continue

                    raw_data_json = json.dumps(job)

                    records_to_insert.append((
                        str(job_id), source, keyword, country, extracted_at, raw_data_json
                    ))

            except json.JSONDecodeError:
                logging.error(f"Failed to parse JSON file: {file_path}")

    return records_to_insert

def load_to_postgres(conn, records):
    """Bulk inserts records using DO NOTHING to preserve Bronze immutability."""
    if not records:
        logging.info("No records to load into PostgreSQL.")
        return


    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bronze.raw_jobs;")
        count_before = cur.fetchone()[0]


    insert_query = """
        INSERT INTO bronze.raw_jobs (job_id, source, keyword, country, extracted_at, raw_data)
        VALUES %s
        ON CONFLICT (job_id, source) DO NOTHING;
    """

    try:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, records, page_size=1000)
        conn.commit()


        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.raw_jobs;")
            count_after = cur.fetchone()[0]

        new_records = count_after - count_before
        skipped_records = len(records) - new_records

        logging.info(f"Load complete. New records inserted: {new_records}. Skipped (already existed): {skipped_records}.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to load data to PostgreSQL. Error: {e}")

if __name__ == "__main__":
    logging.info("Starting Bronze Data Loading Pipeline...")

    db_conn = get_db_connection()

    try:
        setup_bronze_table(db_conn)

        jobs_data = extract_todays_payloads()
        logging.info(f"Found {len(jobs_data)} valid jobs in today's local JSON files.")

        load_to_postgres(db_conn, jobs_data)

    finally:
        db_conn.close()
        logging.info("Pipeline finished. Database connection closed.")
