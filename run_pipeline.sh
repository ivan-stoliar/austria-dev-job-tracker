#!/bin/bash

set -e

echo "Starting Job Extraction..."
python scraper/extract.py

echo "Loading Raw Data into Postgres (Bronze Layer)..."
python scraper/load.py

echo "Entering dbt project folder..."
cd /app/job_tracker_dbt

echo "Installing dbt dependencies..."
dbt deps

echo "Seeding static taxonomy mapping tables..."
dbt seed

echo "Building Data Warehouse (Bronze -> Silver -> Gold)..."
dbt run

echo "Pipeline Complete!"
