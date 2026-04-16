FROM python:3.11-slim

WORKDIR /app

# system dependencies (needed for dbt and postgres connections)
RUN apt-get update && apt-get install -y libpq-dev gcc \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV DBT_PROFILES_DIR=/app/job_tracker_dbt

RUN chmod +x run_pipeline.sh

CMD ["./run_pipeline.sh"]
