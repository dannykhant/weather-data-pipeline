from airflow.decorators import dag, task  # type: ignore
from datetime import datetime

from weather_s3_ingest import run_weather_extraction

@dag(
    start_date=datetime(2025, 1, 1),
    schedule="30 23 * * *",
    catchup=False,
)
def weather_ingestion():

    @task()
    def extract_weather_data(**context):
        logical_date = context["logical_date"]
        target_date = logical_date.date()

        run_weather_extraction(
            location="Bangkok",
            target_date=target_date,
            s3_bucket="saanay-data-lake",
            s3_prefix="weather/raw",
        )

    extract_weather_data()

weather_ingestion()
