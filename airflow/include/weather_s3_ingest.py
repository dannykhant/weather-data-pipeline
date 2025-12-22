import json
import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3

from airflow.models import Variable
from airflow.hooks.base import BaseHook # type: ignore


# -----------------------
# Configuration
# -----------------------

WEATHER_API_URL = "https://api.weatherapi.com/v1/history.json"


@dataclass(frozen=True)
class WeatherJobConfig:
    location: str
    s3_bucket: str
    s3_prefix: str = "weather/raw"
    request_timeout: int = 10
    max_retries: int = 3
    aws_conn_id: str = "aws-s3"
    weather_api_var: str = "weather_api_key"


# -----------------------
# Logging
# -----------------------

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)


# -----------------------
# HTTP Client
# -----------------------

def _build_http_session(max_retries: int) -> requests.Session:
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


# -----------------------
# AWS / Airflow helpers
# -----------------------

def _build_s3_client(aws_conn_id: str):
    """
    Build boto3 client using Airflow Connection.
    """
    conn = BaseHook.get_connection(aws_conn_id)

    session = boto3.Session(
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=conn.extra_dejson.get("region_name", "us-east-2"),
    )

    return session.client("s3")


def _get_weather_api_key(var_name: str) -> str:
    """
    Read API key from Airflow Variables.
    """
    api_key = Variable.get(var_name, default_var=None)
    if not api_key:
        raise RuntimeError(f"Airflow Variable '{var_name}' is not set")
    return api_key


# -----------------------
# Core Logic
# -----------------------

def fetch_weather(
    session: requests.Session,
    api_key: str,
    location: str,
    timeout: int,
    target_date: str
) -> Dict[str, Any]:
    logger.info("Fetching weather data for location=%s", location)

    response = session.get(
        WEATHER_API_URL,
        params={
            "key": api_key,
            "q": location,
            "aqi": "no",
            "dt": target_date,
        },
        timeout=timeout,
    )

    response.raise_for_status()
    return response.json()


def upload_to_s3(
    s3_client,
    data: Dict[str, Any],
    bucket: str,
    prefix: str,
    target_date: str,
) -> str:
    s3_key = (
        f"{prefix}/"
        f"date={target_date}/"
        f"weather_{target_date}.json"
    )

    logger.info("Uploading weather data to s3://%s/%s", bucket, s3_key)

    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(data),
        ContentType="application/json",
    )

    return s3_key


# -----------------------
# Airflow Entry Point
# -----------------------

def run_weather_extraction(
    location: str,
    target_date: str,
    s3_bucket: str,
    s3_prefix: str = "weather/raw",
):
    """
    Airflow-safe entrypoint.
    """

    config = WeatherJobConfig(
        location=location,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
    )

    api_key = _get_weather_api_key(config.weather_api_var)
    s3_client = _build_s3_client(config.aws_conn_id)

    session = _build_http_session(config.max_retries)

    weather_data = fetch_weather(
        session=session,
        api_key=api_key,
        location=config.location,
        timeout=config.request_timeout,
        target_date=target_date,
    )

    s3_key = upload_to_s3(
        s3_client=s3_client,
        data=weather_data,
        bucket=config.s3_bucket,
        prefix=config.s3_prefix,
        target_date=target_date,
    )

    logger.info("Weather extraction completed successfully")
    logger.info("S3 object created: s3://%s/%s", config.s3_bucket, s3_key)

    return {
        "s3_bucket": config.s3_bucket,
        "s3_key": s3_key,
        "location": config.location,
    }

if __name__ == "__main__":
    # For local testing purposes
    run_weather_extraction(
        location="Bangkok",
        s3_bucket="your-s3-bucket-name",
        target_date="2023-01-01",
    )
