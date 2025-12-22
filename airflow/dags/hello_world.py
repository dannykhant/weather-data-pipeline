from airflow.decorators import dag, task  # type: ignore
from datetime import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule="30 23 * * *",
    catchup=False,
)
def hello_world():

    @task()
    def hello(**context):
        print("hello world")

    hello()

hello_world()
