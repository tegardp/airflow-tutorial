
from airflow.decorators import task

@task()
def transform(df: list):
    return {"file_1000": df}