from airflow.decorators import task

@task()
def transform(df: list):
    return {"disaster": df}