import json
import pandas as pd
from airflow.decorators import task

@task
def transform(items :list):
    new_item = []
    for item in items:
        new_item += item
    df = pd.json_normalize(new_item)
    df = df.drop_duplicates(subset=['id'])
    df = df.to_json(orient="records")
    df = json.loads(df)
    return {"reviews": df}