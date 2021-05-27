import json
import pandas as pd
from airflow.decorators import task

@task
def transform(df :list):
    df = pd.json_normalize(df)
    df = df.applymap(str)
    df = df.to_json(orient="records")
    df = json.loads(df)
    return {"tweet_data": df}
