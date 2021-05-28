import pandas as pd
import json
from airflow.decorators import task

@task()
def transform(items :list):
    artists_data, content_data, genres_data, labels_data, reviews_data, years_data = items
    artists_data = pd.DataFrame(artists_data)
    content_data = pd.DataFrame(content_data)
    genres_data = pd.DataFrame(genres_data)
    labels_data = pd.DataFrame(labels_data)
    reviews_data = pd.DataFrame(reviews_data)
    years_data = pd.DataFrame(years_data)
    df = artists_data.merge(content_data, on='reviewid', how='left').merge(genres_data, on='reviewid', how='left').merge(labels_data, on='reviewid', how='left').merge(reviews_data, on='reviewid', how='left').merge(years_data, on='reviewid', how='left')
    
    df = df.to_json(orient="records")
    df = json.loads(df)
    print(df)

    return {"database_data": df}
