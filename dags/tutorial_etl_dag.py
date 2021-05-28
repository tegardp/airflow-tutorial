# tutorial_etl_dag.py
# Required library
import json
import os
import openpyxl
import re
import sqlite3
import pandas as pd
from academi import disaster_etl, file_1000_etl, reviews_etl, tweet_etl, database_etl, chinook_etl
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

DATA_PATH = './airflow/data/0. raw'
LOAD_PATH = './airflow/data/3. load'
default_args = {
    'owner': 'yourname',
    'start_date': days_ago(1),
}
default_args = {
    'owner': 'tegardp',
    'start_date': days_ago(1),
}

def get_database_tables(filename):
    conn = sqlite3.connect(f'{DATA_PATH}/{filename}')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    
    fetch_tables = cursor.fetchall()
    tables = [table[0] for table in fetch_tables]
    conn.close()
    return tables

@task
def extract(filename, sheet_name=None, index_col=None, table_name=None):
    file_path = f'{DATA_PATH}/{filename}'
    if 'csv' in filename:
        df = pd.read_csv(file_path)
    elif 'json' in filename:
        df = pd.read_json(f"{DATA_PATH}/tweet_data.json", lines=True)
    elif 'xlsx' in filename:
        df = pd.read_excel(file_path, engine='openpyxl')
    elif 'xls' in filename:
        df = pd.read_excel(file_path, sheet_name=sheet_name, index_col=index_col)
    elif 'sqlite' in filename or 'db' in filename:
        conn = sqlite3.connect(file_path)
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        conn.close()

    df = df.to_json(orient="records")
    df = json.loads(df)
    return df

@task()
def load_data(data :dict):
    conn = sqlite3.connect(f'{LOAD_PATH}/data-warehouse.sqlite', timeout=20)
    for key, df in data.items():
        df = pd.json_normalize(df)
        df.to_sql(key, conn, if_exists="replace", index=False)
    conn.close()

with DAG('tutorial_etl_dag', schedule_interval='@once',default_args=default_args, catchup=False) as dag:
    with TaskGroup('File_1000') as File_1000:
        data = extract('file_1000.xls', sheet_name='Sheet2', index_col=[0])
        transformed_data_file_1000 = file_1000_etl.transform(data)
        file_1000_load = load_data(transformed_data_file_1000)
    with TaskGroup('Disaster') as Disaster:
        data = extract('disaster_data.csv')
        transformed_data_disaster = disaster_etl.transform(data)
        disaster_load = load_data(transformed_data_disaster)
    
    with TaskGroup('Review') as Review:
        data = []
        filenames = os.listdir(DATA_PATH)
        filtered_filenames = list(filter(lambda x: re.match(r"(^review)", x), filenames))
        for filename in filtered_filenames:
            with TaskGroup(filename):
                extract_review = extract(filename)
                data.append(extract_review)
        transformed_data_review = reviews_etl.transform(data)
        reviews_load = load_data(transformed_data_review)

    with TaskGroup('Database') as Database:
        data = []
        tables = get_database_tables('database.sqlite')
        for table in tables:
            with TaskGroup(table):
                extract_database = extract('database.sqlite', table_name = table)
                data.append(extract_database)
        transformed_data_database = database_etl.transform(data)
        database_load = load_data(transformed_data_database)


    with TaskGroup('Chinook') as Chinook:
        data = []
        tables = get_database_tables('chinook.db')
        for table in tables:
            with TaskGroup(table):
                extract_database = extract('chinook.db', table_name = table)
                data.append(extract_database)

        tracks_data = data[12]
        albums_data = data[0]
        media_types_data = data[7]
        genres_data = data[4]
        artists_data = data[1]
        tracks_join_list = [tracks_data,albums_data,media_types_data,genres_data,artists_data]
        transformed_chinook_tracks = chinook_etl.join_tracks(tracks_join_list)
        
        playlist_tracks_data = data[8]
        playlists_data = data[9]
        playlist_tracks_list = [playlist_tracks_data, playlists_data, transformed_chinook_tracks]
        transformed_chinook_playlist_tracks = chinook_etl.join_playlist_tracks(playlist_tracks_list)

        invoices_data = data[6]
        customers_data = data[2]
        employees_data = data[3]
        invoices_list = [invoices_data, customers_data, employees_data]
        transformed_chinook_invoices = chinook_etl.join_invoices(invoices_list)

        invoice_items_data = data[5]
        invoice_items_list = [invoice_items_data, transformed_chinook_invoices, transformed_chinook_tracks]
        transformed_chinook_invoice_items = chinook_etl.join_invoice_items(invoice_items_list)
        with TaskGroup('load_tracks') as load_tracks:
            chinook_load_tracks = load_data(transformed_chinook_tracks)
            
        with TaskGroup('load_invoice_items') as load_invoice_items:
            chinook_load_invoice_items = load_data(transformed_chinook_invoice_items)
        
        with TaskGroup('load_invoices') as load_invoices:
            chinook_load_invoices = load_data(transformed_chinook_invoices)
        
        with TaskGroup('load_playlist_tracks') as load_playlist_tracks:
            chinook_load_playlist_tracks = load_data(transformed_chinook_playlist_tracks)

    start = DummyOperator(task_id='start')
    loaded = DummyOperator(task_id='All_data_loaded')
    end = DummyOperator(task_id='end')
    start >> [File_1000, Disaster, Review, Database, load_tracks, load_invoice_items, load_invoices, load_playlist_tracks ] >> loaded >> end

