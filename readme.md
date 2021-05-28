# Airflow-ETL

This project is about simulating ETL process using Apache Airflow. We have some data that comes with many forms as shown inside `./data/0. raw/` directory.
Our objective is to store them into a Data Warehouse as a single source.

## Installation
requirements: you sould have Airflow installed. If you don't know how to install it, [read here](https://airflow.apache.org/docs/apache-airflow/stable/installation.html)

```
airflow webserver
```

and in another terminal

```
airflow scheduler
```

You can access Airflow UI in `localhost:8080` on your favorite web browser. Input username and password `airflow` and `airflow` if asked

## Data
You can find the data in folder `./data/0. raw/`.