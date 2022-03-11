from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
from plugins import slack

import requests
import logging


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def extract(**context):
    execution_date = context['execution_date']

    logging.info(execution_date)
    params = {
        "lat": "37.532600",
        "lon": "127.024612",
        "appid": context["params"]["appid"],
        "exclude": "current,minutely,hourly,alerts",
        "units": "metric",
    }
    f = requests.get('https://api.openweathermap.org/data/2.5/onecall', params=params)
    return f.json()


def transform(**context):
    json = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    return [(datetime.fromtimestamp(d["dt"]).strftime('%Y-%m-%d'), d['temp']['day'], d['temp']['max'], d['temp']['min']) for d in json['daily']]


def load(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]

    cur = get_Redshift_connection()
    rows = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    sql += f"INSERT INTO {schema}.{table} VALUES {str(rows)[1:-1].replace('[', '(').replace(']', ')')};"
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


dag_weather_forecast = DAG(
    dag_id='weather_forecast',
    start_date=datetime(2022, 3, 11),
    schedule_interval='0 2 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback,
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'appid': Variable.get("open_weather_api_key")
    },
    provide_context=True,
    dag=dag_weather_forecast)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    params={
    },
    provide_context=True,
    dag=dag_weather_forecast)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'tj970203',
        'table': 'weather_forecast'
    },
    provide_context=True,
    dag=dag_weather_forecast)

extract >> transform >> load