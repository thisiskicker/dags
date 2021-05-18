# -*- coding: utf-8 -*-
import pandas
import pysftp
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from pymongo import MongoClient

def sftp_csv_mongo():
    client = MongoClient(Variable.get('mongodb'))
    db = client["testdb"]
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp_d = Variable.get("sftp", deserialize_json=True)
    with pysftp.Connection(cnopts=cnopts, host=sftp_d["host"], username=sftp_d["username"], password=sftp_d["password"]) as sftp:
        with sftp.open("file.csv") as f:
            df = pandas.read_csv(f)
            db.collection.insert_many(df.to_dict('records'))
        sftp.remove("file1.csv")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}

# create DAG
with DAG(
    "aws",
    start_date=datetime(2021, 4, 20),
    max_active_runs=1,
    concurrency=1,
    schedule_interval="*/5 * * * *"
    default_args=default_args,
    catchup=False,
) as dag:
    transform = PythonOperator(
        task_id='sftp_csv_mongo',
        python_callable=sftp_csv_mongo,
        op_kwargs={},
        provide_context=True
    )
    transform
