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
        if sftp.isfile("file.csv"):
            with sftp.open("file.csv") as f:
                df = pandas.read_csv(f)
                df['customer_id']=1
                print(df.to_dict('records'))
                db['food'].insert_many(df.to_dict('records'))
            sftp.remove("file.csv")
    sftp_d2 = Variable.get("sftp2", deserialize_json=True)
    with pysftp.Connection(cnopts=cnopts, host=sftp_d2["host"], username=sftp_d2["username"], password=sftp_d2["password"]) as sftp2:
        if sftp2.isfile("file.csv"):
            with sftp2.open("file.csv") as f:
                df2 = pandas.read_csv(f)
                df2['customer_id']=2
                print(df2.to_dict('records'))
                db['food'].insert_many(df2.to_dict('records'))
            sftp2.remove("file.csv")


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
    "aws3",
    start_date=datetime(2021, 4, 20),
    max_active_runs=1,
    concurrency=1,
    schedule_interval="*/5 * * * *",
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
