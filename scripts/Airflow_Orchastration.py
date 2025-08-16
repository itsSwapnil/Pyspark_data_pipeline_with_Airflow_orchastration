import os
import time
#Import MySQL module for mysql connectivity
import MySQLdb
import pandas as pd
from pandas.core.common import flatten
import airflow
from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta,datetime
from datetime import date
today = date.today()
d4 = today.strftime("%Y-%b-%d")

#today = date.today().strftime("%A")
default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': None,
    'email_on_failure': False,
    'email_on_success': False,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'cmd_timeout' : None,
    'do_xcom_push': False,
    }

dag = DAG(
    dag_id='vehicle-usage-service-Incremental',
    start_date= datetime(year=2022, month=2, day=15),
    default_args=default_args,
    catchup=False,
    description='Loading Incremental data with operations',
    schedule_interval=None
    )

ssh_conn_id = "ssh_vehicle"

Start=DummyOperator(task_id="Start",dag=dag)


Task1 = SSHOperator(
                    ssh_conn_id=ssh_conn_id,
                    task_id='data_preprocessing',
                    command='spark-submit --num-executors 12 --executor-cores 5 --executor-memory 23g --driver-memory 23g --driver-cores 6  --conf spark.dynamicAllocation.enabled=false --conf spark.sql.catalogImplementation=hive --conf spark.hadoop.hive.exec.dynamic.partition=true --conf spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict --conf spark.hadoop.hive.enforce.bucketing=true --conf spark.shuffle.compress=true --conf spark.broadcast.compress=true  --conf spark.sql.autoBroadcastJoinThreshold=-1 --conf spark.sql.files.ignoreCorruptFiles=true  /scripts/vehicle_data_pipeline.py  & > /scripts/logs/vehicle_data_pipeline.log', depends_on_past=False, dag=dag)

Task2 = SSHOperator(
                    ssh_conn_id=ssh_conn_id,
                    task_id='incremental_date',
                    command='python3 /scripts/incremental_date.py > /scripts/logs/incremental_date_$(date +\\%Y-\\%m-\\%d).log',
                    depends_on_past=False,dag=dag)


Task3 = TriggerDagRunOperator(
                    task_id='Trigger_dag_increment_date',
                    trigger_dag_id='vehicle-usage-service-Incremental',
                    dag=dag)


End=DummyOperator(task_id="End",dag=dag)


Start>>Task1>>Task2>>Task3>>End
