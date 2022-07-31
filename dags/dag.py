from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

with DAG('syarif',
    start_date=datetime(2022, 7, 26)       
) as dag:

    start = DummyOperator(
        task_id='start'
    )    
      
    create_tb_mysql = BashOperator(
        task_id='create_tb_mysql',
        bash_command=f"""python3 /opt/airflow/dags/create/create_mysql_tb.py"""
    )

    load_case = BashOperator(
        task_id='load_case',
        bash_command=f"""python3 /opt/airflow/dags/load/insert_mysql_tb.py"""
    )

    start >> create_tb_mysql >> load_case