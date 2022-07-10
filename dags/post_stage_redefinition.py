"""
@author: anilkumardegala
"""
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import date, timedelta, datetime
from collections import OrderedDict
from scripts.dag_pebbles import DagPebbles
from airflow.configuration import conf
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.docker_operator import DockerOperator

default_args = {
    "owner": "anilkdegala",
    "depends_on_past": True,
    "max_active_runs": 1,
    "start_date": datetime(2015, 6, 1),
    "is_active": True,
    "is_paused_upon_creation": False,
}

def post_stage_redefinition(**kwargs):
    dp = DagPebbles()
    dp.post_stage_redefinition()
 

with DAG( "POST_STAGE_REDEFINITION",
          description="Post stage redefinition",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['STAGE'],
          dagrun_timeout=timedelta(hours=72)
    ) as dag:


    t_post_stage_redefinition = PythonOperator(
        task_id="post_stage_redefinition",
        python_callable=post_stage_redefinition,
        provide_context=True,
        dag=dag,
    )
     

    t_post_stage_redefinition
