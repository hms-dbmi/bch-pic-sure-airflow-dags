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

default_args = {
    "owner": "anilkdegala",
    "depends_on_past": True,
    "max_active_runs": 1,
    "start_date": datetime(2015, 6, 1),
    "is_active": True,
    "is_paused_upon_creation": False,
}

def begin_pipeline(**kwargs):
    dp = DagPebbles()
    pipeline = dp.get_current_pipeline()
    print(pipeline)

def check_recreate_hpds_source_data(**kwargs):
    if True:
        return "recreate_hpds_source_data"
    else:
        return "skip_recreate_hpds_source_data"

def recreate_hpds_source_data(**kwargs):
    dp = DagPebbles()
    dp.recreate_bch_hpds_data()
    
    
def clean_hpds_source_data(**kwargs):
    dp = DagPebbles()
    dp.clean_hpds_source_data()    

def skip_recreate_hpds_source_data(**kwargs):
    print("skip_recreate_hpds_source_data()")

def end_pipeline(**kwargs):
    print("end_pipeline()")


with DAG( "HPDS_EXTRACT_DATA",
          description="Extracts data for HPDS from Loaded Data",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['HPDS'],
          dagrun_timeout=timedelta(hours=72)
    ) as dag:


    t_pipeline_begin = PythonOperator(
        task_id="begin_pipeline",
        python_callable=begin_pipeline,
        provide_context=True,
        dag=dag,
    )

    t_check_recreate_hpds_source_data = BranchPythonOperator(
        task_id="check_recreate_hpds_source_data",
        python_callable=check_recreate_hpds_source_data,
        provide_context=True,
        dag=dag,
    )

    t_recreate_hpds_source_data = PythonOperator(
        task_id="recreate_hpds_source_data",
        python_callable=recreate_hpds_source_data,
        provide_context=True,
        dag=dag,
    )
    
    t_clean_hpds_source_data = PythonOperator(
        task_id="clean_hpds_source_data",
        python_callable=clean_hpds_source_data,
        provide_context=True,
        dag=dag,
    )     

    t_skip_recreate_hpds_source_data = PythonOperator(
        task_id="skip_recreate_hpds_source_data",
        python_callable=skip_recreate_hpds_source_data,
        provide_context=True,
        dag=dag,
    )
    
    t_end_pipeline = PythonOperator(
        task_id="end_pipeline",
        python_callable=end_pipeline,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )

    t_notify = PythonOperator(
        task_id="notifications",
        python_callable=end_pipeline,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )

    t_cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=end_pipeline,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )

    t_end = PythonOperator(
        task_id="end",
        python_callable=end_pipeline,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )

    t_pipeline_begin >> t_check_recreate_hpds_source_data
    t_check_recreate_hpds_source_data>> t_recreate_hpds_source_data >> t_clean_hpds_source_data  >> t_end_pipeline
    t_check_recreate_hpds_source_data >> t_skip_recreate_hpds_source_data >>  t_end_pipeline
    t_end_pipeline>> t_notify >> t_cleanup >> t_end
