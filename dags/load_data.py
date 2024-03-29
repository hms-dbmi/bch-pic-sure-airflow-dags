"""
@author: anilkdegala
"""
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import date, timedelta, datetime
from collections import OrderedDict  
from scripts.dag_pebbles import DagPebbles
from airflow.configuration import conf
from scripts.configurations import *
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
    print("begin_pipeline:")
    #TODO:: 
    
def pipeline_enable_check(**kwargs):
    dp = DagPebbles()
    if dp.pipeline_enable_check('DATA_LOAD'):
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")  
    
def load_data(**kwargs):
    print("load_data:") 
    dp = DagPebbles()  
    dp.load_data(log_file_id = None)    

def end_pipeline(**kwargs):
    print("end_pipeline:")


def pipeline_check_skipped(**kwargs):
    print("pipeline_check_skipped:") 
     
def cleanup(**kwargs):
    dp = DagPebbles()
    print("cleanup")       

    
def notify(**kwargs):
    dp = DagPebbles()
    print("notify")    

    
def end(**kwargs):
    dp = DagPebbles()
    print("end")             

with DAG( "LOAD_DATA",
          description="Loads Data",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['DATA LOAD'],
          dagrun_timeout=timedelta(hours=240)
    ) as dag:
     

    t_pipeline_begin = PythonOperator(
        task_id="begin_pipeline",
        python_callable=begin_pipeline,
        provide_context=True,
        dag=dag,
    )
    
    t_check_pipeline = BranchPythonOperator(
        task_id="check_pipeline",
        python_callable=pipeline_enable_check,
        provide_context=True,
        dag=dag,
    )
    
    t_pipeline_check_passed = PythonOperator(
        task_id="pipeline_check_passed",
        python_callable=pipeline_check_passed,
        provide_context=True,
        dag=dag,
    )
    
    t_load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
        dag=dag,
    )    
    
    
    t_pipeline_check_skipped = PythonOperator(
        task_id="pipeline_check_skipped",
        python_callable=pipeline_check_skipped,
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
        task_id="send_notifications",
        python_callable=notify,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )
    
    t_cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )
    
    t_end = PythonOperator(
        task_id="end",
        python_callable=end,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )
    
    
    t_pipeline_begin >> t_check_pipeline
    t_check_pipeline >> t_pipeline_check_skipped >> t_end_pipeline 
    t_check_pipeline >> t_pipeline_check_passed >> t_load_data >> t_end_pipeline 
    
    t_end_pipeline >> t_cleanup >> t_notify >> t_end
