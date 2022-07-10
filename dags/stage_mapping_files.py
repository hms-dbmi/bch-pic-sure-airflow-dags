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
    if dp.pipeline_enable_check('STAGE_DMP_FILES'):
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")  
    
def stage_biobank_file(**kwargs):
    print("stage_biobank_file:") 
    dp = DagPebbles()  
    dp.stage_biobank_file(log_file_id = None)  
    
def stage_uuid_mapping_file(**kwargs):
    print("stage_uuid_mapping_file:") 
    dp = DagPebbles()  
    dp.stage_uuid_mapping_file("UUID_mapping_08Apr2022.csv")      

def end_pipeline(**kwargs):
    print("end_pipeline:")


def pipeline_check_skipped(**kwargs):
    print("pipeline_check_skipped:") 
     
             

with DAG( "STAGE_MAPPING_FILES",
          description="Stage Mapping Files",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['STAGE'],
          dagrun_timeout=timedelta(hours=72)
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

    t_stage_biobank_file = PythonOperator(
        task_id="stage_biobank_file",
        python_callable=stage_biobank_file,
        provide_context=True,
        dag=dag,
    )
        
    t_stage_uuid_mapping_file = PythonOperator(
        task_id="stage_uuid_mapping_file",
        python_callable=stage_uuid_mapping_file,
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
     
      
    
    t_pipeline_begin >> t_check_pipeline
    t_check_pipeline >> t_pipeline_check_skipped >> t_end_pipeline 
    t_check_pipeline >> t_pipeline_check_passed >> [ 
        t_stage_biobank_file,
        t_stage_uuid_mapping_file
    ] >> t_end_pipeline 
    
    