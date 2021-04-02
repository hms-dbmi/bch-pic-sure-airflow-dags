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
from scripts.mappings import Mappings
from airflow.operators.docker_operator import DockerOperator
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
    
def pipeline_enable_check(**kwargs):
    dp = DagPebbles()
    if dp.pipeline_enable_check('CONCEPT_DIM_MAPPING'):
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")  
    
    
def concept_dim_mapping_prep(**kwargs):
    print("concept_dim_mapping_prep:")
    m =  Mappings()
    m.concept_dim_mapping_prep()  

def allergies(**kwargs):
    print("allergies:") 
    m =  Mappings()
    m.allergies()

def specimens(**kwargs):
    print("specimens:") 
    m =  Mappings()
    m.specimens()

def clinic_site(**kwargs):
    print("clinic_site:") 
    m =  Mappings()
    m.clinic_site()

def demographics(**kwargs):
    print("demographics:") 
    m =  Mappings()
    m.demographics()
    

def diagnosis(**kwargs):
    print("diagnosis:")
    m =  Mappings()
    m.diagnosis() 

def insurance_payors(**kwargs):
    print("insurance_payors:") 
    m =  Mappings()
    m.insurance_payors() 

def laboratory_results(**kwargs):
    print("laboratory_results:") 
    m =  Mappings()
    m.laboratory_results() 

def medications(**kwargs):
    print("medications:")
    m =  Mappings()
    m.medications() 

def notes(**kwargs):
    print("notes:") 
    m =  Mappings()
    m.notes() 

def procedures(**kwargs):
    print("procedures:") 
    m =  Mappings()
    m.procedures() 

def protocols(**kwargs):
    print("protocols:")
    m =  Mappings()
    m.protocols()  

def services(**kwargs):
    print("services:") 
    m =  Mappings()
    m.services()  


def vital_signs(**kwargs):
    print("vital_signs:")
    m =  Mappings()
    m.vital_signs()   

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

with DAG( "CONCEPT_DIMENSION_MAPPING",
          description="Concept Dimension Mapping",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['ADHOC'],
          dagrun_timeout=timedelta(hours=1)
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
    
    t_concept_dim_mapping_prep = PythonOperator(
        task_id="concept_dim_mapping_prep",
        python_callable=concept_dim_mapping_prep,
        provide_context=True,
        dag=dag,
    )
    
    t_allergies = PythonOperator(
        task_id="allergies",
        python_callable=allergies,
        provide_context=True,
        dag=dag,
    )
    
    t_specimens = PythonOperator(
        task_id="specimens",
        python_callable=specimens,
        provide_context=True,
        dag=dag,
    )
    
    t_clinic_site = PythonOperator(
        task_id="clinic_site",
        python_callable=clinic_site,
        provide_context=True,
        dag=dag,
    )
    
    t_demographics = PythonOperator(
        task_id="demographics",
        python_callable=demographics,
        provide_context=True,
        dag=dag,
    )
    
    t_diagnosis = PythonOperator(
        task_id="diagnosis",
        python_callable=diagnosis,
        provide_context=True,
        dag=dag,
    )
    
    t_insurance_payors = PythonOperator(
        task_id="insurance_payors",
        python_callable=insurance_payors,
        provide_context=True,
        dag=dag,
    )
    
    t_laboratory_results = PythonOperator(
        task_id="laboratory_results",
        python_callable=laboratory_results,
        provide_context=True,
        dag=dag,
    )
    
    t_medications = PythonOperator(
        task_id="medications",
        python_callable=medications,
        provide_context=True,
        dag=dag,
    )
    
    t_notes = PythonOperator(
        task_id="notes",
        python_callable=notes,
        provide_context=True,
        dag=dag,
    )
    
    t_procedures = PythonOperator(
        task_id="procedures",
        python_callable=procedures,
        provide_context=True,
        dag=dag,
    )
    
    t_protocols = PythonOperator(
        task_id="protocols",
        python_callable=protocols,
        provide_context=True,
        dag=dag,
    )
    
    t_services = PythonOperator(
        task_id="services",
        python_callable=services,
        provide_context=True,
        dag=dag,
    )
    
    t_vital_signs = PythonOperator(
        task_id="vital_signs",
        python_callable=vital_signs,
        provide_context=True,
        dag=dag,
    )   
        
    
    t_pipeline_begin >> t_check_pipeline
    t_check_pipeline >> t_pipeline_check_skipped >> t_end_pipeline 
    t_check_pipeline >> t_pipeline_check_passed
    
    t_pipeline_check_passed >>  t_concept_dim_mapping_prep >> [ 
        t_allergies,
        t_specimens,
        t_clinic_site,
        t_demographics,
        t_diagnosis,
        t_insurance_payors,
        t_laboratory_results,
        t_medications,
        t_notes,
        t_procedures,
        t_protocols,
        t_services,
        t_vital_signs  
    ] >> t_end_pipeline 
    
    t_end_pipeline >> t_cleanup >> t_notify >> t_end
