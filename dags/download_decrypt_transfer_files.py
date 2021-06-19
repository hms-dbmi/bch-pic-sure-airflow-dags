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
    files = kwargs['dag_run'].conf.get('files')
    
    download_decrypt_arguments = ''
    transfer_arguments_list = []
    for f in files:
        print("download_decrypt_transfer_files: file:  ", f['name'], ', location: ', f['path'])  
        output = f['name']+','+f['path']+','+f['final_name']
        download_decrypt_arguments = download_decrypt_arguments + " " + output
        transfer_arguments_list.append(DATA_LOCATION + "/"+f['final_name'])
    
    transfer_arguments = ",".join(transfer_arguments_list)
    print("final download_decrypt_arguments: ",download_decrypt_arguments)
    print("final transfer_arguments: ",transfer_arguments) 
    kwargs["ti"].xcom_push(key="download_decrypt_arguments", value=download_decrypt_arguments)
    kwargs["ti"].xcom_push(key="transfer_arguments", value=transfer_arguments)
     
    
def pipeline_enable_check(**kwargs):
    dp = DagPebbles()
    if dp.pipeline_enable_check('DATA_LOAD'):
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")    

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

with DAG( "DOWNLOAD_DECRYPT_TRANSFER",
          description="Download, Decrypt, Transfer files (Source: S3, Staging: EC2: Target: RDS Oracle)",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['Utils'],
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
    
    t_pipeline_check_skipped = PythonOperator(
        task_id="pipeline_check_skipped",
        python_callable=pipeline_check_skipped,
        provide_context=True,
        dag=dag,
    )
    
    download_files_cmd = "/opt/bitnami/airflow/airflow-data/scripts/download_files.sh  "+"{{ ti.xcom_pull(key='download_decrypt_arguments')}}"                             
    t_download_files = BashOperator(
        task_id='download_files',
        bash_command=download_files_cmd,
        dag=dag)    
    
    decrypt_files_cmd = "/opt/bitnami/airflow/airflow-data/scripts/decrypt_files.sh  "+"{{ ti.xcom_pull(key='download_decrypt_arguments')}} "
    t_decrypt_files = BashOperator(
        task_id='decrypt_files',
        bash_command=decrypt_files_cmd,
        dag=dag) 
    
    transfer_files_cmd = "/opt/bitnami/airflow/airflow-data/scripts/transfer_files_rds.pl  "+"{{ ti.xcom_pull(key='transfer_arguments')}} "
    t_transfer_files = BashOperator(
        task_id='transfer_files',
        bash_command=transfer_files_cmd,
        dag=dag)     
    
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
    t_check_pipeline >> t_pipeline_check_passed >> t_download_files >> t_decrypt_files >> t_transfer_files  >> t_end_pipeline 
    
    t_end_pipeline >> t_cleanup >> t_notify >> t_end
