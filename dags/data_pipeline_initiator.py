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
from airflow.operators.docker_operator import DockerOperator
from airflow.configuration import conf

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
    dp = DagPebbles()
    log_file = kwargs['dag_run'].conf.get('log_file')
    kwargs["ti"].xcom_push(key="log_file", value=log_file) 
    kwargs["ti"].xcom_push(key="S3_BUCKET", value=os.environ.get("S3_BUCKET",""))
    print("log_file: ",log_file)
    #TODO::

def pipeline_enable_check(**kwargs):
    dp = DagPebbles()
    if dp.pipeline_enable_check('DATA_PIPELINE_INITIATOR'): 
        #TODO:: get it from db
        kwargs["ti"].xcom_push(key="SKIP_DOWNLOAD_LOG_FILE", value='N') 
        kwargs["ti"].xcom_push(key="SKIP_DECRYPT_LOG_FILE", value='N')  
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:") 
    
def save_pipeline_log(**kwargs):
    print("save_pipeline_log:") 
    dp = DagPebbles()
    dp.save_pipeline_log(kwargs['dag_run'].conf.get('log_file'))
     
  
def validate_log_file(**kwargs):
    print("validate_log_file:") 
    dp = DagPebbles()
    if dp.validate_pipeline_log(kwargs['dag_run'].conf.get('log_file')):
        return "pipeline_log_validation_passed"  
    else:
        return "pipeline_log_validation_failed"  

def pipeline_log_validation_passed(**kwargs):
    print("pipeline_log_validation_passed:")
    
def pipeline_log_validation_failed(**kwargs):
    print("pipeline_log_validation_failed:")
                

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


dag = DAG(
    "DATA_PIPELINE_INITIATOR",
    description="Initiates BCH PL Data Pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    orientation="TB",
    tags=['CHECKS'],
    dagrun_timeout=timedelta(hours=1)
)

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

download_log_file_cmd = "/opt/bitnami/airflow/airflow-data/scripts/download_s3_file.sh  " + "{{ dag_run.conf['log_file'] }} {{ ti.xcom_pull(key='S3_BUCKET')}} {{ ti.xcom_pull(key='SKIP_DOWNLOAD_LOG_FILE')}} "
t_download_log_file = BashOperator(
    task_id='download_log_file',
    bash_command=download_log_file_cmd,
    dag=dag)

decrypt_log_file_cmd = "/opt/bitnami/airflow/airflow-data/scripts/decrypt_s3_file.sh  " + "{{ dag_run.conf['log_file'] }} {{ dag_run.conf['log_file'] }} {{ ti.xcom_pull(key='SKIP_DECRYPT_LOG_FILE')}} "
t_decrypt_log_file = BashOperator(
    task_id='decrypt_log_file',
    bash_command=decrypt_log_file_cmd,
    dag=dag)

t_save_pipeline_log = PythonOperator(
    task_id="save_pipeline_log",
    python_callable=save_pipeline_log,
    provide_context=True,
    dag=dag,
)

t_validate_log_file = BranchPythonOperator(
    task_id="validate_log_file",
    python_callable=validate_log_file,
    provide_context=True,
    dag=dag,
)

t_pipeline_log_validation_failed = PythonOperator(
    task_id="pipeline_log_validation_failed",
    python_callable=pipeline_log_validation_failed,
    provide_context=True,
    dag=dag,
)

t_pipeline_log_validation_passed = PythonOperator(
    task_id="pipeline_log_validation_passed",
    python_callable=pipeline_log_validation_passed,
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
t_check_pipeline >> t_pipeline_check_passed >> t_download_log_file >> t_decrypt_log_file >> t_save_pipeline_log >> t_validate_log_file
 
 
t_validate_log_file >>  t_pipeline_log_validation_failed >> t_end_pipeline 
t_validate_log_file >>  t_pipeline_log_validation_passed >> t_end_pipeline 


 
 
t_end_pipeline >> t_cleanup >> t_notify >> t_end
