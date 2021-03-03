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
from scripts.configurations import *

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
    if dp.pipeline_enable_check('TRANSFER_FILES'):
        kwargs["ti"].xcom_push(key="S3_BUCKET", value=os.environ.get("S3_BUCKET",""))
        kwargs["ti"].xcom_push(key="SKIP_TRANSFER_FILES", value="N")
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")  
    
def transfer(**kwargs):
    print("transfer:")                

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

with DAG( "TRANSFER_FILES",
          description="Transfer Data Pipeline Input Files",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['TRANSFER'],
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
    
    
    t_pipeline_begin >> t_check_pipeline
    t_check_pipeline >> t_pipeline_check_skipped >> t_end_pipeline 
    t_check_pipeline >> t_pipeline_check_passed
    
    try: 
        dp = DagPebbles() 
        pipeline = dp.get_current_pipeline()  
        log_file = DATA_LOCATION + "/"+ pipeline['log_file_name'].replace(".encrypted", "")
        transfer_log_file_cmd = "perl  /opt/bitnami/airflow/airflow-data/scripts/transfer_file_rds.pl   " +  log_file + "   {{ ti.xcom_pull(key='SKIP_TRANSFER_FILES')}}"
        t_transfer_log_file = BashOperator(
            task_id='transfer_log_file',
            bash_command=transfer_log_file_cmd,
            dag=dag)
        
        t_pipeline_check_passed >> t_transfer_log_file 
                        
        files = dp.get_files(log_file_id = None, type = 'transfer')
        
        if files == None or len(files) == 0:
            t_transfer_log_file  >> t_end_pipeline
        else:
            for index, file in enumerate(files):
                file = DATA_LOCATION + "/"+ file.replace(".encrypted", "")
                transfer_file_cmd = "perl  /opt/bitnami/airflow/airflow-data/scripts/transfer_file_rds.pl   " +  file + "   {{ ti.xcom_pull(key='SKIP_TRANSFER_FILES')}}"
                t_transfer_dmp_file = BashOperator(
                    task_id='transfer_dmp_file_'+str(index),
                    bash_command=transfer_file_cmd,
                    dag=dag)
                t_transfer_log_file >> t_transfer_dmp_file >> t_end_pipeline    
    except Exception as e:
        print(e) 
        t_transfer_log_file >> t_end_pipeline
        pass
     
    
    t_end_pipeline >> t_cleanup >> t_notify >> t_end
