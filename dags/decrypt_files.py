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
    #TODO:: 
    
def pipeline_enable_check(**kwargs):
    dp = DagPebbles()
    if dp.pipeline_enable_check('DECRYPT_FILES'):
        kwargs["ti"].xcom_push(key="S3_BUCKET", value=os.environ.get("S3_BUCKET",""))
        kwargs["ti"].xcom_push(key="SKIP_DECRYPT_FILES", value="N")
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")  
    
def decryt(**kwargs):
    print("decryt:")                

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

with DAG( "DECRYPT_FILES",
          description="Decrypt Data Pipeline Input Files",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['DECRYPT'],
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
        files = dp.get_files_to_decrypt(None)
        #files = ['1','2','3','4','5','6'] 
        #files = None
        
        if files == None or len(files) == 0:
            t_pipeline_check_passed  >> t_end_pipeline
        else:
            for index, file in enumerate(files):
                target_file = file.replace(".encrypted", "")
                decrypt_dmp_file_cmd = "/opt/bitnami/airflow/airflow-data/scripts/decrypt_s3_file.sh  " + file + " " + target_file + " {{ ti.xcom_pull(key='SKIP_DECRYPT_FILES')}} "
                t_download_dmp_file = BashOperator(
                    task_id='decrypt_dmp_file_'+str(index),
                    bash_command=decrypt_dmp_file_cmd,
                    dag=dag)
                t_pipeline_check_passed >> t_download_dmp_file >> t_end_pipeline    
    except Exception as e:
        print(e) 
        t_pipeline_check_passed >> t_end_pipeline
        pass
     
    
    t_end_pipeline >> t_cleanup >> t_notify >> t_end
