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
    dp = DagPebbles()
    pipeline = dp.get_current_pipeline() 
    s3_bucket = os.environ.get("S3_BUCKET","")
    folder_path = pipeline['log_file_path']  
    s3_file = pipeline['log_file_name'] 
    download_key = dp.get_download_key(s3_bucket, folder_path, s3_file) 
    kwargs["ti"].xcom_push(key="folder_path", value=folder_path)
    kwargs["ti"].xcom_push(key="s3_file", value=s3_file)   
    kwargs["ti"].xcom_push(key="download_key", value=download_key) 
    
    
    
def pipeline_enable_check(**kwargs):
    dp = DagPebbles()
    if dp.pipeline_enable_check('DOWNLOAD_FILES'):
        kwargs["ti"].xcom_push(key="S3_BUCKET", value=os.environ.get("S3_BUCKET",""))
        kwargs["ti"].xcom_push(key="SKIP_DOWNLOAD_FILES", value="N")
        return "pipeline_check_passed"
    else:
        return "pipeline_check_skipped" 

def pipeline_check_passed(**kwargs):
    print("pipeline_check_passed:")  
    
def download(**kwargs):
    print("download:")                

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

with DAG( "DOWNLOAD_FILES",
          description="Download Data Pipeline Input Files",
          default_args=default_args,
          schedule_interval=None,
          catchup=False,
          orientation="TB",
          tags=['DOWNLOADS'],
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
        s3_bucket = os.environ.get("S3_BUCKET","")
        folder_path = pipeline['log_file_path']  
        s3_file = pipeline['log_file_name']
        download_key = dp.get_download_key(s3_bucket, folder_path, s3_file) 
        download_log_file_cmd = "/opt/bitnami/airflow/airflow-data/scripts/download_s3_file.sh  " + download_key + " "  + s3_file +  " " + "N"
        t_download_log_file = BashOperator(
            task_id='download_log_file',
            bash_command=download_log_file_cmd,
            dag=dag) 
        t_pipeline_check_passed >> t_download_log_file
        
        files = dp.get_files(log_file_id = None, type = 'download')
        if files == None or len(files) == 0:
            t_download_log_file  >> t_end_pipeline
        else:
            for index, file in enumerate(files):
                s3_bucket = os.environ.get("S3_BUCKET","")
                folder_path = pipeline['log_file_path']  
                s3_file = file
                download_key = dp.get_download_key(s3_bucket, folder_path, s3_file) 
                
                download_dmp_file_cmd = "/opt/bitnami/airflow/airflow-data/scripts/download_s3_file.sh  " + download_key + " "  + s3_file +  " " + " {{ ti.xcom_pull(key='SKIP_DOWNLOAD_FILES')}} "                                
                t_download_dmp_file = BashOperator(
                    task_id='download_dmp_file_'+str(index),
                    bash_command=download_dmp_file_cmd,
                    dag=dag)
                t_download_log_file >> t_download_dmp_file >> t_end_pipeline    
    except Exception as e:
        print(e) 
        t_download_log_file = DummyOperator(dag=dag, task_id='download_log_file')
        t_pipeline_check_passed >> t_download_log_file >> t_end_pipeline
        pass
     
    
    t_end_pipeline >> t_cleanup >> t_notify >> t_end
