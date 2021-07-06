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

def begin_pipeline(**kwargs):
    dp = DagPebbles()
    pipeline = dp.get_current_pipeline()
    print(pipeline)

def check_hpds_data_load(**kwargs):
    if True:
        return "hpds_sql_loader"
    else:
        return "skip_hpds_data_load" 

def skip_hpds_data_load(**kwargs):
    print("skip_hpds_data_load()") 

def end_pipeline(**kwargs):
    print("end_pipeline()")


with DAG( "HPDS_LOAD_DATA",
          description="Generate javabins using SQL Loader",
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
    
    generate_encryption_command = " /opt/bitnami/airflow/airflow-data/scripts/generate_encryption_key.sh "
    t_generate_encryption_key = BashOperator(
        task_id='generate_encryption_key',
        bash_command=generate_encryption_command ,
        trigger_rule="none_failed",
        dag=dag)    

    t_check_hpds_data_load = BranchPythonOperator(
        task_id="check_hpds_data_load",
        python_callable=check_hpds_data_load,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag,
    )

    t_hpds_data_load = DockerOperator(
            default_owner='root',
            task_id='hpds_sql_loader',
            image=os.environ.get("BCH_PIC_SURE_HPDS_ETL_IMAGE", None),
            api_version='auto',
            auto_remove=True,
            command="/bin/sleep 30",
            docker_url="unix://var/run/docker.sock",
            network_mode="bch_network",
            start_date=datetime(2001, 5, 1),
            environment={
                'AF_EXECUTION_DATE': "{{ ds }}",
                'AF_OWNER': "{{ task.owner }}",
                'HEAPSIZE': 40960,
                'LOADER_NAME': 'SQLLoader',
            },
            volumes=[os.environ["BCH_HPDS_EXTERNAL"] + ":/opt/local/hpds"],
            trigger_rule="none_failed",
        )

    t_skip_hpds_data_load = PythonOperator(
        task_id="skip_hpds_data_load",
        python_callable=skip_hpds_data_load,
        provide_context=True,
        trigger_rule="none_failed",
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

    t_pipeline_begin >> t_generate_encryption_key >> t_check_hpds_data_load 
    t_check_hpds_data_load >> t_hpds_data_load >>  t_end_pipeline
    t_check_hpds_data_load >> t_skip_hpds_data_load >>  t_end_pipeline 
    t_end_pipeline>> t_notify >> t_cleanup >> t_end
