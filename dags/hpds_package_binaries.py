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
    current_time = datetime.now() 
    packed_file_name = "hpds_phenotype_"+ os.environ.get("BCH_PIC_SURE_HPDS_ALS_TAG")  + "_" + current_time.strftime('%m_%d_%Y_%H_%M_%S')+ "_"+ os.environ.get("BCH_PIC_SURE_HPDS_ETL_TAG") + ".tar.gz" 
    packed_dir=os.environ.get("BCH_HPDS_INTERNAL")    
    kwargs["ti"].xcom_push(key='packed_file_name', value=packed_file_name)
    kwargs["ti"].xcom_push(key='packed_dir', value=packed_dir)
    dp.save_hpds_package_file_name(packed_file_name)

def end_pipeline(**kwargs):
    print("end_pipeline()")


with DAG( "HPDS_PACKAGE_BINARIES",
          description="Packages HPDS Binaries",
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
    
    package_command = " /opt/bitnami/airflow/airflow-data/scripts/hpds_package_binaries.sh " + " {{ ti.xcom_pull(key='packed_file_name')  }} {{ ti.xcom_pull(key='packed_dir')  }} "
    t_package_hpds_files = BashOperator(
        task_id='package_hpds_binaries',
        bash_command=package_command ,
        trigger_rule="none_failed",
        dag=dag)   
    
    
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

    t_pipeline_begin >> t_package_hpds_files >>   t_end_pipeline >> t_notify >> t_cleanup >> t_end
