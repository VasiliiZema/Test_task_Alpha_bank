from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'covid_etl_pipeline',
    default_args=default_args,
    description='COVID-19 ETL pipeline: RAW -> STAGE -> MART',
    schedule_interval='@once',
    catchup=False,
    tags=['covid', 'etl', 'spark'],
) as dag:
    
    
    # Задача 1: Загрузка RAW слоя (CSV -> PostgreSQL)
    load_raw_layer = SparkSubmitOperator(
        task_id='load_raw_layer',
        application='/usr/local/airflow/dags/spark_scripts/load_raw.py',
        name='load_raw_covid',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        },
        # Передаем Connection ID как параметр
        application_args=[
            '--postgres_conn_id', 'postgres_default'
        ]
    )
    
    # Задача 2: Транспонирование данных (RAW -> STAGE)
    transform_to_stage = SparkSubmitOperator(
        task_id='transform_to_stage',
        application='/usr/local/airflow/dags/spark_scripts/transform_stage.py',
        name='transform_covid_stage',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        },
        application_args=[
            '--postgres_conn_id', 'postgres_default'
        ]
    )
    
    # Задача 3: Построение витрины (STAGE -> MART)
    build_mart_layer = SparkSubmitOperator(
        task_id='build_mart_layer',
        application='/usr/local/airflow/dags/spark_scripts/build_mart.py',
        name='build_covid_mart',
        conn_id='spark_default',
        verbose=True,
        conf={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        },
        application_args=[
            '--postgres_conn_id', 'postgres_default'
        ]
    )
  
    # Определяем последовательность выполнения

    load_raw_layer >> transform_to_stage >> build_mart_layer
