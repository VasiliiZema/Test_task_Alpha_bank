from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import sys
import argparse
from airflow.hooks.base_hook import BaseHook
import os

def get_postgres_connection(conn_id):
    """Получаем параметры подключения из Connection Airflow"""
    connection = BaseHook.get_connection(conn_id)
    return {
        'url': f"jdbc:postgresql://{connection.host}:{connection.port}/{connection.schema}",
        'user': connection.login,
        'password': connection.password,
        'driver': 'org.postgresql.Driver'
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--postgres_conn_id', required=True, help='Airflow connection ID for PostgreSQL')
    args = parser.parse_args()
    
    # Получаем параметры подключения
    db_config = get_postgres_connection(args.postgres_conn_id)
    
    # Создаем Spark сессию
    spark = SparkSession.builder \
        .appName("LoadRawCOVID") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Загрузка CSV файлов по годам
    years = [2020, 2021, 2022, 2023]
    data_path = '/usr/local/airflow/data'
    
    for year in years:
        csv_file = f"{data_path}/raw_{year}.csv"
        
        if not os.path.exists(csv_file):
            print(f"File {csv_file} not found, skipping...")
            continue
            
        print(f"Loading {csv_file}...")
        
        # Читаем CSV с автоопределением схемы
        df = spark.read \
                .option("header", "true") \
                .option("delimiter", ";") \
                .option("inferSchema", "true") \
                .option("multiLine", "true") \
                .option("encoding", "UTF-8") \
                .csv(csv_file)
        
        # Добавляем год как отдельный столбец
        df = df.withColumn("year", lit(year))
        
        # Сохраняем в PostgreSQL
        table_name = f"raw.raw_covid_{year}"
        print(f"Writing to {table_name}...")
        
        df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", db_config['url']) \
            .option("dbtable", table_name) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", db_config['driver']) \
            .option("batchsize", "10000") \
            .option("truncate", "true") \
            .save()
        
        print(f"Successfully loaded {csv_file} into {table_name}")
    
    spark.stop()

if __name__ == "__main__":
    main()
