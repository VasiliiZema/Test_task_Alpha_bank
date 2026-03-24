from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, sum as spark_sum, count, date_trunc
import argparse
from airflow.hooks.base_hook import BaseHook

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
        .appName("BuildMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Чтение STAGE таблицы
    df_stage = spark.read \
        .format("jdbc") \
        .option("url", db_config['url']) \
        .option("dbtable", "stage.covid_stage_daily") \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", db_config['driver']) \
        .load()
    
    print(f"Loaded {df_stage.count()} rows from stage_covid_daily")
    
    # Агрегация по месяцам и годам
    df_mart = df_stage.groupBy(
        year("date").alias("year"),
        month("date").alias("month"),
        date_trunc("month", "date").alias("start_month")
    ).agg(
        spark_sum("cases_sick").alias("total_cases_sick"),
        count("*").alias("days_count")
        #,(spark_sum("cases_sick") / count("*")).alias("avg_daily_cases_sick")
    ).orderBy("year", "month")
    
    print(f"Created mart with {df_mart.count()} rows")
    
    table_name = "mart.statistics_covid_sick"
    
    # Сохраняем витрину
    df_mart.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", db_config['url']) \
        .option("dbtable", table_name) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", db_config['driver']) \
        .save()
    
    print(f"Successfully wrote to {table_name}")
    
    # Выводим статистику для логов
    print("\n=== MART Data Summary ===")
    df_mart.show(20, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()
