from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, to_date
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
        .appName("TransformToStage") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Чтение всех RAW таблиц
    raw_tables = ['raw_covid_2020', 'raw_covid_2021', 'raw_covid_2022', 'raw_covid_2023']
    schema = 'raw.'
    
    # Собираем DataFrame из всех RAW таблиц
    dfs = []
    for table in raw_tables:
        df = spark.read \
            .format("jdbc") \
            .option("url", db_config['url']) \
            .option("dbtable", schema + table) \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", db_config['driver']) \
            .load()
        dfs.append(df)
    
    # Объединяем все таблицы
    df_raw = dfs[0]
    for df in dfs[1:]:
        df_raw = df_raw.unionByName(df, allowMissingColumns=True)
    
    print(f"Loaded {df_raw.count()} rows from RAW tables")
    
    # Идентифицируем столбцы с датами (формат M/D/YY или подобный)
    # Исключаем известные текстовые столбцы
    exclude_columns = {'Country/Region', 'Province/State', 'year'}
    date_columns = [c for c in df_raw.columns if c not in exclude_columns]
    
    print(f"Found {len(date_columns)} date columns")
    
    # КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: экранируем имена колонок с точками
    # Создаем список колонок с экранированными именами
    escaped_columns = []
    for col_name in date_columns:
        # Экранируем имя колонки обратными кавычками
        escaped_col = f"`{col_name}`"
        # Преобразуем в int, NULL если не получается
        escaped_columns.append(f"CAST({escaped_col} AS INT) AS {escaped_col}")

    
    # Создаем выражение для select
    select_expr = [
            "`Country/Region` AS country_region",
            "`Province/State` AS province_state"
        ] + escaped_columns
        
    # Применяем преобразование
    df_casted = df_raw.selectExpr(select_expr)
        
    # Теперь создаем выражение stack для преобразования широкого формата в длинный
    # stack(n, col1, col2, ...) as (date_str, cases)
    stack_parts = []
    for col_name in date_columns:
        escaped_col = f"`{col_name}`"
        stack_parts.append(f"'{col_name}', {escaped_col}")
        
    stack_expr = f"stack({len(date_columns)}, {', '.join(stack_parts)}) as (date_str, cases_sick)"
        
    # Применяем stack
    df_stage = df_casted.select(
            col("country_region"),
            col("province_state"),
            expr(stack_expr)
        )
        
    # Фильтруем нулевые значения
    df_stage = df_stage.filter(col("cases_sick").isNotNull())# & (col("cases") > 0))
    
    # Преобразуем строку даты в формат DATE
    # Пробуем разные форматы
    df_stage = df_stage.withColumn(
            "date",
            expr("""
                CASE 
                    WHEN date_str LIKE '%/%' THEN 
                        TO_DATE(date_str, 'M/d/yy')
                    WHEN date_str LIKE '%.%' THEN 
                        TO_DATE(date_str, 'MM.dd.yyyy')
                    ELSE NULL
                END
            """)
        )
        
    # Убираем строки с неправильными датами
    df_stage = df_stage.filter(col("date").isNotNull()).select(col("country_region"), col("province_state"), col("cases_sick"), col("date"))
        
    print(f"Transformed {df_stage.count()} rows")
    
    # Создаем таблицу в PostgreSQL
    stage_table = "stage.covid_stage_daily"
        
    print(f"Writing to {stage_table}...")
    
    # Записываем в STAGE слой
    df_stage.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", db_config['url']) \
        .option("dbtable", stage_table) \
        .option("user", db_config['user']) \
        .option("password", db_config['password']) \
        .option("driver", db_config['driver']) \
        .option("batchsize", "10000") \
        .save()
    
    print("Successfully wrote to stage_covid_daily")
    
    spark.stop()

if __name__ == "__main__":
    main()
