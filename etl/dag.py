from datetime import datetime
from airflow import DAG
from scripts import extract, transform, load
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

from scripts import extract, transform, load

SPARK_HOST = "spark://spark-master-2:7077"
DB_HOST="customerdb"
DB_PORT=5432
DB_NAME="customerdb"
HDFS_NAMENODE_URL="hdfs://hdfs-namenode:8020"
# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Initialize the DAG
with DAG(
    dag_id='example_etl_pipeline',
    default_args=default_args,
    description='An example DAG for an ETL pipeline',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    spark = SparkSession.builder \
        .appName("CASK-ETL") \
        .master(SPARK_HOST) \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE_URL) \
        .config("spark.driver.memory", "12g") \
        .getOrCreate()
    
    @dag.task(task_id="extract_data")
    def extract_data():
        spark.catalog.clearCache()
        input_path = "hdfs://hdfs-namenode:8020/data/raw/orders.csv"
        return extract.extract(spark, input_path, DB_HOST, DB_PORT, DB_NAME, "customers")
    
    @dag.task(task_id="transform_data")
    def transform_data(data):
        orders, customers = data
        return transform.transform(orders, customers)
    
    @dag.task(task_id="load_data")
    def load_data(data):
        orders, customers = data
        return load.load(spark, customers, orders, DB_HOST, DB_PORT, DB_NAME, "public.integratedorders")
    
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)