from datetime import datetime
from airflow import DAG
from scripts import extract, transform, load
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession
from scripts import etl_ob

import pickle
import json

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
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrationRequired", "false") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE_URL) \
        .config("spark.driver.memory", "12g") \
        .getOrCreate()
    
    configs = {
        "db_host": DB_HOST,
        "db_port": DB_PORT,
        "db_name": DB_NAME,
        "db_user": "postgres",
        "db_password": "postgres"
    }

    etl = etl_ob.ETL(spark, configs)

    @dag.task(task_id="extract_data")
    def extract_data():
        spark.catalog.clearCache()
        input_path = "hdfs://hdfs-namenode:8020/data/raw/orders.csv"
        orders, customers = etl.extract(input_path)
        orders = orders.toPandas().to_json(orient="records", date_format="iso")
        customers = customers.toPandas().to_json(orient="records", date_format="iso")
        # return pickle.dumps((orders, customers))
        return json.dumps({"orders": orders, "customers": customers})

    @dag.task(task_id="transform_data")
    def transform_data(data):
        data = json.loads(data)
        # orders, customers = pickle.loads(data)
        # orders = spark.createDataFrame(json.loads(orders))
        # customers = spark.createDataFrame(json.loads(customers))
        orders_df = etl.read_json(data["orders"])
        customers_df = etl.read_json(data["customers"])

        # transform = etl.transform(orders_df, customers_df)
        orders_transformed, customers_transformed = etl.transform(orders_df, customers_df)

        # return pickle.dumps(transform)
        
        # Serialize transformed data to JSON strings
        return json.dumps({
            "orders": orders_transformed.toPandas().to_json(orient="records", date_format="iso"),
            "customers": customers_transformed.toPandas().to_json(orient="records", date_format="iso")
        })
    
    @dag.task(task_id="load_data")
    def load_data(data):
        # orders_transformed, customers_transformed = pickle.loads(data)
        # etl.load(customers_transformed, orders_transformed, "public.integratedorders")
        
        serialized_data = json.loads(data)
        orders_transformed_df = etl.read_json(serialized_data["orders"])
        customers_transformed_df = etl.read_json(serialized_data["customers"])

        # Load data into the destination
        etl.load(customers_transformed_df, orders_transformed_df, "public.integratedorders")

    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)