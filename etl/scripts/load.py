from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window

def load(spark: SparkSession, customers, orders, db_host, db_port, db_name, table: str):

    star_df = customers.join(orders, customers.customer_id == orders.customer_id, "inner") \
                        .drop(orders.customer_id)
                        
    star_df = star_df.withColumn('total_amount', f.sum(
    'amount').over(Window.partitionBy('customer_id')))


    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    columns = ["customer_id", "customer_name", "region", "order_id", "order_date", "total_amount"]
    star_df = star_df.select(columns)

    star_df.write.jdbc(db_url, table, mode="append", properties=db_properties)

# if __name__ == "__main__":
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("FinancialTransactionsETL") \
#         .master("local[*]") \
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
#         .getOrCreate()

#     # Extract data
#     orders, customers = extract(spark)

#     # Transform data
#     orders, customers = transform(orders, customers)

#     # Load data
#     load(spark, customers, orders, "localhost", 5432, "customerdb", "public.integratedorders")

#    spark.stop()