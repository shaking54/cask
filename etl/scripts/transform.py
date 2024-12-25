from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

def transform(orders, customers):
    # Join the DataFrames
    orders.dropDuplicates()
    customers.dropDuplicates()

    orders = orders.withColumn("order_date", orders["order_date"].cast("timestamp"))
    customers = customers.withColumn("dob", customers["dob"].cast("timestamp"))

    # format datetime to YYYY-MM-DD
    orders = orders.withColumn("formatted_date", date_format("order_date", "yyyy-MM-dd"))
    customers = customers.withColumn("formatted_dob", date_format("dob", "yyyy-MM-dd"))
    
    return orders, customers

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

#     # Show the DataFrame
#     orders.show()
#     customers.show()

#     spark.stop()