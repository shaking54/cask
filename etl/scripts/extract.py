from pyspark.sql import SparkSession

def get_from_csv(spark: SparkSession, input_path: str):
    return spark.read.csv(input_path, header=True)  


def get_from_db(spark: SparkSession, db_host, db_port, db_name, table: str):
    return spark.read.format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
        .option("dbtable", table) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .load()

def extract(spark: SparkSession, input_path, db_host="localhost", db_port=5432, db_name="customerdb", table="customers"):
    # Read CSV data
    orders = get_from_csv(spark, input_path)
    customers = get_from_db(spark, db_host, db_port, db_name, table)

    return orders, customers

# if __name__ == "__main__":
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("FinancialTransactionsETL") \
#         .master("local[*]") \
#         .config("spark.jars.packages", "org.postgresql:postgresql:42.2.5") \
#         .getOrCreate()

#     # Extract data
#     orders, customers = extract(spark, "localhost", 5432, "customerdb", "customers")

#     # Show the DataFrame
#     orders.show()
#     customers.show()

#     spark.stop()
