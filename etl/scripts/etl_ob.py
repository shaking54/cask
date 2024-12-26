import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, from_unixtime, to_date, col  
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.types import DateType


class ETL:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger("ETL")

    def get_from_csv(self, input_path: str):
        self.logger.info(f"Reading CSV from: {input_path}")
        return self.spark.read.csv(input_path, header=True)

    def get_from_db(self, db_host, db_port, db_name, table):
        self.logger.info(f"Reading table '{table}' from database at {db_host}:{db_port}/{db_name}")
        return self.spark.read.format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", f"jdbc:postgresql://{db_host}:{db_port}/{db_name}") \
            .option("dbtable", table) \
            .option("user", self.config["db_user"]) \
            .option("password", self.config["db_password"]) \
            .option("inferSchema", "true") \
            .load()

    def write_to_db(self, dataframe, table, mode="append"):
        db_url = f"jdbc:postgresql://{self.config['db_host']}:{self.config['db_port']}/{self.config['db_name']}"
        db_properties = {
            "user": self.config["db_user"],
            "password": self.config["db_password"],
            "driver": "org.postgresql.Driver"
        }
        self.logger.info(f"Writing data to table '{table}'")
        dataframe.write.jdbc(db_url, table, mode=mode, properties=db_properties)

    def extract(self, input_path, db_host="customerDB", db_port=5432, db_name="customerdb", table="customers"):
        orders = self.get_from_csv(input_path)
        customers = self.get_from_db(db_host, db_port, db_name, table)
        return orders, customers
    
    def transform(self, orders, customers):
        
        orders = orders.dropDuplicates()
        customers = customers.dropDuplicates()
        
        orders = orders.withColumn("order_date", orders["order_date"].cast("timestamp"))
        customers = customers.withColumn("dob", customers["dob"].cast("timestamp"))

        orders = orders.withColumn("order_date", date_format("order_date", "yyyy-MM-dd"))
        customers = customers.withColumn("dob", date_format("dob", "yyyy-MM-dd"))

        orders.show()
        customers.show()

        return orders, customers
    
    def load(self, customers, orders, table):
        star_df = customers.join(orders, customers.customer_id == orders.customer_id, "inner") \
                            .drop(orders.customer_id)
        star_df = star_df.withColumn('total_amount', f.sum('amount').over(Window.partitionBy('customer_id')))
        star_df = star_df.withColumn("order_date", col("order_date").cast(DateType()))
        columns = ["customer_id", "customer_name", "region", "order_id", "order_date", "total_amount"]
        star_df = star_df.select(columns)
        self.write_to_db(star_df, table)
        return star_df
    
    def read_json(self, object):
        return self.spark.read.json(self.spark.sparkContext.parallelize([object]))