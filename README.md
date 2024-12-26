## Pipeline Workflow

- **Extract**:
  - Connect to `CustomerDB` and pull the required customer data.
  - Load the `Orders.csv` from HDFS.
  
- **Transform**:
  - Use Apache Spark for processing:
    - Combine the customer data and order data.
    - Apply transformations: 
        - Convert datetime formats to YYYY-MM-DD.
        - Calculate the total order amount.
  
- **Load**:
  - Save the final integrated data into the `IntegratedOrders` database for reporting and analytics.
  
- **Scheduling**:
  - Airflow triggers this pipeline once daily.

## Prerequisites

1. **Apache Spark**:
   - Ensure that Spark is correctly installed and configured.
   
2. **Airflow**:
   - Airflow should be set up for task scheduling and workflow orchestration.
   - Set up a daily schedule to trigger the pipeline.
   
3. **Hadoop (HDFS)**:
   - The `Orders.csv` file should be present in the Hadoop Distributed File System (HDFS).

4. **Database Connection**:
   - Ensure that the `CustomerDB` and `IntegratedOrders` databases are accessible.
   
## Requirements

- Apache Spark 3.0+
- Airflow 2.0+
- Hadoop 3.0+ (for HDFS)
- Python 3.x
- PySpark


![alt text](images/architechture.png "Architecture")

The pipeline is scheduled to run every hour and the data is processed and stored in the data lake.

## DEPLOYMENT

I have tested the pipeline in local and the etl have worked fine. However, I have still some bugs in docker environments.

Some Images of the pipeline:


![alt text](images/IntegrateOrders.png "Integrated Orders")

![alt text](images/airflow.png "Orders")

## Some Code:

#### Extract Function

```python
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
```

#### Transform Function

```python
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
```

#### Load function
```python
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

```

### AIRFLOW

```bash
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```