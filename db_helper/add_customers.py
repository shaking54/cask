from customer_helper import customer_DBHelper
import pandas as pd

POSTGRES_HOST="localhost"
POSTGRES_PORT=5432
POSTGRES_DB="customerdb"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"

def add_customers(customers):
    db = customer_DBHelper()
    for customer in customers:
        db.insert_customer(customer)
        print(f"Added customer: {customer[1]}")
    db.close()

if __name__ == "__main__":
    customers = pd.read_csv("data/raw/customers.csv")
    add_customers(customers.values)
