import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# Constants
NUM_CUSTOMERS = 100
NUM_ORDERS = 300

# Generate Customers
def generate_customers(num_customers):
    customers = []
    for _ in range(num_customers):
        customer_id = fake.uuid4()[:8]
        name = fake.name()
        email = fake.email()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        region = fake.country()
        customers.append([customer_id, name, email, dob, region])
    return pd.DataFrame(customers, columns=["customer_id", "name", "email", "dob", "region"])

# Generate Accounts
def generate_orders(customers, num_accounts):
    orders = []
    for _ in range(num_accounts):
        order_id = fake.uuid4()[:8]
        customer_id = random.choice(customers["customer_id"])
        order_status = random.choice(["pending", "completed", "cancelled"])
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        amount = round(random.uniform(100, 10000), 2)
        orders.append([order_id, customer_id, order_status, order_date, amount])
    return pd.DataFrame(orders, columns=["order_id", "customer_id", "order_status", "order_date", "amount"])

# Generate Data
customers = generate_customers(NUM_CUSTOMERS)
orders = generate_orders(customers, NUM_ORDERS)

# Save to CSV
customers.to_csv("data/raw/customers.csv", index=False)
orders.to_csv("data/raw/orders.csv", index=False)

print("Synthetic data generated and saved to 'data/' directory.")
