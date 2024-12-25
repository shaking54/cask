import psycopg2

POSTGRES_HOST="localhost"
POSTGRES_PORT=5432
POSTGRES_DB="customerdb"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"

class customer_DBHelper:
    def __init__(self):
        self.connection = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        self.cursor = self.connection.cursor()
    
    def insert_customer(self, customer):
        query = """
            INSERT INTO customers (customer_id, customer_name, email, dob, region)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, customer)
        self.connection.commit()
    
    def close(self):
        self.cursor.close()
        self.connection.close()
    

if __name__ == "__main__":
    db = customer_DBHelper()
    db.close()