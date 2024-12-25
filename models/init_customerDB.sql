\c customerdb;

CREATE TABLE IF NOT EXISTS public.customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    dob TIMESTAMP,
    region VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS public.integratedorders (
    customer_id VARCHAR(255) REFERENCES public.customers(customer_id),
    customer_name VARCHAR(255),
    region VARCHAR(255),
    order_id VARCHAR(255),
    order_date TIMESTAMP,  -- We'll use STRING for date, but you can change this if you want more strict date handling
    total_amount FLOAT
);