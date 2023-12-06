# Step 1: Importing Modules
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

# Step 2: Creating DAG Object
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='DAG-2',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

create_dim_table_sql = '''
-- 1. dimension Product, Product Category, Supplier
CREATE TABLE dim_product_supplier AS
SELECT
    p.id AS product_id,
    p.name AS product_name,
    p.price,
    p.category_id,
    p.supplier_id AS product_supplier_id,  -- Unique alias for the conflicting column
    pc.id AS product_category_id,
    pc.name AS product_category_name,
    s.id AS supplier_id,
    s.name AS supplier_name,
    s.country
FROM
    products AS p
JOIN
    product_categories pc ON p.category_id = pc.id
JOIN
    suppliers s ON p.supplier_id = s.id;
--select * from dim_product_supplier; 
--drop table dim_product_supplier;

-- 2. dimension dari Order, OrderItem, Coupons
CREATE TABLE dim_order_coupon AS
SELECT 
    o.id AS order_id,
    o.customer_id,
    o.status,
    o.created_at,
    oi.id AS order_item_id,
    oi.order_id AS oi_order_id,
    oi.product_id,
    oi.amount,
    oi.coupon_id AS order_item_coupon_id,
    c.id AS coupon_id,
    c.discount_percent
FROM
    orders AS o
JOIN
    order_items oi ON o.id = oi.order_id
JOIN 
    coupons c ON oi.coupon_id = c.id;
--select * from dim_order_coupon;
--drop table dim_order_coupon;

-- 3. dimension "Orders"
CREATE TABLE dim_orders AS
SELECT
    o.id AS order_id,
    o.customer_id,
    o.status,
    o.created_at 
FROM
    orders AS o;
--select * from dim_orders;
--drop table dim_orders;

-- 4. dimension "Customers" & login_attempt_history
CREATE TABLE dim_customers_login AS
SELECT
    c.id AS customer_id,
    c.first_name,
    c.last_name,
    c.address,
    c.gender,
    c.zip_code,
    la.id AS login_attempt_history_id,
    la.login_successful,
    la.attempted_at,
    case 
    	WHEN EXTRACT(HOUR FROM attempted_at) >= 0 AND EXTRACT(HOUR FROM attempted_at) < 6 THEN 'Dini Hari'
    	WHEN EXTRACT(HOUR FROM attempted_at) >= 6 AND EXTRACT(HOUR FROM attempted_at) < 12 THEN 'Pagi'
    	WHEN EXTRACT(HOUR FROM attempted_at) >= 12 AND EXTRACT(HOUR FROM attempted_at) < 18 THEN 'Siang-Sore'
    	WHEN EXTRACT(HOUR FROM attempted_at) >= 18 AND EXTRACT(HOUR FROM attempted_at) <= 23 THEN 'Malam'
    	ELSE 'Unknown'
	end as login_time_category
FROM
    customers AS c
JOIN
    login_attempt_history la ON c.id = la.customer_id;
 
--select * from dim_customers_login;
--drop table dim_customers_login;
'''

create_fact_table_sql ='''
-- 1. Fact Table Login History
create table fact_login_history as
select
    login_time_category,
    COUNT(login_time_category)
FROM
    dim_customers_login
GROUP by login_time_category;

--select * from fact_login_history;
--drop table fact_login_history;

-- 2. Fact Table Order Items 
CREATE TABLE fact_subtotal_diskon AS
SELECT
    oc.order_item_id,
    cl.customer_id,
    oc.order_id,
    ps.product_id,
    oc.amount,
    oc.coupon_id,
    ps.price AS product_price,
    oc.discount_percent AS coupon_discount_percent,
    oc.amount * ps.price * (1 - (oc.discount_percent * 0.01)) AS subtotal
FROM
    dim_product_supplier ps
JOIN
    dim_order_coupon oc ON ps.product_id = oc.product_id
JOIN
    dim_customers_login cl ON oc.customer_id = cl.customer_id;   
--select * from fact_subtotal_diskon;
--drop table fact_subtotal_diskon;


-- 3. Fact Table Supplier based on Product Category
create table fact_supplier_product_category as
select
	product_category_name,
	COUNT(distinct product_supplier_id) AS supplier_count
from 
	dim_product_supplier
GROUP by 1;

--select * from fact_supplier_product_category;
--drop table fact_supplier_product_category;
'''

# Step 3: Creating Tasks
task_1 = PostgresOperator(
    task_id='task_1',
    sql=create_dim_table_sql,
    postgres_conn_id='postgres-fp-12',
    dag=dag
)

task_2 = PostgresOperator(
    task_id='task_2',
    sql=create_fact_table_sql,
    postgres_conn_id='postgres-fp-12',
    dag=dag
)

# Step 4: Setting up Dependencies
task_1 >> task_2
