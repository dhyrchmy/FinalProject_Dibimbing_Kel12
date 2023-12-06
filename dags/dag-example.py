# Step 1: Importing Modules
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import fastavro
import psycopg2
from os import listdir
from os.path import isfile, join
from sqlalchemy import create_engine

# Step 2: Extract Function
def extract ():
  path ="/opt/airflow/data"
  onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

  # now do the for loop
  for  i in onlyfiles:
    #customer (csv)
    if ".csv" in i and "customer" in i:
      df_customer = pd.DataFrame()
      for j in range(10):
        df = pd.read_csv(path+"/"+i)
        df_customer = pd.concat([df_customer,df])

    #product dan product category (xls)
    if ".xls" in i and "product" in i:
      if "product_category" in i:
        df_product_category = pd.read_excel(path+"/"+i)
      else:
        df_product = pd.read_excel(path+"/"+i)

    #supplier (xls)
    if ".xls" in i and "supplier" in i:
      df_supplier = pd.read_excel(path+"/"+i)

    #login attempts (json)
    if ".json" in i and "login_attempts" in i:
      df_login = pd.DataFrame()
      for j in range(10):
        df = pd.read_json(path+"/"+i)
        df_login = pd.concat([df_login,df])

    #coupons (json)
    if ".json" in i and "coupons" in i:
      df_coupons = pd.read_json(path+"/"+i)

    #order (parquet)
    if ".parquet" in i and "order" in i:
      df_order = pd.read_parquet(path+"/"+i, engine='pyarrow')

    #order_ item (avro)
    if ".avro" in i and "order_item" in i:
      order_item_list = []
      with open(path+"/"+"order_item.avro", "rb") as f:
          reader = fastavro.reader(f)
          for record in reader:
              order_item_list.append(record)
      df_order_item = pd.DataFrame(order_item_list)

  return [df_customer, df_product, df_product_category, df_supplier, df_login, df_coupons, df_order, df_order_item]

# Step 3: Load Function
def load(*args, **kwargs):
    connection = psycopg2.connect(
        host="dataeng-warehouse-postgres",
        port=5432,
        dbname="data_warehouse",
        user="user",
        password="password"
    )

    cursor = connection.cursor()
    list_tabel = extract()
    tabel = {}
    tabel['customers'] = list_tabel[0]
    tabel['products'] = list_tabel[1]
    tabel['product_categories'] = list_tabel[2]
    tabel['suppliers'] = list_tabel[3]
    tabel['login_attempt_history'] = list_tabel[4]
    tabel['coupons'] = list_tabel[5]
    tabel['orders'] = list_tabel[6]
    tabel['order_items'] = list_tabel[7]

    engine = create_engine('postgresql://user:password@dataeng-warehouse-postgres:5432/data_warehouse')

    for nama_tabel, df_tabel in tabel.items():
      df_tabel.to_sql(nama_tabel, engine)
    cursor.close()
    connection.commit()
    connection.close()
# Step 4: Creating DAG Object
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 11, 12),
}

dag = DAG(
    dag_id='DAG-1',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
)

# Step 5: Creating Tasks
task_1 = PythonOperator(
    task_id='task_1',
    python_callable=extract,
    provide_context=True,
    dag=dag
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=load,
    provide_context=True,
    dag=dag
)

# Step 6: Setting up Dependencies
task_1 >> task_2
