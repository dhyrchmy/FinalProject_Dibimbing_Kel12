# FinalProject_Dibimbing_Kel12
Repository ini berisi program untuk melakukan ETL dan data modelling dari raw data di folder "data".\
\
Pada folder "data" terdapat 26 file raw data dengan format csv, xls, json, avro, dan parquet.
Untuk menjalankan program ETL dan data modelling, lakukan langkah berikut:
1. Masuk ke directory pekerjaan, dan build semua image dengan perintah:
```sh
sudo make build
```
2. Spinup semua container docker dengan perintah:
```sh
sudo make spinup
```
3. Setelah semua container berstatus "up", buka airflow UI di **http://localhost:8081**. Terdapat 2 dag, yaitu dag-etl.py dan dag-sql.py.
4. Untuk melakukan extract semua file dalam folder "data" sekaligus melakukan transform dan load ke database **data warehouse**, trigger dag-etl.py sampai semua task success.
5. Setelah semua task dag-etl.py success, data warehouse di postgreSQL sudah memiliki tabel hasil ETL. Database data warehouse dapat diakses dengan connection berikut:
```sh
DW_POSTGRES_USER=user
DW_POSTGRES_PASSWORD=password
DW_POSTGRES_DB=data_warehouse
DW_POSTGRES_PORT=5433
DW_POSTGRES_CONTAINER_NAME=dataeng-warehouse-postgres
```
6. Selanjutnya, lakukan data modelling dengan PostgresOperator dalam airflow. Sebelumnya, buat koneksi pada airflow dengan cara:\
   a. Tekan "Admin" pada airflow UI\
   b. Tekan "Connection"\ 
   c. Buat connection baru\
   d. Isi connection dengan:
```sh
Connection Id = postgres-fp-12
Connection Type = Postgres
Host = dataeng-warehouse-postgres
Schema = data_warehouse
Login = user
Port =  5432
```
   e. Save connection. Pastikan connection postgres-fp-12 sudah ada dalam list connection.\
   
8. Trigger dag-sql.py untuk menjalankan task data modelling, yaitu membuat dimension table dan fact table dan tunggu sampai semua task success.
9. Lakukan visualisasi data dan pembuatan dashboard di metabase dengan mengakses **http://localhost:3001** dengan user sebagai berikut:
```sh
METABASE_USER_EMAIL=admin@admin.com
METABASE_USER_PASSWORD=superadmin123
```
   Tabel-tabel hasil ETL dan data modelling sudah terbuat di dalam metabase.
