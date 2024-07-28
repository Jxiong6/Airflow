from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from tempfile import NamedTemporaryFile


import csv
import logging

default_args = {
    'owner': 'jiahui',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3(ds_nodash,next_ds_nodash):
    # step1: query data from postgresql db and save into text file
    
    # the conn_id can be found in the 'Connections' page in the Web UI
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    # connect to the db
    conn = hook.get_conn()
    # use cursor to execute our query
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= %s and date < %s",
                  (ds_nodash,next_ds_nodash) )
    # return the data in a txt file
    with NamedTemporaryFile(mode='w', suffix=f"{ds_nodash}") as f:
    # with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
        f.flush()
    # close the cursor and conn
        cursor.close()
        conn.close()
    # logs
        logging.info("Saved orders data in text file : %s", f"dags/get_orders_{ds_nodash}.txt")
    # step2: upload text file into s3
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        s3_hook.load_file(
            # call the name attribute of the file object as filename
            filename=f.name,
            key=f"orders/{ds_nodash}.txt",
            bucket_name="airflow",
            replace=True
        )
        # replace the filename to the temporary file name which is f.name
        logging.info("Orders file %s has been pushed to S3!", f.name)
    
with DAG(
    dag_id='dag_with_postgres_hook_v04',
    default_args=default_args,
    start_date=datetime(2024, 7,22),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
    task1