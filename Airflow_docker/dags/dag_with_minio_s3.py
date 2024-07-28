from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner':'code2j',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

with DAG(
    dag_id='dag_with_minio_s3_v002',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily',
    default_args=default_args
)as dag:
    # create a task using the s3 sensor operator
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn',
        #change the poke_interval and timeout
        mode='poke',
        poke_interval=5,
        timeout=30
    )
    task1
