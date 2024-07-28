from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

# python function
# ti which is the task instance since xcom_pull can only be called by ti.
def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"hello world! My name is {first_name} {last_name},"
          f" and I am {age} years old.")



def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')


def get_age(ti):
    ti.xcom_push(key='age', value=19)

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v06',
    description='our first dag using python operator',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
        # In PythonOperator, there is a parameter called op_kwars arguments that
        # will get unpacked in the python function
        # op_kwargs={'age':20}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task3 =PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task3,task2] >> task1