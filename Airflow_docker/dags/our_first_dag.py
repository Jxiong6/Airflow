from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

# we would also like to define the common parameters which will be used to initialise the operator in default_args
default_args = {
    #set the owner of the dag 
    'owner':'code2j',
    #set the maximum time of retries 
    'retries':5,
    #retry delay will be 2 minutes
    'retry_delay':timedelta(minutes=2)
}

# create an instance of DAG using the with statement
with DAG(
    dag_id='our_first_dag_v5',
    #set the dag's default args equals to our defined default args dictionary variable.
    default_args=default_args,
    description='This is our first dag that we write',
    # when we want to start our dag and how often we want to execute it 
    start_date=datetime(2024,7,25,2),
    schedule_interval='@daily'
) as dag:
    #create a simple task
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo  hey, I am task2 and will be running after task1!"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo hey, I am task3 and will be running after task1 at the same time as task2!"
    )
    # build tasks dependency

    #tasks dependency method1:
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    #tasks dependency method2:
    # task1 >> task2
    # task1 >> task3

    #tasks dependency method3:
    task1 >> [task2, task3]
