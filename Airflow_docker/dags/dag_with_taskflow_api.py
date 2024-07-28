from datetime import datetime,  timedelta


from airflow.decorators import dag, task


default_args ={
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}


@dag(dag_id='dag_with_taskflow_api_v02',
     default_args=default_args,
     start_date=datetime(2024,7,25),
     schedule_interval='@daily')
def hello_world_etl():
    

# change the paramter
    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name':'Jerry',
            'last_name':'Fridman'
        }
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World! My name is {first_name} {last_name} "
              f"and I am {age} years old!")

    # The Taskflow API will automatically calculate the dependency
    # what we need to do is to call these 
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
           age=age)



greet_dag = hello_world_etl()