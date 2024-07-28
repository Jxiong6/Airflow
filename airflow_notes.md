# Airflow 

[TOC]



## Airflow Introduction



First of all, let's create a Python project and install airflow.

1) open vscode 

2) create a project folder , name it airflow_tutorial 

3) airflow 2.0 require a python version above 3.6 

4) conda create --all airflow_env 



##### install airflow  locally 

```bash
conda activate airflow_env 
```



``` bash
pip install 'apache-airflow==2.9.3' 
 --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.8.txt"
```

``` bash 
export AIRFLOW_HOME =. 
```



##### run airflow in docker 



###### Fetching docker-compose.yaml

``` bash 
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.3/docker-compose.yaml'
```



```yam
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Basic Airflow cluster configuration for CeleryExecutor with Redis and PostgreSQL.
#
# WARNING: This configuration is for local development. Do not use it in a production deployment.
#
# This configuration supports basic configuration using environment variables or an .env file
# The following variables are supported:
#
# AIRFLOW_IMAGE_NAME           - Docker image name used to run Airflow.
#                                Default: apache/airflow:2.7.3
# AIRFLOW_UID                  - User ID in Airflow containers
#                                Default: 50000
# AIRFLOW_PROJ_DIR             - Base path to which all the files will be volumed.
#                                Default: .
# Those configurations are useful mostly in case of standalone testing/running Airflow in test/try-out mode
#
# _AIRFLOW_WWW_USER_USERNAME   - Username for the administrator account (if requested).
#                                Default: airflow
# _AIRFLOW_WWW_USER_PASSWORD   - Password for the administrator account (if requested).
#                                Default: airflow
# _PIP_ADDITIONAL_REQUIREMENTS - Additional PIP requirements to add when starting all containers.
#                                Use this option ONLY for quick checks. Installing requirements at container
#                                startup is done EVERY TIME the service is started.
#                                A better way is to build a custom image or extend the official image
#                                as described in https://airflow.apache.org/docs/docker-stack/build.html.
#                                Default: ''
#
# Feel free to modify this file to suit your needs.
---
version: '3.8'
x-airflow-common:
  &airflow-common
  # In order to add custom dependencies or upgrade provider packages you can use your extended image.
  # Comment the image line, place your Dockerfile in the directory where you placed the docker-compose.yaml
  # and uncomment the "build" line below, Then run `docker-compose build` to build the images.
  image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.7.3}
  # build: .
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    # For backward compatibility, with Airflow <2.3
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
    AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'
    # yamllint disable rule:line-length
    # Use simple http server on scheduler for health checks
    # See https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/check-health.html#scheduler-health-check-server
    # yamllint enable rule:line-length
    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
    # WARNING: Use _PIP_ADDITIONAL_REQUIREMENTS option ONLY for a quick checks
    # for other purpose (development, test and especially production usage) build/extend Airflow image.
    _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
  volumes:
    - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
    - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
    - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
    - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  user: "${AIRFLOW_UID:-50000}:0"
  depends_on:
    &airflow-common-depends-on
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always



  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-scheduler:
    <<: *airflow-common
    command: scheduler
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8974/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully



  airflow-triggerer:
    <<: *airflow-common
    command: triggerer
    healthcheck:
      test: ["CMD-SHELL", 'airflow jobs check --job-type TriggererJob --hostname "$${HOSTNAME}"']
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 30s
    restart: always
    depends_on:
      <<: *airflow-common-depends-on
      airflow-init:
        condition: service_completed_successfully

  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    # yamllint disable rule:line-length
    command:
      - -c
      - |
        function ver() {
          printf "%04d%04d%04d%04d" $${1//./ }
        }
        airflow_version=$$(AIRFLOW__LOGGING__LOGGING_LEVEL=INFO && gosu airflow airflow version)
        airflow_version_comparable=$$(ver $${airflow_version})
        min_airflow_version=2.2.0
        min_airflow_version_comparable=$$(ver $${min_airflow_version})
        if (( airflow_version_comparable < min_airflow_version_comparable )); then
          echo
          echo -e "\033[1;31mERROR!!!: Too old Airflow version $${airflow_version}!\e[0m"
          echo "The minimum Airflow version supported: $${min_airflow_version}. Only use this or higher!"
          echo
          exit 1
        fi
        if [[ -z "${AIRFLOW_UID}" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
          echo "If you are on Linux, you SHOULD follow the instructions below to set "
          echo "AIRFLOW_UID environment variable, otherwise files will be owned by root."
          echo "For other operating systems you can get rid of the warning with manually created .env file:"
          echo "    See: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#setting-the-right-airflow-user"
          echo
        fi
        one_meg=1048576
        mem_available=$$(($$(getconf _PHYS_PAGES) * $$(getconf PAGE_SIZE) / one_meg))
        cpus_available=$$(grep -cE 'cpu[0-9]+' /proc/stat)
        disk_available=$$(df / | tail -1 | awk '{print $$4}')
        warning_resources="false"
        if (( mem_available < 4000 )) ; then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
          echo "At least 4GB of memory required. You have $$(numfmt --to iec $$((mem_available * one_meg)))"
          echo
          warning_resources="true"
        fi
        if (( cpus_available < 2 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
          echo "At least 2 CPUs recommended. You have $${cpus_available}"
          echo
          warning_resources="true"
        fi
        if (( disk_available < one_meg * 10 )); then
          echo
          echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
          echo "At least 10 GBs recommended. You have $$(numfmt --to iec $$((disk_available * 1024 )))"
          echo
          warning_resources="true"
        fi
        if [[ $${warning_resources} == "true" ]]; then
          echo
          echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow (see above)!\e[0m"
          echo "Please follow the instructions to increase amount of resources available:"
          echo "   https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#before-you-begin"
          echo
        fi
        mkdir -p /sources/logs /sources/dags /sources/plugins
        chown -R "${AIRFLOW_UID}:0" /sources/{logs,dags,plugins}
        exec /entrypoint airflow version
    # yamllint enable rule:line-length
    environment:
      <<: *airflow-common-env
      _AIRFLOW_DB_MIGRATE: 'true'
      _AIRFLOW_WWW_USER_CREATE: 'true'
      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-airflow}
      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-airflow}
      _PIP_ADDITIONAL_REQUIREMENTS: ''
    user: "0:0"
    volumes:
      - ${AIRFLOW_PROJ_DIR:-.}:/sources

  airflow-cli:
    <<: *airflow-common
    profiles:
      - debug
    environment:
      <<: *airflow-common-env
      CONNECTION_CHECK_MAX_COUNT: "0"
    # Workaround for entrypoint issue. See: https://github.com/apache/airflow/issues/16252
    command:
      - bash
      - -c
      - airflow

  # You can enable flower by adding "--profile flower" option e.g. docker-compose --profile flower up
  # or by explicitly targeted on the command line e.g. docker-compose up flower.
  # See: https://docs.docker.com/compose/profiles/


volumes:
  postgres-db-volume:

```

###### Initializing Env

``` bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env // run this when you are using Linux os 
```

###### Initializing the database

```bash 
docker-compose up airflow-init 
```

###### Running airfow 

```bash 
docker-compose up -d 
```

###### Check the airflow webserver

```bash
localhost:8080
```

default username and password: airflow 









## Airflow Core Concept



### What is Airflow ?

- Starts in Airbnb 2014
- Manage complex workflows
- Workflow management platform 
- written in python 

### What is workflow?

- Workflow is a sequence of tasks. In airflow, workflow is defined as DAG, namely directed acyclic graph.

### What is the task, operator?

- A task defines a unit of work within a DAG;  it is represented as a node in the DAG graph, and it is written in python. And there is a dependency between tasks.
- The goal os the task is to achieve a specific thing, the method it uses is called operator.
- While DAGs describe how to run a workflow, Operators determine what actually gets done by a task. In Airflow, there are many kindsof operators, like BashOperator, PythonOperator and you can also write your own customized operator. Each task is an implementation of  an operator, for example a PythonOperator to execute some Python code, or a BashOperator to run a Bash command.
- To sum up, the `operator` determines what is going to be done. The `task` implements an operator by defining specific values for that operator. And `dag` is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies.

### What is the execution date, dag run and task instance?

- The execution_date is the logical date and time which the DAG Run.
- A task instance is a run of a specific point of time (excution_date).
- A DAG run is an instantiation of a DAG, containing task instances that run for a specific execution_date.



![image](.\images\1.png)



## Task Lifecycle Basic Architecture



When a DAG run is triggered. Its tasks are going to be executed one after another according to their dependencies. Each task will go through different stages from start to completion. 

### Task Lifecycle 

Every stage indicates a specifie status of the task instance. For example, when the task is in progress. its status is running; when the task has been finished flawlessly, it has a success status and so on.



In the early phase, a task might go through from no_status to queued; after that a task will be at the execution, from running to success; if the task fails it will be directed into up_for_retry, upstream_failed, or up_for_reschedule. And during the task lifecycle, if we manually abort or skip it. The task will be in the status of shutdown or skipped accordingly.

`No status`  

- a task is usually starting with no status, which means the scheduler created an empty task instance; after that there are 4 different stages that the task can be moved to; they are scheduled, removed, upstream failed or skipped.

`Scheduled`

- Scheduled means that the scheduler determined task instance needs to be run

`upstream Failed` 

- Upstream_failed once the task's upstream task failed 

`skipped`

- if the task is skipped; or removed when the task has been removed.

![image](.\images\2.png)





## Airflow Basic Architecture

![image](.\images\3.png)





- first of all , data engineer to start building and monitoring all the ETL processes. Data engineers have to configure the airflow setup, like the type of executor, which database to use, and etc. Data Engineers create and manage the dags they authored through the airflow user interface which is supported by the web server. In addition to that, the dags are also visible to the scheduler and workers, which change the task's status during the whole task lifecycle. Apart from that, there is a component called executor. In order to persist the update and retrieve the info of  the dags, those four components are connected to the datebase closely. There are a variety  of database engines you can choose, like mysql, postgres.

![image](.\images\4.png)



## Airflow DAG with BashOperator



```bash 
docker-compose up -d
```

```bash
docker-compose down -v
```

> note : 
>
> -v means we are not only shutting  down the airflow containers, but also removing the volumes we defined in our docker-compose yaml file. 

Then we change the value of `AIRFLOW__CORE__LOAD_EXAMPLES` from `true` to `false`.

Then we init the airflow by 

```bash
docker-compose up airflow-init 
```

And then we launch airflow again by 

```bash
docker-compose up -d 
```



Next we are going to create our first dag.

In airflow, airflow dag is defined as a python file under 

 the dags folder. Therefore, let's create one called `our_first_dag.py`and open it.

The dag implementation is an instantiation of the class DAG

`our_first_dag.py`

```python
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
    dag_id='our_first_dag',
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

    task1

```

And then we go to `localhost :8080` , triger this dag, and we can see some output in the logs button.



Next, let's  create task2 which will be exectued after the success of task1.

```python 
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
    dag_id='our_first_dag_v2',
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
    # build tasks dependencies
    task1.set_downstream(task2)

```

What if we want to have 3 tasks and task2 and 3 will be run once task1 finishes.

```python
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

```

## Airflow DAG with PythonOperator  

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

# python function
def greet():
    print("hello world!")

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v01',
    description='our first dag using python operator',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task1
```



### How do we pass the python function's parameters using the PythonOperator? 

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

# python function
def greet(name, age):
    print(f"hello world! My name is {name},"
          f" and I am {age} years old.")

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v02',
    description='our first dag using python operator',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # In PythonOperator, there is a parameter called op_kwars arguments that will get unpacked in the python function
        op_kwargs={'name':'Tom', 'age':20}
    )

    task1
```



### Can we share information between different tasks?

- Yes! we can use airflow xcoms. Basically we can push infromation to xcoms in one task and pull information in other tasks. By default, every function's return value will be automatically  pushed into xcom.



```python
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
def greet(age, ti):
    name = ti.xcom_pull(task_ids='get_name')
    print(f"hello world! My name is {name},"
          f" and I am {age} years old.")



def get_name():
    return 'Jerry'


with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v04',
    description='our first dag using python operator',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # In PythonOperator, there is a parameter called op_kwars arguments that
        # will get unpacked in the python function
        op_kwargs={'age':20}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task2 >> task1
```



### What if we want to push multiple values into xcoms in one function, can we distinguish them?



- Yes! 

```python 
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
def greet(age, ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    print(f"hello world! My name is {first_name} {last_name},"
          f" and I am {age} years old.")


#multiple values 
def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Fridman')


with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v05',
    description='our first dag using python operator',
    start_date=datetime(2024,7,24),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet,
        # In PythonOperator, there is a parameter called op_kwars arguments that
        # will get unpacked in the python function
        op_kwargs={'age':20}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )

    task2 >> task1
```



### Let's also update our code to get age via xcoms instead of op_kwargs

```python
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
### new task
    task3 =PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )
    ### new dependency
    [task3,task2] >> task1
```

> note:
>
> Never use xcoms to share large data 



## Airflow TaskFlow API

Let's see how much code we can reduce by rewriting it using the TaskFlow API.

`dag_with_taskflow_api.py`

```python
from datetime import datetime,  timedelta


from airflow.decorators import dag, task


default_args ={
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}


@dag(dag_id='dag_with_taskflow_api_v01',
     default_args=default_args,
     start_date=datetime(2024,7,25),
     schedule_interval='@daily')
def hello_world_etl():
    
    @task()
    def get_name():
        return "Jerry"
    
    @task()
    def get_age():
        return 19
    
    @task()
    def greet(name, age):
        print(f"Hello World! My name is {name} "
              f"and I am {age} years old!")

    # The Taskflow API will automatically calculate the dependency
    # what we need to do is to call these 
    name = get_name()
    age = get_age()
    greet(name=name, age=age)



greet_dag = hello_world_etl()
```

### What if we want to return firstname and lastname instead of name in our get_name task?

```python
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

# change here
    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],
          last_name=name_dict['last_name'],
           age=age)



greet_dag = hello_world_etl()
```



## Airflow Catchup and Backfill

`dag_with_catchup_and_backfill.py`

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_catchup_backfill_v01',
    default_args=default_args,
    start_date=datetime(2024,7,20),
    schedule_interval='@daily',
    # there is a parameter called catchup, by default it is set to True
    catchup=True
) as dag:
    task1 = BashOperator(
        task_id ='task1',
        bash_command='echo This is a simple bash command!'
    )
```

> note : The catchup feature helped us run the dag since the start  date.

### How can we achieve as `catchup`  by using the  ` backfill`?

First we go back to vscode terminal to find the airflow scheduler container by `docker ps`

And then we open its bash interactively in the container by command

`docker exec -it fd9442268648 bash`

To backfill the dag run, we need to execute the  command `airflow dags backfill -s 2021-11-01 -e 2021-11-08 dag_with_catchup_backfill_v02`  

> note: -s : start date , -e : end_date    + dag_id



## Schedule with Corn Expression 

In airflow, creating a dag needs the schedule_interval parameter, which receives a cron expression  as a string, or a datetime.timedelta object. 

![image](.\images\5.png)

### What is Cron Expression?

A Cron Expression is a string comprising five fields separated by white space that represents a set of times, normally as a schedule to execute some routine.

![image](.\images\6.png)

Airflow already provides some presets for the schedule_intereval, like `@daily`,`@hourly`, which is linked to its cron expression string. So if you know how to use cron expressions, you can schedule your dag in any way you want.

![image](.\images\7.png)



`dag_with_cron_expression.py`



``` python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_with_cron_expression_v02',
    default_args=default_args,
    start_date=datetime(2024,7,20),
    #schedule dag daily using the cron expression string
    schedule_interval='0 0 * * *'
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command="echo dag with cron expression!"
    )
    task1
```

### How to generate customized schedule_interval using the cron expression?

There is a website `crontab.guru`  which gives us a visual way to generate and verify the cron expression.

 

## Airflow Connection to Postgres 

Normally when we build an ETL DAG, we need to connect to some external services.

![image](.\images\8.png)

In airflow web server ui, if we mouse hover the `admin`, we can see the connections menu.



## Airflow PostgresOperator

To demonstrate the postgresOperator, we need a postgres database.

 We add the ports 54320:5432 to the postgres services in the docker-compose.yaml file. 

```sql
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    ports:
      - 54320:5432
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      retries: 5
      start_period: 5s
    restart: always
```



Then we recreate the postgres container 

```bash
docker-compose up -d --no-deps --build postgres
```

Let's connect it  using the `dbeaver`, which is an open-source, cross-platform database management tool.

 open it => select database => postgresql



> note :  why we can't  use 5432:5432, because the local postgres is using the port 5432. so we use 54320:5432

![image](.\images\9.png)

![image](.\images\10.png)

After this, we can klick the database and create a new database `test`

Now, Let's use an airflow dag to create a table an insert some values.

Go back to vscode and create `dag_with_postgres_operator.py`

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2024,7,24),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        #use airflow webserver ui to create
        postgres_conn_id= 'postgres_localhost',
        #we create a table 
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    task1
```

> note:

For the `postgres_conn_id`,  we go to the airflow webserver ui to create one.

![image](.\images\11.png)



Let's create another task which inserts the dag id and execution date into the dag_runs table using the PostgresOperator.

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

with DAG(
    dag_id='dag_with_postgres_operator_v02',
    default_args=default_args,
    start_date=datetime(2024,7,24),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        #use airflow webserver ui to create
        postgres_conn_id= 'postgres_localhost',
        #we create a table 
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    # ds is the dag run's execution date and dag_id is the dag_id, which 
    # are set by default by airflow engine and can be accessed by puting the 
    # variable name into two curly brackets.
    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id= 'postgres_localhost',
        sql="""
            insert into dag_runs (dt,dag_id) values ('{{ ds }}','{{ dag.dag_id }}')
        """
    )

    task1 >> task2
```

And then we can verify it using the dbeaver, 

![image](.\images\12.png)

In airflow, it's recommended to delete data before insert to avoid data duplication or primary key violation.

For example, if we cleared one of the successful insert tasks, it tries t insert data which already exists in the table. In the end, it will fail since it violates the primary key constraint. 

So let's fix this by adding a delete task before insert.

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)

}

with DAG(
    dag_id='dag_with_postgres_operator_v03',
    default_args=default_args,
    start_date=datetime(2024,7,24),
    schedule_interval='0 0 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id='create_postgres_table',
        #use airflow webserver ui to create
        postgres_conn_id= 'postgres_localhost',
        #we create a table 
        sql="""
            create table if not exists dag_runs(
                dt date,
                dag_id character varying,
                primary key (dt, dag_id)
            )
        """
    )
    # ds is the dag run's execution date and dag_id is the dag_id, which 
    # are set by default by airflow engine and can be accessed by puting the 
    # variable name into two curly brackets.
    task2 = PostgresOperator(
        task_id='insert_into_table',
        postgres_conn_id= 'postgres_localhost',
        sql="""
            insert into dag_runs (dt,dag_id) values ('{{ ds }}','{{ dag.dag_id }}')
        """
    )

    task3 = PostgresOperator(
        task_id='delete_data_from_table',
        postgres_conn_id= 'postgres_localhost',
        sql="""
            delete from dag_runs where dt = '{{ ds }}' and dag_id = '{{ dag.dag_id }}';

        """
    )

    task1 >> task3 >> task2
```

## Airflow Docker Install Python Package

Two ways to install python dependencies to your aiflow docker container

In general, you can either extend or customize the airflow docker image to install your python dependencies.

![image](.\images\13.png)



### Extend the airflow docker image

First, go to Airflow_docker file and create the requirements text file.

```txt
scikit-learn==0.24.2
```



Next we are going to install all the dependencies defined in the requirement file by extending the airflow image. To do this, we have to create a Dockerfile in our project root folder and open it .

```dockerfile
FROM apache/airflow:2.0.1
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip 
RUN pip install --no-cache-dir --user -r /requirements.txt
```

And let's build the extended image by command 

```bash
docker build . --tag extending_airflow:latest
```

Once it finishes, we need to open our `docker-compose.yaml` file to change the image name from `apache/airflow` to `extending_airflow:latest` 



Let's create a dag file to verife whether the `scikit-learn` package has been installed.

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner':'code2j',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def get_sklearn():
    import sklearn
    print(f"scikit_learn with version: {sklearn.__version__}")

with DAG(
    default_args=default_args,
    dag_id='dag_with_python_dependencies_v01',
    start_date=datetime(2024,7,25),
    schedule_interval='@daily'
) as dag:
    
    get_sklearn =PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )

    get_sklearn
```

and do it by the command :

```bash
docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
```

#### What if we want to add more python dependencies?

We go back vscode, add `matplotlib` with version to the requirements text file.

```python
scikit-learn==0.24.2
matplotlib==3.3.3
```

```python
docker build . --tag extending_airflow:latest
```

```python
docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
```

### Extending or Customising？

we can go with the first method (image extending) in 99% of the case. 



## Airflow AWS S3 Sensor

### What is Sensor?

- a special type of operator which waits for something to occur. It is perfect tool for use cases in which you don't know exactly when the data will available.  For example, your client will upload a csv file to the AWS S3 bucket daily but can be at any time in the day. You basically need to check whether it is uploaded at a certain time interval before starting other tasks like downloading or cleaning.

In this course, we will use `Minio`, which is API compatible with AWS S3 cloud storage service.

### Run Minio on Docker

Copy the command 

```bash 
docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   --name minio \
   -v ~/minio/data:/data \
   -e "MINIO_ROOT_USER=ROOTNAME" \
   -e "MINIO_ROOT_PASSWORD=CHANGEME123" \
   quay.io/minio/minio server /data --console-address ":9001"
```

Go back to vscode, create a new terminal, paste the command.

>  note :The docker command basically runs a MinIO docker image, exposing two ports 9000 and 9001. And set up the root username and password. 9000 port is for the API, 9001 is  for the console.



Copy the console addre, open it in the browser and login.

```bash
http://127.0.0.1:9001
```

Once we logged in, let's create one s3 bucket by clicking the `create bucket` button.  Make sure we have the read and write permission.

![image](.\images\14.png)

The we click the browser button, here we can either create a path or upload a file.

<img src=".\images\15.png" alt="image" style="zoom:60%;" />

Let's go back to vscode to generate a CSV file which will be uploaded later to our aiflow bucket. First create a folder called `data` in our root project directory, then we  create a csv file called  `data.csv` with two columns `product_id`, `delivery_dt` with some rows for demonstration.  Save it (by clicking the data.csv and choose  reveal..) and go back to the browser to drag the file from the folder to our airflow bucket.

Now we have a proper S3 bucket set up, Let's start building an airflow dag to connect to our S3 bucket and sense the file's exiistence. 

- we first create a python file called `dag_with_minio_s3.py`

- And we make sure which version of the amazon airflow provider package we installed. Let's open our docker-compose yaml file  to change the airflow image back to the latest extending_airflow or offical apache-airflow version 2.0.1

  ```bash
  image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}
  ```

  and run the command 

  ```bash
  docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler
  ```

- Then we find and copy the container id of the `airflow scheduler` by command: `docker ps`

- After that we enter into the airflow scheduler container by command

  ```bash
  docker exec -it 0608c266e97f bash 
  ```

- Let's run command `pip list | grep amazon`, and the output shows we have an amazon airflow provider package with version 8.10.0. Let's go to the browser and search for the apache airflow official documentation site. click the amazon, and choose the version `8.10.0` . Then we check the `Python API` look for sensor S3`airflow.providers.amazon.aws.sensors.s3`. To use it we need a couple of parameters like  bucket_name, bucket_key, aws_conn_id,etc.

   <img src=".\images\16.png" alt="image" style="zoom:60%;" />

Let's copy the package directory and go back to vscode to paste it.

<img src=".\images\17.png" alt="image" style="zoom:60%;" />

For the `aws_conn_id`, we go to the airflow ui, from the admin, connections, click the plus button. 

<img src=".\images\18.png" alt="image" style="zoom:60%;" />

> note: the first two line is the name and password we created in the Minio
>
> <img src=".\images\20.png" alt="image" style="zoom:60%;" />
>
>   and the last line is the output after we run the minio docker command , for the API:9000. And we change it to `host.docker.internal:9000`

``` python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner':'code2j',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

with DAG(
    dag_id='dag_with_minio_s3_v01',
    start_date=datetime(2024,7,26),
    schedule_interval='@daily',
    default_args=default_args
)as dag:
    # create a task using the s3 sensor operator
    task1 = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name='airflow',
        bucket_key='data.csv',
        aws_conn_id='minio_conn'
    )

```

Run the dag and you might got error like:

```
botocore.exceptions.ClientError: An error occurred (403) when calling the HeadObject operation: Forbidden
```



To solve this, first check the network of the MinIO and Airflow containers.

```
docker inspect --format '{{.NetworkSettings.Networks}}' <container-id>
```

I checked and got this output:

```bash
#MinIo
$ docker inspect --format '{{.NetworkSettings.Networks}}' <MinIo container id>
map[bridge:0xc00036a600]
#Airflow

$ docker inspect --format '{{.NetworkSettings.Networks}}' <docker container id >
map[airflow_docker_default:0xc000456180]

```

We can see that MinIO is running on the bridge network while airflow is running on its user defined network. So the 2 services cannot communicate since they are not in the same network. Let's connect MinIO to the `airflow_docker_default` network.

```bash
# MinIO container_id
$ docker network connect airflow_docker_default <minio container id >

$ docker inspect --format '{{.NetworkSettings.Networks}}' <minio container id >

map[airflow_docker_default:0xc00036a3c0 bridge:0xc00036a480]
(base) 
```

And we can run it successfully.

```bash
[2024-07-27, 19:27:54 CEST] {s3.py:106} INFO - Poking for key : s3://***/data.csv
```

From the log, we can see it was checking the data.csv file existence in our airflow s3 bucket using mode poking. `Poke` is the default mode for sensor operators. Basically, it checks the file's existence at every poke_interval as long as it is within the timeout limit. Since the data.csv already exists in the airflow bucket, it finishes immediately after the firsting poking and is marked as success.

```bash
[2024-07-27, 19:27:55 CEST] {base.py:287} INFO - Success criteria met. Exiting.
```

We go back to vscode change the poke_interval and timeout. And delect the data.csv from our airflow bucket.

```bash 
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner':'code2j',
    'retries':5,
    'retry_delay':timedelta(minutes=10)
}

with DAG(
    dag_id='dag_with_minio_s3_v02',
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

```

Every 5seconds, it will poke the file until out and fails. Because it didn't find the data.csv file within the 30 seconds timeout limit in the airflow bucket. What if the data.csv is uploaded during the poking?  - it will success after we re-upload our data.csv to the airflow bucket.

## Airflow Hooks S3 PostgreSQL

We download the Order.csv to the data direction.

We need to first import it into our postgres database, then write a DAG to query data from it and upload it to the s3 bucket.

Let's open dbeaver, connect to our postgres database. And we are going to create a table called `orders` in the database test1.

Right click the `test` and set as the default database. Then create a table called `orders` using the statement:

```sql
create table if not exists public.orders (
	order_id character varying,
  date date,
  product_name character varying,
  quantity integer,
  primary key (order_id)
)
```

Refresh the `test` database, we can see the `orders` table. Then we right click the table and import our `Orders.csv` file. We can see that the data has been imported successfully.

Let's query the first 100 rows for double check.

```sql
select * from orders o limit 100
```

Let's go back to vscode, create a python file called `dag_with_postgres_hook.py` 

To query the data from postgres db, we need the **postgres hook**. Let's check the version of the installed postgres package.

```bash
docker ps

# enter the docker container
docker exec -it <scheduler container id> bash

# check the version of amazon provider
pip list | grep postgres
```

```bash
apache-airflow-providers-postgres        5.7.1
```

Let's go to the airflow documentation, search and click the postgresql in the providers packages section. Select a version to match our local installation. Then click the python api. Go to the [hook](https://airflow.apache.org/docs/apache-airflow-providers-postgres/5.7.1/_api/airflow/providers/postgres/hooks/postgres/index.html) subpackages.

Hook is used to interact with Postgres DB and we can establish a connection to a postgres database via the `get_conn` function.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
```



```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import logging

default_args = {
    'owner': 'jiahui',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3():
    # step1: query data from postgresql db and save into text file
    
    # the conn_id can be found in the 'Connections' page in the Web UI
    hook = PostgresHook(postgres_conn_id="postgres_localhost")
    # connect to the db
    conn = hook.get_conn()
    # use cursor to execute our query
    cursor = conn.cursor()
    cursor.execute("select * from orders where date <= '20220501'")
    # return the data in a txt file
    with open("dags/get_orders.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
    # close the cursor and conn
    cursor.close()
    conn.close()
    # logs
    logging.info("Saved orders data in text file get_orders.txt")
    # step2: upload text file into s3
    
with DAG(
    dag_id='dag_with_postgres_hook_v01',
    default_args=default_args,
    start_date=datetime(2024, 7,22),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
    task1
```

We found only one `get_orders.txt` file, which contains orders upto May 1st, 2022. The reason is that the second dag run overwrites the text file which was generated in the first one. Let's update our query to make sure we only get the orders dara during the specific dag run's execution interval and save them with a dynamic text file name  ao that they will not be overwritten. To achieve this, we have to give the current and next execution date to our python function.

Let's open the airflow docs, select the installed airflow version which is `2.7.3`. Search for the macro, then click it. https://airflow.apache.org/docs/apache-airflow/2.7.3/templates-ref.html

There we find the `{{ ds_nodash }}` which is the execution date. Copy them and paste directly as the parameters of our python function since airflow will render those macros values during execution.

```bash
{{ next_ds_nodash }} the next execution date as YYYYMMDD if exists, else None
{{ ds_nodash }} Same as {{ dag_run.logical_date | ds_nodash }}.
```

```python
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
    with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
    # close the cursor and conn
    cursor.close()
    conn.close()
    # logs
    logging.info("Saved orders data in text file : %s", f"dags/get_orders_{ds_nodash}.txt")
    # step2: upload text file into s3
```

Now let's tackle step2, which is uploading the order text file into the S3 bucket. We need the S3 hook library to achieve this.

```python
docker ps

# enter the docker container
docker exec -it <scheduler container id> bash

# check the version of amazon provider
pip list | grep amazon
```

```bash
apache-airflow-providers-amazon          8.10.0
```

Go to the airflow docs and select amazon of matching version. Click the python api and look for hood for s3.

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
```



```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
    with open(f"dags/get_orders_{ds_nodash}.txt", "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)
    # close the cursor and conn
    cursor.close()
    conn.close()
    # logs
    logging.info("Saved orders data in text file : %s", f"dags/get_orders_{ds_nodash}.txt")
    # step2: upload text file into s3
    s3_hook = S3Hook(aws_conn_id='minio_conn')
    s3_hook.load_file(
        filename=f"dags/get_orders_{ds_nodash}.txt",
        key=f"orders/{ds_nodash}.txt",
        bucket_name="airflow",
        replace=True
    )
    
with DAG(
    dag_id='dag_with_postgres_hook_v03',
    default_args=default_args,
    start_date=datetime(2024, 7,22),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='postgres_to_s3',
        python_callable=postgres_to_s3
    )
    task1
```

Run the dag and go to the MinIO page. We can see in the `orders` folder, the txt file has been uploaded.

<img src=".\images\21.png" alt="image" style="zoom:60%;" />

> note :
>
> remember to check to container net (minio & airflow ) 
>
> remember to check the key  in connection 

If you don't want the txt files to exist in your vscode folder, python provides tempfile package which enables us to create files in the system temporary directory.

```python
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
        conn.close( )
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
```

