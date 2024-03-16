from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import random

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir a DAG
dag = DAG(
    'condicional_dag',
    default_args=default_args,
    description='Uma DAG de exemplo com múltiplas tarefas',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Tarefas
start = DummyOperator(task_id='start', dag=dag)

def extract_data():
    print("Simulando a extração de dados...")

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

def choose_path():
    return 'process_path_a' if random.randint(0,1) == 0 else 'process_path_b'

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_path,
    dag=dag,
)

process_path_a = DummyOperator(task_id='process_path_a', dag=dag)
process_path_b = DummyOperator(task_id='process_path_b', dag=dag)

def push_function(**kwargs):
    # Empurra um valor para XCom
    kwargs['ti'].xcom_push(key='sample_key', value='sample_data')

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_function,
    provide_context=True,
    dag=dag,
)

def pull_function(**kwargs):
    # Puxa o valor de XCom
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='sample_key', task_ids='push_task')
    print(f"Pulled value: {pulled_value}")

pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Definindo as dependências
start >> extract >> branch_task
branch_task >> process_path_a >> push_task >> pull_task >> end
branch_task >> process_path_b >> end
