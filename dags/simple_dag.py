from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'simple_dag',
    default_args=default_args,
    description='Uma DAG simples para iniciantes',
    schedule_interval=timedelta(days=1),
    catchup=False
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

def process_data():
    print("Simulando o processamento de dados...")

process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Definindo as dependências
start >> extract >> process >> end
