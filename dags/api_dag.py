from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_api_dag',
    default_args=default_args,
    description='Uma DAG que faz uma chamada a uma API pública',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)

def fetch_posts():
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    posts = response.json()[:5]  # Pegamos apenas os primeiros 5 posts
    for post in posts:
        print(f"Post ID: {post['id']}, Title: {post['title']}")

fetch_posts_task = PythonOperator(
    task_id='fetch_posts',
    python_callable=fetch_posts,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> fetch_posts_task >> end
