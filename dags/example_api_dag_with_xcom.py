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
    'example_api_dag_with_xcom',
    default_args=default_args,
    description='Uma DAG que faz uma chamada a uma API pública e usa XCom',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start = DummyOperator(task_id='start', dag=dag)

def fetch_posts():
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    posts = response.json()[:5]
    return posts

fetch_posts_task = PythonOperator(
    task_id='fetch_posts',
    python_callable=fetch_posts,
    dag=dag,
)

def print_posts_titles(**kwargs):
    ti = kwargs['ti']
    posts = ti.xcom_pull(task_ids='fetch_posts')
    for post in posts:
        print(f"Post ID: {post['id']}, Title: {post['title']}")

print_posts_titles_task = PythonOperator(
    task_id='print_posts_titles',
    python_callable=print_posts_titles,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> fetch_posts_task >> print_posts_titles_task >> end
