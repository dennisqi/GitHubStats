from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from time import sleep

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 29, 3, 51),
    'email': ['dennis09121111@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG(
    'githubstats_dag',
    default_args=default_args,
    schedule_interval='15 0 * * *'
)

generate_url = BashOperator(
    task_id='generate_url',
    bash_command='python ~/GitHubStats/src/s3_upload/url_generator.py',
    dag=dag)

upload_to_s3 = BashOperator(
    task_id='upload_to_s3',
    bash_command='~/GitHubStats/src/s3_upload/s3upload.sh ~/GitHubStats/data/url_generator_coming_urls.txt ~/GitHubStats/data/url_generator_saved_urls.txt gharchive',
    dag=dag)

spark_submit = BashOperator(
    task_id='spark_submit',
    bash_command='spark-submit ~/GitHubStats/src/calculate/main.py present',
    dag=dag)

generate_url >> upload_to_s3 >> spark_submit
