from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor


GCP_CONN_ID = 'gcp_connection_id'
GCS_BUCKET_NAME = 'movie-review-bucket'
MOVIE_REVIEW_OBJECT_KEY = 'movie_review.csv'
LOG_REVIEW_OBJECT_KEY = 'log_review.csv'

with DAG(
    dag_id='movie_review_pipeline',
    start_date=days_ago(1),
    schedule='@daily',
    description="A pipeline for movie review analytics",
    catchup=False
) as dag:

    start_workflow = EmptyOperator(task_id='start_workflow')

    verify_today_movie_review = GCSObjectExistenceSensor(
        task_id='verify_today_movie_review',
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=MOVIE_REVIEW_OBJECT_KEY
    )

    verify_today_log_review = GCSObjectExistenceSensor(
        task_id='verify_today_log_review',
        google_cloud_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        object=LOG_REVIEW_OBJECT_KEY
    )

    end_workflow = EmptyOperator(task_id='end_workflow')

start_workflow >> [verify_today_movie_review, verify_today_log_review] >> end_workflow