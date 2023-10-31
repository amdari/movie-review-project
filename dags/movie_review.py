import google
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.google.cloud.hooks.dataproc import DataprocHook
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import ClusterGenerator, DataprocCreateClusterOperator, DataprocSubmitJobOperator

GCP_CONN_ID = 'gcp_connection_id'
GCS_BUCKET_NAME = 'movie-review-bucket'
MOVIE_REVIEW_OBJECT_KEY = 'movie_review.csv'
LOG_REVIEW_OBJECT_KEY = 'log_review.csv'
GCP_PROJECT_ID = 'user-behaviour-project'
DATAPROC_CLUSTER_NAME = 'amdari-dataproc-cluster'
GCP_REGION = 'us-central1'

MOVIE_REVIEW_JOB_URI = f'gs://{GCS_BUCKET_NAME}/pyspark/movie_review.py'
LOG_REVIEW_JOB_URI = f'gs://{GCS_BUCKET_NAME}/pyspark/log_review.py'
LOAD_TO_BIGQUERY_JOB_URI = f'gs://{GCS_BUCKET_NAME}/pyspark/movie_review_analytics.py'

def form_pyspark_job(job_file_uri):
    PYSPARK_JOB = dict(
        reference=dict(project_id=GCP_PROJECT_ID),
        placement=dict(cluster_name=DATAPROC_CLUSTER_NAME),
        pyspark_job=dict(main_python_file_uri=job_file_uri)
    )

    return PYSPARK_JOB


DATAPROC_CLUSTER_CONFIG = ClusterGenerator(
    project_id=GCP_PROJECT_ID,
    zone='us-central1-a',
    master_machine_type='n2-standard-2',
    master_disk_size=32,
    worker_machine_type='n2-standard-2',
    worker_disk_size=32,
    num_workers=2,
    idle_delete_ttl=1200,
    optional_components=['JUPYTER'],
    enable_component_gateway=True
).make()


def _create_cluster_or_continue(cluster_name, project_id, region):
    dataproc_hook = DataprocHook(gcp_conn_id=GCP_CONN_ID)
    try:
        cluster_info = dataproc_hook.get_cluster(
            cluster_name=cluster_name,
            project_id=project_id,
            region=region
        )
        return 'continue_pipeline'
    except google.api_core.exceptions.NotFound:
        return 'create_dataproc_cluster'

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

    create_cluster_or_continue = BranchPythonOperator(
        task_id='create_cluster_or_continue',
        python_callable=_create_cluster_or_continue,
        op_kwargs=dict(
            cluster_name=DATAPROC_CLUSTER_NAME,
            project_id=GCP_PROJECT_ID,
            region=GCP_REGION
        )
    )

    continue_pipeline = EmptyOperator(task_id='continue_pipeline')


    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name=DATAPROC_CLUSTER_NAME,
        gcp_conn_id=GCP_CONN_ID,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID
    )


    continue_workflow = EmptyOperator(task_id='continue_workflow')

    submit_log_review_job = DataprocSubmitJobOperator(
        task_id="submit_log_review_job",
        job=form_pyspark_job(LOG_REVIEW_JOB_URI),
        project_id=GCP_PROJECT_ID,
        region='us-central1',
        gcp_conn_id=GCP_CONN_ID,
    )

    submit_movie_review_job = DataprocSubmitJobOperator(
        task_id="submit_movie_review_job",
        job=form_pyspark_job(MOVIE_REVIEW_JOB_URI),
        project_id=GCP_PROJECT_ID,
        region='us-central1',
        gcp_conn_id=GCP_CONN_ID,
    )

    submit_load_to_bigquery_job = DataprocSubmitJobOperator(
        task_id="submit_load_to_bigquery_job",
        job=form_pyspark_job(LOAD_TO_BIGQUERY_JOB_URI),
        project_id=GCP_PROJECT_ID,
        region='us-central1',
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    end_workflow = EmptyOperator(task_id='end_workflow')


(
    start_workflow 
    >> [verify_today_movie_review, verify_today_log_review] 
    >> create_cluster_or_continue
    >> [create_dataproc_cluster, continue_pipeline]
    >> continue_workflow
    >> [submit_log_review_job, submit_movie_review_job]
    >> submit_load_to_bigquery_job
    >> end_workflow
)

