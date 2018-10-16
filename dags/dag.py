import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id="my_first_dag",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 8, 1),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
)


def print_exec_date(**context):
    print(context["execution_date"])


my_task = PythonOperator(
    task_id="task_name", python_callable=print_exec_date, provide_context=True, dag=dag
)

psql_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="read_postgres",
    postgres_conn_id="postgres_training",
    sql="select * from gdd.land_registry_price_paid_uk where transfer_date = '{{ ds }}'::date",
    bucket="gs://airflow-training-simple-dag/",
    filename="training-price-paid-uk/{{ ds }}",
    dag=dag
)

psql_to_gcs >> my_task