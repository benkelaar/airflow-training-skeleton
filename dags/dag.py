import datetime as dt

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

with DAG(
    dag_id="training-money-maker",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 10),
        "depends_on_past": True,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
) as dag:
    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id="read_postgres",
        postgres_conn_id="postgres_training",
        sql="select * from land_registry_price_paid_uk where transfer_date = '{{ ds }}'::date",
        bucket="airflow-training-simple-dag",
        filename="training-price-paid-uk/{{ ds }}/land_registry.json"
    )

    cluster_name = "cluster-{{ ds }}"
    gcs_project_id = "airflowbolcom-544f36a42f5c0d9d"

    create_cluster = DataprocClusterCreateOperator(
        task_id="create_cluster",
        cluster_name=cluster_name,
        project_id=gcs_project_id,
        num_workers=2,
        zone="europe-west4-a"
    )

    cloud_analytics = DataProcPySparkOperator(
        task_id="analyze-data",
        main="gs://europe-west1-training-airfl-b3ce8eaa-bucket/other/spark_statistics.py",
        cluster_name=cluster_name,
        arguments=["{{ ds }}"]
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id="delete_cluster",
        cluster_name=cluster_name,
        project_id=gcs_project_id,
        trigger_rule=TriggerRule.ALL_DONE
    )

    psql_to_gcs >> create_cluster >> cloud_analytics >> delete_cluster
