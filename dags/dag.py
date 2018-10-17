import datetime as dt

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator
)
from godatadriven.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from currency_conversion_operator import HttpToGcsOperator

with DAG(
    dag_id="training-money-maker",
    schedule_interval="30 7 * * *",
    default_args={
        "owner": "airflow",
        "start_date": dt.datetime(2018, 10, 10),
        "depends_on_past": False,
        "email_on_failure": True,
        "email": "airflow_errors@myorganisation.com",
    },
) as dag:
    currency_retrieval = HttpToGcsOperator(
        http_conn_id='currency_converter',
        endpoint="airflow-training-transform-valutas?date={{ ds }}&from=GBP&to=USD",
        gcs_path="currency_rates/{{ ds }}"
    )

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
        task_id="analyze_data",
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

    store_analytics = GoogleCloudStorageToBigQueryOperator(
        task_id="store_statistics",
        bucket="airflow-training-simple-dag",
        source_objects=["average_prices/transfer_date={{ ds }}/*.parquet"],
        destination_project_dataset_table=gcs_project_id + ".property_price_averages.average_{{ ds_nodash }}",
        source_format="parquet",
        write_disposition="WRITE_TRUNCATE"
    )

    psql_to_gcs >> create_cluster >> cloud_analytics >> delete_cluster
    currency_retrieval >> create_cluster
    cloud_analytics >> store_analytics
