from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

current_date = str(date.today())
BUCKET = "gs://feisty-flow-bucket-437715/"
PROJECT_ID = "feisty-flow-437715-t9"
PYSPARK_JOB = BUCKET + "/spark-job/flights-etl.py"

DEFAULT_DAG_ARGS = {
    'owner': "airflow",
    'depends_on_past': False,
    "start_date": datetime.utcnow(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "project_id": PROJECT_ID,
    "schedule_interval": "30 2 * * *"
}

with DAG("flights_delay_etl", default_args=DEFAULT_DAG_ARGS, catchup=False) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name="ephemeral-spark-cluster-{{ ds_nodash }}",
        region="asia-east1",
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-1"
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2"
            },
            "gce_cluster_config": {
                "zone_uri": "asia-east1-a"
            }
        },
    )

    submit_pyspark = DataprocSubmitJobOperator(
        task_id="run_pyspark_etl",
        job={
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': "ephemeral-spark-cluster-{{ ds_nodash }}"},
            'pyspark_job': {'main_python_file_uri': PYSPARK_JOB}
        },
        region="asia-east1",
        project_id=PROJECT_ID,
    )

    bq_load_delays_by_distance = GCSToBigQueryOperator(
        task_id="bq_load_avg_delays_by_distance",
        bucket=BUCKET,
        source_objects=["flights_data_output/" + current_date + "_distance_category/part-*"],
        destination_project_dataset_table=PROJECT_ID + ".data_analysis.avg_delays_by_distance",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
    )

    bq_load_delays_by_flight_nums = GCSToBigQueryOperator(
        task_id="bq_load_delays_by_flight_nums",
        bucket=BUCKET,
        source_objects=["flights_data_output/" + current_date + "_flight_nums/part-*"],
        destination_project_dataset_table=PROJECT_ID + ".data_analysis.avg_delays_by_flight_nums",
        autodetect=True,
        source_format="NEWLINE_DELIMITED_JSON",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_name="ephemeral-spark-cluster-{{ ds_nodash }}",
        project_id=PROJECT_ID,
        region="asia-east1",
        trigger_rule=TriggerRule.ALL_DONE
    )

    delete_transformed_files = BashOperator(
        task_id="delete_transformed_files",
        bash_command="gsutil -m rm -r " + BUCKET + "/flights_data_output/*"
    )

    create_cluster >> submit_pyspark >> [bq_load_delays_by_distance, bq_load_delays_by_flight_nums, delete_cluster] >> delete_transformed_files
