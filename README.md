## Description

Example Airflow DAG and Spark Job for Google Cloud Dataproc


## Motivation

This example is meant to demonstrate basic functionality within Airflow for managing Dataproc Spark Clusters and Spark Jobs.
It also demonstrates usage of the BigQuery Spark Connector.


## Features

* Uses Airflow DataProcHook and Google Python API to check for existence of a Dataproc cluster
* Uses Airflow BranchPythonOperator to decide whether to create a Dataproc cluster
* Uses Airflow DataprocClusterCreateOperator to create a Dataproc cluster
* Uses Airflow DataProcSparkOperator to launch a spark job
* Uses Airflow DataprocClusterDeleteOperator to delete the Dataproc cluster
* Creates Spark Dataset from data loaded from a BigQuery table by BigQuery Spark Connector


## Usage

1. run `sbt assembly` in spark-example to create an assembly jar
2. upload the assembly jar to GCS
3. Add dataproc_dag.py to your dags directory (`/home/airflow/airflow/dags/` on your airflow server or your dags directory in GCS if using Cloud Composer)
4. In the Airflow UI, set variables:
  * `project` GCP project id
  * `region` GCP region ('us-central1')
  * `subnet` VPC subnet id (short id, not the full uri)
  * `bucket` GCS bucket
  * `prefix` GCS prefix ('/dataproc_example')
  * `dataset` BigQuery dataset
  * `table` BigQuery table
  * `jarPrefix` where you uploaded the assembly jar


## License

Apache License 2.0
