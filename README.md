# dataops-test

This repository contains a simple DAG that download a CSV data from Github, create a Postgres table and then COPY data into.

In the `dags` folder you will find a `load.py` file with a working Airflow DAG for Airflow 2.0.

In order to make the DAG run you need those prerequisites:
* An Airflow connection named `postgres_test` (cf. [documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#storing-a-connection-in-environment-variables))
* Install Airflow 2.0 with extra package postgres
