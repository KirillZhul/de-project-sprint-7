from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
import os

os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 1, 1),
}

dag_spark = DAG(
    dag_id="sprint7_project",
    default_args=default_args,
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
)

start_task = DummyOperator(task_id="start")

users_mart = SparkSubmitOperator(
    task_id="users_mart",
    dag=dag_spark,
    application="/lessons/users_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/kirillzhul/data/geo/events/",
        "/user/kirillzhul/geo_2.csv",
        "/user/kirillzhul/data/analytics/",
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
        "spark.sql.broadcastTimeout": 1200,
    },
    executor_cores=2,
    executor_memory="4g",
)

locations_mart = SparkSubmitOperator(
    task_id="locations_mart",
    dag=dag_spark,
    application="/lessons/locations_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/kirillzhul/data/geo/events/",
        "/user/kirillzhul/geo_2.csv",
        "/user/kirillzhul/data/analytics/",
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
        "spark.sql.broadcastTimeout": 1200,
    },
    executor_cores=2,
    executor_memory="4g",
)


recommendations_mart = SparkSubmitOperator(
    task_id="recommendations_mart",
    dag=dag_spark,
    application="/lessons/recommendations_mart.py",
    conn_id="yarn_spark",
    application_args=[
        "2022-05-31",
        "30",
        "/user/kirillzhul/data/geo/events/",
        "/user/kirillzhul/geo_2.csv",
        "/user/kirillzhul/data/analytics/",
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
        "spark.sql.broadcastTimeout": 1200,
    },
    executor_cores=2,
    executor_memory="4g",
)

end_task = DummyOperator(task_id="end")

start_task >> users_mart >> locations_mart >> recommendations_mart >> end_task