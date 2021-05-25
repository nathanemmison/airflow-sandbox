# DAGs
# A DAG (Directed Acyclic Graph) is the core concept of Airflow, collecting Tasks together, organized with dependencies and relationships to say how they should run.

# DAG basically defines the ETL process

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

import psycopg2
import csv

# Some arguments for the dag
default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    'wait_for_downstream': True,
    "email": ["airflow@airflow.com"],
    "start_date": datetime(2021, 5, 23),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# config
# local
sql ='./scripts/sql/filter_unload_user_purchase.sql'
csv = '/temp/temp_filtered_user_purchase.csv'

# Give the DAG (Pipeline) a name, the arguments, schedule
dag = DAG("user_behaviour", default_args=default_args,
          schedule_interval="0 0 * * *", max_active_runs=1)

# Operators are the pipeline steps
end_of_data_pipeline = DummyOperator(task_id='end_of_data_pipeline', dag=dag)

def pg():

    try:
        connection = psycopg2.connect(user="airflow",
                                      password="airflow",
                                      host="localhost",
                                      port="5432",
                                      database="airflow")
        cursor = connection.cursor()
        postgreSQL_select_Query = "select * from retail.user_purchase"

        cursor.execute(postgreSQL_select_Query)
        records = cursor.fetchall()

        print("Print each row and it's columns values")

        with open('retail.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)

            for row in records:
                writer.write(row)

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


pg_unload = PythonOperator(
    dag=dag,
    task_id='get_data',
    python_callable=pg,
)

pg_unload >> end_of_data_pipeline