from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pandas as pd


args = {
    'owner': 'enrique',
    'start_date': '2021-10-08',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id="a_local_connection_test_v1",
    default_args=args,
    tags=["firstVersion"],
    schedule_interval=None,                                 # '@daily',   None
    catchup=False,                                          # True->Run the days since 'start_date', False->only once
)


def connection(query, pandas_view=0, conn_id='airflow_local_postgres_connection'):
    db_hook_s = PostgresHook(postgres_conn_id=conn_id)
    connection = db_hook_s.get_conn()
    if pandas_view==1:
        df = pd.read_sql_query(query, connection)
        print('data_frame: ', df)
        print("Credentials: ", Variable.get("credential_name"))
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    connection.close()

with dag:
    def create_table():
        query_create_table = """
        DROP TABLE IF EXISTS organic_analytica;
        CREATE TABLE IF NOT EXISTS organic_analytica (id varchar, date timestamp, total numeric);
        """
        # query_check_data_bases = "SELECT datname FROM pg_database;"       # query to view all databases
        connection(query_create_table)

    def insert_data():
        query_insert_data = "INSERT INTO organic_analytica (id, date, total) VALUES (210895, '2016-06-22 19:10:25-07', 89);"
        connection(query_insert_data)

    def view_table():
        query_view_table = "SELECT * FROM organic_analytica;"
        connection(query_view_table, pandas_view=1)


    t1 = PythonOperator(
        task_id="connection_ok_create_table",
        python_callable=create_table,
    )

    t2 = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data,
    )

    t3 = PythonOperator(
        task_id="view_tables",
        python_callable=view_table,
    )

    t1 >> t2 >> t3