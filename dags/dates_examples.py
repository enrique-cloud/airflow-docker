from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta


args = {
    'owner': 'enrique',
    'start_date': '2021-10-25',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    dag_id="dates_examples_and_connecting_functions_v1.0",
    default_args=args,
    tags=["firstVersion"],
    schedule_interval='@daily',                            # '@daily',   None
    catchup=True,                                          # True->Run the days since 'start_date', False->only once
)


def show_dates_airflow_params(date):
    print("Airflow dates examples: ", date)
    return date

def integrate_with_previous_function(date):
    value_example = show_dates_airflow_params(date)
    print("Functions joined: ", value_example)


with dag:
    seconds = "{{ execution_date.timestamp() | int }}"
    ds = "{{ ds }}"

    show_dates_params = PythonOperator(
        task_id="dates_params",
        python_callable=show_dates_airflow_params,
        op_kwargs={"date": ds}
    )

    joined_functions = PythonOperator(
        task_id="joined_functions",
        python_callable=integrate_with_previous_function,
        op_kwargs={"date": ds}
    )

    show_dates_params >> joined_functions
