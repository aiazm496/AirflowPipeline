from airflow import DAG
import pendulum
from airflow.operators.python_operator import PythonOperator


dag = DAG (
    dag_id="python_operators",
    start_date= pendulum.datetime(2022,11,27,17,0,0,tz="Asia/Kolkata"), # specify a datetime in past.
    schedule_interval='*/2 * * * *' #every 2 minutes the scheduler should run the dag.
    )


check_even = PythonOperator(
    task_id = 'check_even',
    python_callable= lambda x : x%2==0,
    op_args = [2],
    dag = dag
)

check_even