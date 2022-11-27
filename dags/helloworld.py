from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import pendulum
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.sensors.http_sensor import HttpSensor

dag = DAG(
        dag_id="hello_world_dag",
        schedule_interval='*/2 * * * *', #every 2 minutes
        start_date= pendulum.now("Asia/Kolkata").subtract(minutes=10) #first time ran 10 minutes ago from current time.
)


class MyOperator(BaseOperator):
    
    @apply_defaults
    def __init__(self,name,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.name = name

    def execute(self,context):
        message = f'Hello {self.name}'
        print(message)
        return message


task_1 = BashOperator(
    task_id = "t1",
    bash_command="echo <", #it will fail command.
    dag = dag,
    retries=3, #retry 3 times on failure.
    retry_delay	 = pendulum.duration(seconds=5) #task retry in 10 seconds, default is 5 minutes.
)

task_2 = BashOperator(
    task_id = "t2",
    bash_command="echo task t1 is failed so started t2",
    trigger_rule='all_failed', #if upstream is failed then only it is triggered else skipped. default is all_success -> all upstream tasks should be success.
    dag = dag
)

task_3 = BashOperator(
    task_id = "t3",
    bash_command="echo task t1 compelted so started t3",
    dag = dag
)

task_4 = MyOperator(
    name  = 'Akash',
    task_id = 't4',
    dag = dag,
    trigger_rule = 'one_success'
)

sensor = HttpSensor(
    task_id = 'sensor',
    http_conn_id= "info", #create in airflow UI admin/connections.
    endpoint='/',
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

sensor >> task_1 >> [task_2,task_3] >> task_4 #task_1 is upstream/parent of task_2 and task_3. 

