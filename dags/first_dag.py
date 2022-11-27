from airflow import DAG
import pendulum
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import settings
from airflow.models import Connection
from airflow.providers.slack.operators.slack_webhook  import SlackWebhookOperator

dag = DAG (
    dag_id="customer_360_pipeline",
    start_date= pendulum.datetime(2022,11,26,10,0,0,tz="Asia/Kolkata"), # specify a datetime in past.
    schedule_interval='*/30 * * * *' #every 30 minutes the scheduler should run the dag.
    )

sensor = HttpSensor( #check if s3 file is present. hits  GET https://trendytech-bigdata.s3.ap-south-1.amazonaws.com/orders.csv
    task_id = "aws_s3_check_orders_file",
    http_conn_id="aws_s3", #in admin->connections -> we create this HTTP connection.
    endpoint='/orders.csv',
    response_check= lambda response : response.status_code == 200,
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

#now we will download the orders.csv from s3 to edge node of the itversity cluster.
def get_order_url():
    session = settings.Session() 
    connection = session.query(Connection).filter(Connection.conn_id == 'aws_s3').first()
    return f'{connection.schema}://{connection.host}/orders.csv'


download_to_edgenode = SSHOperator( #ssh to edge node of itversity cluster to run bash commands and download s3 file.
    task_id = "download_orders_to_itversity_edge_node",
    ssh_conn_id = 'itversity',
    # command = 'pwd && ls', #will land on /home/itv003288
    command = f'''rm -Rf airflow_pipeline && mkdir -p airflow_pipeline && cd airflow_pipeline && wget {get_order_url()} ''', #wget command will hit s3 url and download file to pwd in edge node.
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

#move orders.csv from local to hdfs on cluster
upload_orders_info = SSHOperator(
    task_id  = 'upload_orders_to_hdfs',
    ssh_conn_id = 'itversity',
    command = 'hdfs dfs -rm -R -f airflow_input && hdfs dfs -mkdir -p airflow_input && hdfs dfs -put ./airflow_pipeline/orders.csv airflow_input', #clear folders as task executed in cycle.
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)


#run spark program to filter only closed orders in orders.csv. wrote a spark program which filters only closed orders and bundled it to jar and put in local.
def get_order_filter_cmd():
    command_zero = 'export SPARK_MAJOR_VERSION=2' #to use spark2.
    command_one = 'hdfs dfs -rm -R -f airflow_output' #spark output directory should be empty
    command_two = 'spark-submit --class com.akash.aws.Customer360 ./Desktop/customer360.jar'
    return f'{command_zero} && {command_one} && {command_two}'

process_order_info = SSHOperator(
    task_id = 'process_closed_orders',
    ssh_conn_id = 'itversity',
    cmd_timeout  = 20, #20 seconds timeout for ssh commands to itv.
    command = get_order_filter_cmd(),
    retries = 10,
    retry_delay = pendulum.duration(seconds=20),
    dag = dag
)
#closed orders orders.csv output from spark is saved in hdfs location airflow_output


#create hive table from closed orders.csv
def create_order_hive_table_cmd():
    command_one = '''hive -e "create external table if not exists zion.orders(order_int int, order_date string , customer_id int, status string) row format delimited fields terminated by 
                      ',' location '/user/itv003288/airflow_output' " '''
    return f'{command_one}'
    

hive_order_info = SSHOperator(
    task_id = 'create_hive_order_table',
    ssh_conn_id = 'itversity',
    command = create_order_hive_table_cmd(),
    retries = 10,
    cmd_timeout  = 20, #20 seconds timeout for ssh commands to itv.
    retry_delay = pendulum.duration(seconds=20),
    dag = dag
)

#created customers_itv0032888 table in mysql retail_export table using hdfs cust1/customers.csv(no headers else fail).
#mysql -uretail_user -pitversity -hms.itversity.com -P3306 -f retail_export -A
# create table customers_itv0032888 (customer_id int,customer_fname varchar(50), customer_lname varchar(50), customer_email varchar(50), 
# customer_password varchar(50), customer_street varchar(100),customer_city varchar(50),customer_state char(2),customer_zipcode varchar(20));
#USE sqoop export to load customers data in the mysql table using customers file in hdfs.
#sqoop-export --connect jdbc:mysql://ms.itversity.com:3306/retail_export --username retail_user --password itversity --table customers_itv0032888
#  --export-dir '/user/itv003288/cust1' --fields-terminated-by ','

#commands to create hive managed table from mysql customers_itv0032888 table using sqoop.
def fetch_customer_info_cmd():
    command_one  = "hive -e 'drop table zion.customers'"
    command_two = "hdfs dfs -rm -R -f warehouse/zion.db/customers" #two avoid dir exist error rare.
    command_three = '''sqoop import --connect jdbc:mysql://ms.itversity.com:3306/retail_export --username retail_user --password-file file:///home/itv003288/pass.txt \
                   --table customers_itv0032888 --hive-import --create-hive-table --hive-table zion.customers --autoreset-to-one-mapper''' # as if no pk on customer table it won't fail
    return f'{command_one} && {command_two} && {command_three}'

import_customer_info = SSHOperator( #ssh to edge node of itversity cluster to sqoop import and create hive table from mysql table.
    task_id = "import_customer_info_to_hive",
    ssh_conn_id = 'itversity',
    command = fetch_customer_info_cmd(),
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)
#data for hive managed table is kept in warehouse/zion.db/customers;

#now we have to create hive hbase airflow_hbase table using join of customers and orders from hive so that the queries can be executed quickly from hbase.
def load_hbase_cmd():
    command_one = '''hive -e  "create table if not exists zion.airflow_hbase_itv003288( customer_id int, customer_fname string, customer_lname string, order_int int, order_date string)  
                  stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with SERDEPROPERTIES('hbase.columns.mapping'=':key, personal:customer_fname,
                  personal:customer_lname, personal:order_int, personal:order_date')"'''

    command_two = '''hive -e "insert overwrite table zion.airflow_hbase_itv003288 select c.customer_id,c.customer_fname,c.customer_lname, o.order_int, o.order_date from 
                    zion.customers c join zion.orders o on c.customer_id = o.customer_id" '''
    return f'{command_one} && {command_two}' 

load_hbase_info = SSHOperator(
    task_id = 'load_hbase_customer_order_join_table',
    ssh_conn_id = 'itversity',
    command = load_hbase_cmd(),
    retries = 10, 
    cmd_timeout  = 20, #20 seconds timeout for ssh commands to itv.
    retry_delay = pendulum.duration(seconds=20),
    dag = dag
)

#if the load hbase task pass, success notification is sent to slack
#create http connection with slack_webhook. copy slack webhook from slack app.
success_notify = SlackWebhookOperator(
    task_id = "slack_notification_pass",
    http_conn_id = "slack_webhook",
    message = "Data loaded successfully in HBase",
    channel = '#automation', #slack channel.
    username = 'airflow',
    webhook_token = 'T04CYGNN43B/B04CNBJH44U/7Or8IRY6EjIVKB3kJ9w9yeEy',
    dag = dag
)

failure_notify = SlackWebhookOperator(
    task_id = "slack_notification_fail",
    http_conn_id = "slack_webhook",
    message = "Data loading failed in HBase",
    channel = '#automation',
    username = 'airflow',
    webhook_token = 'T04CYGNN43B/B04CNBJH44U/7Or8IRY6EjIVKB3kJ9w9yeEy', #slack password, all characters after services/ in url.
    trigger_rule = 'all_failed', 
    dag = dag
)

dummy = DummyOperator(
    task_id = 'dummy',
    dag = dag
)

sensor >> import_customer_info 
sensor >> download_to_edgenode >> upload_orders_info >> process_order_info >> hive_order_info 
[import_customer_info,hive_order_info] >> load_hbase_info
load_hbase_info >> [success_notify,failure_notify]