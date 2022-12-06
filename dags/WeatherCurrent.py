#every 30min, we hit the api to get current temperature of Delhi from free source api.weatherstack.com
#the response in json is parsed using spark and temperature value is filtered and put in hdfs.
#create mysql table and put the temperature value in it.

from airflow import DAG
import pendulum
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow import settings
from airflow.models import Connection
from airflow.providers.sftp.operators.sftp import SFTPOperator

dag = DAG (
    dag_id="current_weather",
    start_date= pendulum.datetime(2022,12,5,10,0,0,tz="Asia/Kolkata"), # specify a datetime in past.
    schedule_interval='*/30 * * * *' #every 30 minute the scheduler should run the dag.
    )


#check if connection to weather api is ok.(200)
#http://api.weatherstack.com/current?access_key=ACCESS_KEY&query=Delhi

estabilish_connection_to_weatherstack_api = HttpSensor(
    task_id = "estabilish_connection_to_weatherstack_api",
    http_conn_id="weather_stack", #http://api.weatherstack.com/
    endpoint="current?access_key={{var.value.weatherStackAccessToken}}&query=Delhi", 
    response_check= lambda response : response.status_code == 200,
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

#extract the current temperature using api to local /home/itv003288
#use SSH
download_to_edgenode = SSHOperator( #ssh to edge node of itversity cluster to run bash commands.
    task_id = "hit_temp_api_and_put_to_local",
    ssh_conn_id = 'itversity',
    command = '''rm -R -f weather_store && mkdir -p weather_store && cd weather_store && wget "http://api.weatherstack.com/current?access_key={{var.value.weatherStackAccessToken}}&query=Delhi" && cd .. && mv ./weather_store/* ./weather_store/temp ''', 
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

#put the file to hdfs
transfer_to_hdfs = SSHOperator( 
    task_id = "transfer_to_hdfs",
    ssh_conn_id = 'itversity',
    command = f'''hdfs dfs -rm -R -f weather_store_hdfs && hdfs dfs -mkdir -p weather_store_hdfs && hdfs dfs -put ./weather_store/temp weather_store_hdfs ''', 
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

#build jar using maven plugin, jar created at local location ->  C:/Users/akashpc/IdeaProjects/SparkWordCount/target
#use SFTP operator to transfer this file to remote itversity machine, using same ssh connection id.
# airlow worker runs on ubuntu machine so we have to pick file from mnt
#/mnt/c/Users/akashpc/IdeaProjects/SparkWordCount/target/SparkLearn-1.0-SNAPSHOT.jar to /home/itv003288

copy_jar_to_linux = SFTPOperator(
    task_id = 'copy_jar_to_linux',
    ssh_conn_id = 'itversity', #local is ubuntu
    local_filepath = '/mnt/c/Users/akashpc/IdeaProjects/SparkWordCount/target/SparkLearn-1.0-SNAPSHOT.jar',
    remote_filepath = 'weather_jar/SparkLearn-1.0-SNAPSHOT.jar', #will be created if not exists
    operation = 'put', #to transfer file from local to remote
    create_intermediate_dirs = True,
    dag = dag
)

#we have the current temperature in json response kept at hdfs location weather_store_hdfs
#run spark job to get temperature by parsing the json and getting only temperature and putting 
#it in new hdfs location -> weather_store_hdfs_temperature.

parse_json_get_temperature_using_spark = SSHOperator(
    task_id = "parse_json_get_temperature_using_spark",
    ssh_conn_id = 'itversity',
    command = '''export SPARK_MAJOR_VERSION=2 && hdfs dfs -rm -R -f weather_store_hdfs_temperature && spark-submit --class  com.akash.aws.JsonWeatherParse ./weather_jar/SparkLearn-1.0-SNAPSHOT.jar ''' ,
    cmd_timeout = 20,
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)
#we have part-00000 file (partition 1 set using coalesce) containing current temperature.

#create mysql table with temp and current_timestamp() and load data using sqoop from hdfs
#weather_store_hdfs_temperature to that table.

create_mysql_table_for_storing_temperature = SSHOperator(
    task_id = "create_mysql_table_for_storing_temperature",
    ssh_conn_id = 'itversity',
    command = '''mysql -uretail_user -p{{var.value.sqlpassword}} -P3306 -hms.itversity.com -f retail_export -A -e "create table if not exists temperature_store_itv003288(temperature int, record_time timestamp default current_timestamp())" ''' ,
    cmd_timeout = 20,
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

#use sqoop export to load hdfs temperature current data to mysql table 'temperature_store_itv003288'

sqoop_export_temp_data_from_hdfs_to_mysql = SSHOperator(
    task_id = "sqoop_export_temp_data_from_hdfs_to_mysql",
    ssh_conn_id = 'itversity',
    command = '''sqoop-export --connect jdbc:mysql://ms.itversity.com:3306/retail_export --username retail_user --password {{var.value.sqlpassword}} --table temperature_store_itv003288 \
              --export-dir '/user/itv003288/weather_store_hdfs_temperature' ''',
    cmd_timeout = 20,
    retries = 3,
    retry_delay = pendulum.duration(seconds=5),
    dag = dag
)

estabilish_connection_to_weatherstack_api >>download_to_edgenode >> transfer_to_hdfs >> copy_jar_to_linux

copy_jar_to_linux >> parse_json_get_temperature_using_spark >> create_mysql_table_for_storing_temperature

create_mysql_table_for_storing_temperature >> sqoop_export_temp_data_from_hdfs_to_mysql