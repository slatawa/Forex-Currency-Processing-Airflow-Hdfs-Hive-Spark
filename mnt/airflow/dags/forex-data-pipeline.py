from airflow import DAG
from datetime import timedelta
from _datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
import requests as re
import json


def download_forex_currency_data():
    with open(input_file) as file:
        for lineno, rec in enumerate(file):
            print(lineno)
            print(rec)
            if lineno == 0:
                # pass for header
                pass
            else:
                rec = rec.replace('\n', '')
                base = rec.split(';')[0]
                curr_vals = rec.split(';')[1]
                curr = curr_vals.split(' ')
                print(f'{base} - currencies - {curr}')

                ## lets get the data

                url = base_forex_url + 'access_key=' + access_key + '&base=' + base

                response = re.get(url=url)
                if response.status_code == 200:
                    in_data = json.loads(response.text)
                    out_data = {'base': in_data['base'],
                                'date': in_data['date'],
                                'rates': {}}
                    for rec in curr:
                        out_data['rates'][rec] = in_data['rates'][rec]
                else:
                    raise Exception('Invalid Response from Forex API')

                # write data to a file
                with open(output_file, 'a') as outfile:
                    json.dump(out_data, outfile)
                    outfile.write('\n')


default_args = {'owner': 'sanchit.latawa',
                'depends_on_past': False,
                'start_date': datetime.utcnow() - timedelta(minutes=10),
                'email_on_failure': False,
                'email_on_retry': False,
                'email': 'slatawa@yahoo.in',
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'schedule_interval': '@daily',
                'concurrency': 1,
                'max_active_runs': 1,
                'catchup ': False
                }

dag = DAG(dag_id='forex-currency-pipeline', default_args=default_args)

# Step 1 check if Forex API available

access_key = Variable.get('access_key')
endpoint = 'api/latest?access_key=' + access_key + '& base = USD'
start_task = DummyOperator(task_id='start_task', dag=dag)

check_avail_api = HttpSensor(task_id='check_forex_api_avail',
                             http_conn_id='forex_api',
                             endpoint=endpoint, dag=dag,
                             response_check=lambda response: 'success' in response.text,
                             poke_interval=5,
                             timeout=20)

# Step 2 - check forex currency file available

check_curr_file = FileSensor(task_id='check_forex_currency_file_avail', fs_conn_id='forex_path', dag=dag,
                             filepath='forex_currencies.csv',
                             poke_interval=5, timeout=20)

end_task = DummyOperator(task_id='end_task', dag=dag)

# step 3 Download forex currency data
input_file = Variable.get('input_file_forex_json')
output_file = Variable.get('output_file_forex_json')
base_forex_url = Variable.get('base_forex_url')

download_forex_curr_data = PythonOperator(task_id='download_forex_curr_data', dag=dag,
                                          python_callable=download_forex_currency_data)

# step 4 Store the file in HDFS

save_currency_rates = BashOperator(task_id='save_currency_rates', dag=dag,
                                   bash_command="""
                                   hdfs dfs -mkdir -p /forex && \
                                   hdfs dfs -put -f /opt/airflow/dags/files/forex_currencies.json /forex
                                   """)

# step 5 create Hive table

creating_forex_rates_table = HiveOperator(
    task_id="creating_forex_rates_table",
    dag=dag,
    hive_cli_conn_id="hive_conn",
    hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
            base STRING,
            eur DOUBLE,
            usd DOUBLE,
            nzd DOUBLE,
            gbp DOUBLE,
            jpy DOUBLE,
            cad DOUBLE
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """
)

spark_load_hive_table = SparkSubmitOperator(task_id='spark_load_hive_table',
                                            dag=dag,
                                            conn_id='spark_conn',
                                            application='/opt/airflow/dags/scripts/forex_processing.py',
                                            verbose=False)

send_email_notification = EmailOperator(task_id='send_email_notification',
                                        dag=dag,
                                        to='slatawa@yahoo.in',
                                        subject='forex_data_pipeline',
                                        html_content='<h3>Forex Data Pipeline Completed</h3>'
                                        )

start_task >> check_avail_api >> check_curr_file >> download_forex_curr_data >> save_currency_rates
save_currency_rates >> creating_forex_rates_table >> \
spark_load_hive_table >> send_email_notification >> end_task
