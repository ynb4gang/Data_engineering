from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


default_args={
    "email": ["**********"],
    "email_on_failure": True,
    'start_date': "2024-09-24",
    "ssh_conn_id": "***********",
    'provide_context' : True,
    'schedule_interval': '@monthly'
}

def extract_data_to_csv(): 
    pg_hook = PostgresHook(postgres_conn_id='******')
    df = pg_hook.get_pandas_df(sql="SELECT * FROM **********")
    csv_path = '/data/airflow/dags/cvm-srv.csv'
    df.to_csv(csv_path, sep=';', index=False)

def load_data_and_count_rows():
    csv_path = '/data/airflow/dags/cvm-srv.csv'
    df = pd.read_csv(csv_path,sep=';')
    pg_hook = PostgresHook(postgres_conn_id='core_gp')
    
    pg_hook.insert_rows(table="core.analyze_bi.cvm_srv", rows=df.values.tolist(), target_fields=df.columns.tolist())
    return len(df)
              
    
with DAG (
    dag_id='extract_load_cvm_srv',
    default_args = default_args,
) as dag:
        extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data_to_csv,provide_context=True)

        load_data = PythonOperator(task_id='load_data', python_callable=load_data_and_count_rows,provide_context=True)

        notify_email = EmailOperator(
            task_id='send_email',
            to='**********',
            subject='Data Load cvm_srv Notification',
            html_content="""<h3>Data Load Complete</h3> 
<p>{{ task_instance.xcom_pull(task_ids='load_data') }} rows were loaded.</p>""")
         
extract_data >> load_data >> notify_email
