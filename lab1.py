from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def change_datatype():
    df = pd.read_csv('/first/tested.csv')

    convert_dict = {'Pclass': float} 
    df = df.astype(convert_dict)

    df.to_csv('/first/tested.csv')

def filter_male():
    df = pd.read_parquet('/first/tested.csv')
    mask = df['Sex'] == 'male'
    print(mask)
    flter_row_df = df[mask]
    query_row_df = df.query('Sex == "male"')
    print(query_row_df)

def save_data():
    df = pd.read_csv('/first/tested.csv')
    df.to_parquet('/tmp/titanic_lab.parquet', engine='fastparquet')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
}

with DAG(
    'dataframe_lab1', 
    default_args=default_args, 
    schedule_interval=timedelta (days=1), 
    start_date=days_ago(2), 
    tags=['airflow_tab'],
) as dag:
    start = DummyOperator( 
        task_id='start'
    )

    end = DummyOperator( 
        task_id='end'
)
    
change_datatype_task= PythonOperator( 
    task_id='change_datatype', 
    python_callable=change_datatype
)

save_data_task= PythonOperator( 
    task_id='save_data', 
    python_callable=save_data 
    )

start >> change_datatype_task >> save_data_task >> end