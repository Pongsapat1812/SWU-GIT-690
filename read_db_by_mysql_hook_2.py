from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook

def reading_data():
    request = "SELECT * FROM customer WHERE id = 5"
    mysql_hook = MySqlHook(mysql_conn_id = 'localdb', schema = 'homestead')

    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall()
    return 1 if sources else 0

def insert_record_if_needed(**kwargs):
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids = 'read_db')

    if return_value == 0:
        insert_request = "INSERT INTO customer (id , name) VALUE(5, 'bid') "
        mysql_hook = MySqlHook(mysql_conn_id = 'localdb',schema = 'homestead')
        connection = mysql_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(insert_request)
        connection.commit()

default_args = {
    'owner' : 'airflow',
    'depends_on_past': False,
    'email' : ['airflow@example.com'],
}

with DAG(
    'read_db_by_mysql_hook_2',
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    start_date = days_ago(2),
    tags = ['airflow_tab'],
) as dag:
    start = DummyOperator(
        task_id = 'start'
    )

    end = DummyOperator(
        task_id = 'end'
    )

    read_db_task = PythonOperator(
        task_id = 'read_db',
        python_callable = reading_data,
        provide_context = True,
    )

    insert_record_task = PythonOperator(
        task_id = 'insert_record_if_needed',
        python_callable = insert_record_if_needed,
        provide_context = True,
    )

    start >> read_db_task >> insert_record_task >> end