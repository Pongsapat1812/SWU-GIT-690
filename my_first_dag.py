from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

def dummy_test():
    return 'branch_a'

with DAG(
    "my_first_dag1",
    default_args={
        "email": ["pongsapat.kongsereeparp@gmail.com"],
    },
    description="A simple tutorial DAG",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )
    t2 = BashOperator(
        task_id="print_date2",
        bash_command="date",
    )

    A_task = DummyOperator(task_id='branch_a', dag=dag)
    B_task = DummyOperator(task_id='branch_false', dag=dag)

    branch_task = BranchPythonOperator(
        task_id='branching',
        python_callable=dummy_test,
        dag=dag,
    )

    t1 >> t2
    branch_task >> A_task
    branch_task >> B_task