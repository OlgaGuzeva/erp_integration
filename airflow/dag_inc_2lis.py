import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='test_start_python_script',
    default_args=args,
    schedule_interval="@hourly",
)

set_env_var = BashOperator(
    task_id='set_env_var',
    bash_command='export HDFSCLI_CONFIG = /airflow/erp',
    dag=dag,
)

load_2lis_02_itm = BashOperator(
    task_id='load_2lis_02_itm',
    bash_command='python3.7 /airflow/erp/load_inc_from_erp_to_hdp.py 2LIS_02_ITM',
    dag=dag,
)

set_env_var >> load_2lis_02_itm

if __name__ == "__main__":
    dag.cli()
