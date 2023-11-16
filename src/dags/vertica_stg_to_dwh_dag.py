import json
import vertica_python as vp
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import pendulum

with open("/Users/yurisafonov/downloads/Practicum/Project_final/config.json") as config_file:
    config = json.load(config_file)

sql_file = '/Users/yurisafonov/downloads/Practicum/Project_final/sql_queries/stg_to_dwg.sql'

with open(sql_file, 'r') as file:
    sql_query = file.read()

def stg_to_dwh():
    connection = vp.connect(
        host=config["vertica_host"],
        port=config["vertica_port"],
        user=config["vertica_user"],
        password=config["vertica_pass"]
    )

    cursor = connection.cursor()

    try:
        cursor.execute(sql_query)
        res = cursor.fetchall()
    except vp.errors.Error as e:
            raise Exception(f"An error occurred during dwh load: {str(e)}")
    return res

@dag(
    schedule_interval="0 12 * * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=True,
)
def dwh_dag():
    stg_to_dwh_task = PythonOperator(
        task_id="stg_to_dwh",
        python_callable=stg_to_dwh(),
    )

    stg_to_dwh_task


_ = dwh_dag()