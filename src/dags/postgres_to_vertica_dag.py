import psycopg2
import json
import csv
import vertica_python as vp
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
import pendulum

with open("/Users/yurisafonov/downloads/Practicum/Project_final/config.json") as config_file:
    config = json.load(config_file)


def fetch_table_from_postgre(table_name, output_file_path):

    # Настройки подключения к PostgreSQL
    pg_host = config["postgres_host"]
    pg_port = config["postgres_port"]
    pg_user = config["postgres_user"]
    pg_password = config["postgres_pass"]
    pg_database = config["postgres_db"]
    output_file_path = output_file_path
    table_name = table_name

    # Подключаемся к PostgreSQL
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        user=pg_user,
        password=pg_password,
        dbname=pg_database,
        sslmode='verify-ca',
        sslrootcert="/Users/yurisafonov/Downloads/Practicum/Project_final/CA.pem"
    )

    cursor = conn.cursor()

    cursor.execute(
        f"SELECT * FROM {table_name};"
    )

    with open(output_file_path, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")

        writer.writerow([desc[0] for desc in cursor.description])

        for row in cursor:
            writer.writerow(row)

    cursor.close

    conn.close


def load_table_to_vertica(table_name, input_file_path):
    connection = vp.connect(
        host=config["vertica_host"],
        port=config["vertica_port"],
        user=config["vertica_user"],
        password=config["vertica_pass"]
    )

    # прочитаем порядок колонок в заголовке csv-файла, чтобы корректно вставить значения в vertica
    with open(input_file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        headers = next(reader)
        header = (', '.join(headers))

    cursor = connection.cursor()

    cursor.execute(
        f"COPY {table_name} ({header}) FROM LOCAL {input_file_path} \
        DELIMITER ',' \
        SKIP 1 \
        REJECTED DATA '/Users/yurisafonov/Downloads/Practicum/Project_final/rejected_{table_name}.csv';"
    )

    cursor.close

    connection.close


@dag(
    schedule_interval="0 12 1 * *",
    start_date=pendulum.parse("2022-10-01"),
    catchup=False,
)
def staging_dag():
    fetch_currencies_task = PythonOperator(
        task_id="fetch_currencies",
        python_callable=fetch_table_from_postgre(),
        op_kwargs={"table_name": "public.currencies", "output_file_path":
                   "/Users/yurisafonov/Downloads/Practicum/Project_final/currencies.csv"},
    )

    load_currencies_task = PythonOperator(
        task_id="load_currencies_staging",
        python_callable=load_table_to_vertica(),
        op_kwargs={"table_name": "STV2023070330__STAGING.currencies", "input_file_path":
                   "/Users/yurisafonov/Downloads/Practicum/Project_final/currencies.csv"},
    )

    fetch_transactions_task = PythonOperator(
        task_id="fetch_transactions",
        python_callable=fetch_table_from_postgre(),
        op_kwargs={"table_name": "public.transactions", "output_file_path":
                   "/Users/yurisafonov/Downloads/Practicum/Project_final/transactions.csv"},
    )

    load_transactions_task = PythonOperator(
        task_id="load_transactions_staging",
        python_callable=load_table_to_vertica(),
        op_kwargs={"table_name": "STV2023070330__STAGING.transactions", "input_file_path":
                   "/Users/yurisafonov/Downloads/Practicum/Project_final/transactions.csv"},
    )

    (
        fetch_currencies_task
        >> fetch_transactions_task
        >> load_currencies_task
        >> load_transactions_task
    )


_ = staging_dag()
