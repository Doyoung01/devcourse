import requests
import psycopg2
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id="redshift_dev_db")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

@task
def extract(url):
    f = requests.get(url).json()
    return f

@task
def transform(dict):
    records = []
    for d in dict:
        country = d['name']['official']
        population = d['population']
        area = d['area']
        records.append([country, population, area])
    
    return records

@task
def load(schema, table, records):
    cur = get_Redshift_connection()

    try:
        cur.execute("BEGIN;")
        cur.execute(f"DROP TABLE IF EXISTS {schema}.{table};")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            country VARCHAR(255),
            population INT,
            area FLOAT
        )
        """)
        for r in records:
            sql = f"INSERT INTO {schema}.{table} VALUES (%s, %s, %s)"
            cur.execute(sql, (r[0], r[1], r[2]))
        cur.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        cur.execute("ROLLBACK;")
        print(error)
        raise error

with DAG(
    dag_id="rest_countries",
    start_date=datetime(2023, 8, 13),
    # UTC로 매주 토요일 오전 6시 30분에 실행
    schedule='30 6 * * SAT',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    url = "https://restcountries.com/v3.1/all"
    schema = "jjyo0108"
    table = "rest_countries"

    records = transform(extract(url))
    load(schema, table, records)