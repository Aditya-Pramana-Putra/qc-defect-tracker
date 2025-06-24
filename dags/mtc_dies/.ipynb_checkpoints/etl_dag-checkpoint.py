from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from airflow.hooks.base import BaseHook

# Tambahkan path agar bisa mengimpor modul lokal
sys.path.append('/opt/airflow')

from etl_mtc_dies.extract import MTCDiesExtractor
from etl_mtc_dies.transform import MTCDiesTransformer

# ====================
# PostgreSQL Connection
# ====================
def get_postgres_connection():
    airflow_conn = BaseHook.get_connection("airflow")
    conn = psycopg2.connect(
        host=airflow_conn.host,
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
        port=airflow_conn.port
    )
    return conn

def get_sqlalchemy_engine():
    airflow_conn = BaseHook.get_connection("airflow")
    url = URL.create(
        "postgresql+psycopg2",
        username=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port,
        database=airflow_conn.schema
    )
    return create_engine(url)

def save_excel_to_postgres(excel_path, table_name, layer='bronze'):
    try:
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"File Excel tidak ditemukan: {excel_path}")
        df = pd.read_excel(excel_path)
        if df.empty:
            raise ValueError("Data kosong.")
        df['created_at'] = datetime.utcnow()
        if layer == 'silver':
            df['updated_at'] = datetime.utcnow()
        engine = get_sqlalchemy_engine()
        if_exists = 'replace' if layer == 'bronze' else 'append'
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False, method='multi')
        print(f"✅ Data tersimpan ke PostgreSQL: {table_name}")
    except Exception as e:
        print(f"❌ Gagal menyimpan ke PostgreSQL: {e}")
        raise

# ====================
# Microsoft Teams Alerts
# ====================
def notify_teams(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task').task_id
    execution_date = context.get('ts')
    try_number = context.get('ti').try_number
    log_url = f"http://localhost:8080/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"
    message = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"Airflow Task Failed: {task_id}",
        "themeColor": "FF0000",
        "title": f"❌ Airflow Alert: Task Failed in {dag_id}",
        "sections": [{
            "facts": [
                {"name": "DAG", "value": dag_id},
                {"name": "Task", "value": task_id},
                {"name": "Execution Time", "value": execution_date},
                {"name": "Try Number", "value": str(try_number)},
                {"name": "Log URL", "value": log_url},
            ],
            "markdown": True
        }]
    }
    webhook_url = "https://karyabb.webhook.office.com/webhookb2/ee565114-3427-4bac-a7f4-880b13d2fa79@d5318fb6-ef66-45f2-877a-95d4592b8d26/IncomingWebhook/11397220f3514fb08a2d95ba4cd53142/67bf709c-b2a9-406f-9f42-fdacf4592621/V2ouhl16DpFqga8ldF6vt8cindGp2N1_g1ek9PG1xWBAw1"
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(webhook_url, data=json.dumps(message), headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❗ Failed to send Teams notification: {e}")

# ====================
# DAG Definition
# ====================
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'kbu1_mtc_dies_pipeline',
    default_args=default_args,
    description='ETL pipeline untuk Maintenance Dies',
    schedule='0 9 * * *',  # setiap jam 6 pagi
    catchup=False,
)

# ====================
# Path Configuration
# ====================
RAW_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Maintenance Dept/Maintenance Dies/Excel Online/kbu1_maintenance_mtr_bronze.xlsx"
BRONZE_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Maintenance Dept/Maintenance Dies/Extract Data/kbu1_mtc_dies_report_bronze.xlsx"
SILVER_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Maintenance Dept/Maintenance Dies/Silver Data/kbu1_mtc_dies_report_silver.xlsx"
TABLE_BRONZE = "kbu1_mtc_dies_bronze"
TABLE_SILVER = "kbu1_mtc_dies_silver"

# ====================
# Tasks
# ====================
def setup_postgres():
    print("🔧 Setup PostgreSQL connection check")
    get_postgres_connection()

def extract_mtc_dies():
    extractor = MTCDiesExtractor(RAW_PATH, BRONZE_PATH)
    extractor.extract()
    save_excel_to_postgres(BRONZE_PATH, TABLE_BRONZE, 'bronze')

def transform_mtc_dies():
    transformer = MTCDiesTransformer(BRONZE_PATH, SILVER_PATH)
    transformer.transform()
    save_excel_to_postgres(SILVER_PATH, TABLE_SILVER, 'silver')

# ====================
# DAG Flow
# ====================
t0 = PythonOperator(
    task_id='setup_postgres',
    python_callable=setup_postgres,
    dag=dag,
    on_failure_callback=notify_teams
)

t1 = PythonOperator(
    task_id='extract_mtc_dies',
    python_callable=extract_mtc_dies,
    dag=dag,
    execution_timeout=timedelta(minutes=15),
    on_failure_callback=notify_teams
)

t2 = PythonOperator(
    task_id='transform_mtc_dies',
    python_callable=transform_mtc_dies,
    dag=dag,
    execution_timeout=timedelta(minutes=10),
    on_failure_callback=notify_teams
)

t0 >> t1 >> t2
