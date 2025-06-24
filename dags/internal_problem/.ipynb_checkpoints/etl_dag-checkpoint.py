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

from etl_internal_problem.extract import ExcelTableExtractor
from etl_internal_problem.transform import InternalProblemTransformer

# ========== PostgreSQL Connection ==========
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
    if not os.path.exists(excel_path):
        raise FileNotFoundError(f"❌ File tidak ditemukan: {excel_path}")
    df = pd.read_excel(excel_path)
    if df.empty:
        raise ValueError("❌ Data kosong.")

    df['created_at'] = datetime.utcnow()
    if layer == 'silver':
        df['updated_at'] = datetime.utcnow()

    engine = get_sqlalchemy_engine()
    if_exists = 'replace' if layer == 'bronze' else 'append'
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False, method='multi')
    print(f"✅ Data berhasil disimpan ke PostgreSQL: {table_name}")

# ========== Teams Notification ==========
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
        print(f"❗ Gagal mengirim notifikasi Teams: {e}")

# ========== Config ==========
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    dag_id='kbu1_qc_internal_problem_pipeline',
    default_args=default_args,
    description='ETL pipeline untuk internal problem logbook',
    schedule='0 2 * * *',
    catchup=False,
)

# ========== Path & Table ==========
RAW_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Log Book Internal Problem/Internal Problem LogBook QC.xlsx"
EXTRACTED_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Log Book Internal Problem/Bronze Data/kbu1_qc_internal_problem_pilottesting_bronze.xlsx"
TRANSFORMED_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Log Book Internal Problem/Silver Data/kbu1_qc_internal_problem_pilottesting_silver.xlsx"
PARTLIST_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/kbu1_engineering_partlist_silver.csv"
TABLE_BRONZE = "kbu1_qc_internal_problem_bronze"
TABLE_SILVER = "kbu1_qc_internal_problem_silver"

# ========== Tasks ==========
def setup_postgres():
    print("🔧 Checking PostgreSQL connection...")
    get_postgres_connection()

def extract_internal_problem():
    extractor = ExcelTableExtractor(
        file_path=RAW_PATH,
        sheet_name="ALL",
        usecols="A:S"      
    )
    extractor.save_to_excel(EXTRACTED_PATH)
    save_excel_to_postgres(EXTRACTED_PATH, TABLE_BRONZE) 
    
def transform_internal_problem():
    transformer = InternalProblemTransformer(EXTRACTED_PATH, PARTLIST_PATH)
    transformer.load_data()
    transformer.combine_date_columns()
    transformer.fill_created_date()
    transformer.convert_column_types()
    transformer.join_partlist()
    transformer.clean_and_enrich_data()
    transformer.save_to_excel(TRANSFORMED_PATH)
    save_excel_to_postgres(TRANSFORMED_PATH, TABLE_SILVER)

# ========== DAG Flow ==========
t0 = PythonOperator(
    task_id='setup_postgres',
    python_callable=setup_postgres,
    dag=dag,
    on_failure_callback=notify_teams
)

t1 = PythonOperator(
    task_id='extract_internal_problem',
    python_callable=extract_internal_problem,
    dag=dag,
    on_failure_callback=notify_teams,
    execution_timeout=timedelta(minutes=15)
)

t2 = PythonOperator(
    task_id='transform_internal_problem',
    python_callable=transform_internal_problem,
    dag=dag,
    on_failure_callback=notify_teams,
    execution_timeout=timedelta(minutes=15)
)

t0 >> t1 >> t2
