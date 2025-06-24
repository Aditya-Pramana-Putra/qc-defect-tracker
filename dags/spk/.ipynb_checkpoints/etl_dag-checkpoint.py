from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from etl_spk.extract import WeldingDataExtractor, PressDataExtractor, combine_extractors
from etl_spk.transform import SPKDataTransformer

# ==========================
# PostgreSQL Config
# ==========================
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
            raise FileNotFoundError(f"❌ Excel file tidak ditemukan: {excel_path}")
        
        df = pd.read_excel(excel_path)

        if df.empty:
            raise ValueError("❌ DataFrame kosong, tidak ada data untuk disimpan.")

        df['created_at'] = datetime.utcnow()
        if layer == 'silver':
            df['updated_at'] = datetime.utcnow()

        engine = get_sqlalchemy_engine()
        if_exists = 'replace' if layer == 'bronze' else 'append'

        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False, method='multi')

        print(f"✅ Data berhasil disimpan ke tabel {table_name} ({layer})")
    except Exception as e:
        print(f"❌ Gagal simpan ke PostgreSQL: {e}")
        raise

# ==========================
# Teams Notification
# ==========================
def notify_teams(context):
    task = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task.task_id
    execution_date = context.get("ts")
    try_number = task.try_number

    base_url = "http://localhost:8080"
    log_url = f"{base_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"

    webhook_url = "https://karyabb.webhook.office.com/webhookb2/ee565114-3427-4bac-a7f4-880b13d2fa79@d5318fb6-ef66-45f2-877a-95d4592b8d26/IncomingWebhook/11397220f3514fb08a2d95ba4cd53142/67bf709c-b2a9-406f-9f42-fdacf4592621/V2ouhl16DpFqga8ldF6vt8cindGp2N1_g1ek9PG1xWBAw1"

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

    try:
        requests.post(webhook_url, data=json.dumps(message), headers={'Content-Type': 'application/json'})
    except Exception as e:
        print(f"❗ Teams notification gagal: {e}")

# ==========================
# Path Konfigurasi
# ==========================
BRONZE_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/PPIC Dept/Extract Data/kbu1_ppic_spk_combined_bronze.xlsx"
SILVER_PATH = "/opt/airflow/sharepoint_defect_tracker/KBU1/PPIC Dept/Silver Data/kbu1_ppic_spk_combined_silver.xlsx"
TABLE_BRONZE = "kbu1_ppic_spk_bronze"
TABLE_SILVER = "kbu1_ppic_spk_silver"

# ==========================
# Default Argumen
# ==========================
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "kbu1_ppic_spk_pipeline",
    default_args=default_args,
    description="ETL pipeline untuk data SPK PPIC",
    schedule='0 9 * * *',  # Setiap jam 9 pagi
    catchup=False,
)

# ==========================
# Task Functions
# ==========================
def setup_postgres():
    print("🔧 Setup PostgreSQL untuk data SPK")
    get_postgres_connection()

def extract_spk_raw_data():
    print("🛠 Menjalankan ekstraksi SPK Welding dan Press")

    welding_file = "/opt/airflow/sharepoint_defect_tracker/KBU1/PPIC Dept/Excel Online/kbu1_ppic_spk_welding_bronze.xlsx"
    press_file = "/opt/airflow/sharepoint_defect_tracker/KBU1/PPIC Dept/Excel Online/kbu1_ppic_spk_press_bronze.xlsx"
    output_file = BRONZE_PATH  # yaitu kbu1_ppic_spk_combined_bronze.xlsx

    welding_extractor = WeldingDataExtractor(welding_file)
    welding_extractor.process()

    press_extractor = PressDataExtractor(press_file)
    press_extractor.process()

    df_combined = combine_extractors([welding_extractor, press_extractor])

    if "target" in df_combined.columns:
        df_combined["target"] = pd.to_numeric(df_combined["target"], errors="coerce").fillna(0).astype("Int64")

    df_combined.to_excel(output_file, index=False)
    print(f"📁 Data gabungan berhasil disimpan ke: {output_file}")

def load_spk_bronze():
    print("📥 Membaca file Excel Bronze SPK")
    save_excel_to_postgres(BRONZE_PATH, TABLE_BRONZE, 'bronze')

def transform_spk_silver():
    print("⚙️ Transformasi Bronze ➜ Silver untuk SPK")
    transformer = SPKDataTransformer(BRONZE_PATH, SILVER_PATH)
    transformer.run()
    save_excel_to_postgres(SILVER_PATH, TABLE_SILVER, 'silver')

# ==========================
# Tasks Definition
# ==========================
t0 = PythonOperator(
    task_id="setup_postgres",
    python_callable=setup_postgres,
    on_failure_callback=notify_teams,
    dag=dag
)

t1 = PythonOperator(
    task_id="extract_spk_raw_data",
    python_callable=extract_spk_raw_data,
    on_failure_callback=notify_teams,
    dag=dag,
)

t2 = PythonOperator(
    task_id="load_spk_bronze",
    python_callable=load_spk_bronze,
    on_failure_callback=notify_teams,
    dag=dag
)

t3 = PythonOperator(
    task_id="transform_spk_silver",
    python_callable=transform_spk_silver,
    on_failure_callback=notify_teams,
    dag=dag
)

# ==========================
# Task Flow
# ==========================
t0 >> t1 >> t2 >> t3
