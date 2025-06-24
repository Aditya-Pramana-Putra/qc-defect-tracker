from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import json
import requests
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import psycopg2
from psycopg2.extras import execute_values
import psycopg2
from airflow.hooks.base import BaseHook


sys.path.append('/opt/airflow')

from etl_defect_tracker.extract import ExcelTableExtractor
from etl_defect_tracker.transform import DefectTrackerProcessor

# ==========================
# PostgreSQL Configuration
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
    return None

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
    """Save Excel data to PostgreSQL (tanpa created_at dan updated_at)"""
    try:
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"File Excel tidak ditemukan: {excel_path}")

        df = pd.read_excel(excel_path)

        if df.empty:
            raise ValueError("DataFrame kosong. Tidak ada data untuk disimpan.")

        engine = get_sqlalchemy_engine()

        # Jika bronze: replace hanya untuk development sementara, production disarankan append
        if_exists = 'replace' if layer == 'bronze' else 'append'

        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False, method='multi')

        print(f"✅ Data berhasil disimpan ke PostgreSQL table: {table_name}")
        print(f"📊 Total rows: {len(df)}")

    except Exception as e:
        print(f"❌ Error saving to PostgreSQL: {str(e)}")
        raise

# ==========================
# Microsoft Teams Alert
# ==========================
def notify_teams(context):
    dag = context.get('dag')
    task = context.get('task')
    ti = context.get('ti') or context.get('task_instance')  # fallback
    
    dag_id = dag.dag_id if dag else "Unknown DAG"
    task_id = task.task_id if task else "Unknown Task"
    execution_date = context.get('ts') or "Unknown Time"
    try_number = ti.try_number if ti else "N/A"

    # Buat log URL manual (ubah base_url sesuai dengan deployment Anda)
    base_url = "http://localhost:8080"  # Ganti sesuai domain Airflow Anda
    log_url = f"{base_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"

    # URL webhook Teams
    webhook_url = "https://karyabb.webhook.office.com/webhookb2/ee565114-3427-4bac-a7f4-880b13d2fa79@d5318fb6-ef66-45f2-877a-95d4592b8d26/IncomingWebhook/11397220f3514fb08a2d95ba4cd53142/67bf709c-b2a9-406f-9f42-fdacf4592621/V2ouhl16DpFqga8ldF6vt8cindGp2N1_g1ek9PG1xWBAw1"

    # Pesan Teams
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

    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(webhook_url, data=json.dumps(message), headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"❗ Failed to send Teams notification: {e}")

# ==========================
# Default Arguments
# ==========================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'kbu1_qc_defect_tracker_pipeline',
    default_args=default_args,
    description='ETL pipeline untuk QC Defect Tracker with PostgreSQL',
    schedule='0 9 * * *',  # Setiap jam 9 pagi
    catchup=False,
)

# ==========================
# Konfigurasi Path
# ==========================
RAW_TO_BRONZE = [
    (
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Data/Data_Format_1/*.csv',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Metadata/metadata_format_1.yaml',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_1_bronze.xlsx',
        'kbu1_qc_defect_tracker_format_1_bronze'
    ),
    (
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Data/Data_Format_3/*.csv',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Metadata/metadata_format_3.yaml',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_3_bronze.xlsx',
        'kbu1_qc_defect_tracker_format_3_bronze'
    ),
]

BRONZE_TO_SILVER = [
    (
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_1_bronze.xlsx',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_1_silver.xlsx',
        'kbu1_qc_defect_tracker_format_1_silver'
    ),
    (
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_2_bronze.xlsx',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_2_silver.xlsx',
        'kbu1_qc_defect_tracker_format_2_silver'
    ),
    (
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_3_bronze.xlsx',
        '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_3_silver.xlsx',
        'kbu1_qc_defect_tracker_format_3_silver'
    )
]

REFERENCE_CSV = '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/kbu1_engineering_partlist_silver.csv'


# ==========================
# Task Functions
# ==========================
def setup_postgres():
    """Setup PostgreSQL tables"""
    print("🔧 [SETUP] Membuat/Memverifikasi tabel PostgreSQL...")
    get_postgres_connection()

def extract_data_raw():
    print("🔍 [LOG] Tahap Extract Data RAW dimulai (hanya log, tidak ada proses).")

def load_to_bronze_data():
    # Proses semua format dari list RAW_TO_BRONZE
    for folder_path, metadata_path, bronze_path, table_name in RAW_TO_BRONZE:
        print(f"📦 [ETL] Load dari RAW ➜ BRONZE\n  🔹 Folder: {folder_path}\n  🔹 Metadata: {metadata_path}")
        extractor = ExcelTableExtractor(folder_path=folder_path, metadata_path=metadata_path)
        extractor.load_and_combine()
        extractor.save_to_excel(bronze_path)
        print(f"✅ Bronze data tersimpan: {bronze_path}")
        
        # Simpan ke PostgreSQL
        save_excel_to_postgres(bronze_path, table_name, 'bronze')

    # 🔧 Proses khusus untuk format_2
    print(f"\n🔄 [ETL] Load format_2 secara terpisah")
    
    folder_path = '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_1_bronze.xlsx'
    metadata_path = '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Metadata/metadata_format_2.yaml'
    bronze_path = '/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_2_bronze.xlsx'
    table_name = 'kbu1_qc_defect_tracker_format_2_bronze'


    extractor = ExcelTableExtractor(folder_path=folder_path, metadata_path=metadata_path)
    extractor.load_and_combine()
    print("🔧 [Transform] Melakukan unpivot (melt) untuk format_2.")
    extractor.transform_defect_data()
    extractor.save_to_excel(bronze_path)
    print(f"✅ Bronze data format_2 tersimpan: {bronze_path}")
    
    # Simpan ke PostgreSQL
    save_excel_to_postgres(bronze_path, table_name, 'bronze')

def read_data_bronze():
    print("📖 [LOG] Membaca Bronze Data (log only).")

def transform_data():
    for bronze_path, silver_path, table_name in BRONZE_TO_SILVER:
        print(f"🔄 [ETL] Transformasi dari BRONZE ➜ SILVER\n  🔹 Input: {bronze_path}\n  🔹 Output: {silver_path}")
        processor = DefectTrackerProcessor(bronze_path, REFERENCE_CSV, silver_path)
        
        # Jalankan proses utama
        processor.run()
        
        if "format_3" in silver_path:
            format1_path = "/opt/airflow/sharepoint_defect_tracker/KBU1/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_1_silver.xlsx"
            defect_cols = ['KEROPOS', 'KURANG', 'BOLONG', 'UNDERCUT', 'SPATTER', 'TIDAK TEPAT']
            
            # 🔍 DEBUG: Print kolom dari format 1 sebelum agregasi
            
            df_format1 = pd.read_excel(format1_path)
            print("📄 Kolom tersedia di format 1:", df_format1.columns.tolist())
            
            processor.aggregate_defect_data(format1_path, defect_cols)

            # Tambahan: Hapus baris semua defect = 0
            processor.remove_all_zero_defect_rows()
            
            # Simpan ulang setelah injeksi
            processor.save_to_excel()

        print(f"✅ Silver data tersimpan: {silver_path}")
        
        # Simpan ke PostgreSQL
        save_excel_to_postgres(silver_path, table_name, 'silver')

def load_to_silver_data():
    print("📁 [LOG] Silver data telah disimpan melalui proses transformasi dan PostgreSQL.")


# ==========================
# Define Tasks with Alerts
# ==========================
t0 = PythonOperator(
    task_id='setup_postgres',
    python_callable=get_postgres_connection,
    execution_timeout=timedelta(minutes=5),
    on_failure_callback=notify_teams,
    dag=dag
)

t1 = PythonOperator(
    task_id='extract_data_raw',
    python_callable=extract_data_raw,
    execution_timeout=timedelta(minutes=3),
    on_failure_callback=notify_teams,
    dag=dag
)

t2 = PythonOperator(
    task_id='load_to_bronze_data',
    python_callable=load_to_bronze_data,
    execution_timeout=timedelta(minutes=15),
    on_failure_callback=notify_teams,
    dag=dag
)

t3 = PythonOperator(
    task_id='read_data_bronze',
    python_callable=read_data_bronze,
    execution_timeout=timedelta(minutes=3),
    on_failure_callback=notify_teams,
    dag=dag
)

t4 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    execution_timeout=timedelta(minutes=15),
    on_failure_callback=notify_teams,
    dag=dag
)

t5 = PythonOperator(
    task_id='load_to_silver_data',
    python_callable=load_to_silver_data,
    execution_timeout=timedelta(minutes=3),
    on_failure_callback=notify_teams,
    dag=dag
)


# ==========================
# Define Task Flow
# ==========================
t0 >> t1 >> t2 >> t3 >> t4 >> t5 