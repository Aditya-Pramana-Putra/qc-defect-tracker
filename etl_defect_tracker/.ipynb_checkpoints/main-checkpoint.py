import logging
from etl_defect_tracker.extract import ExcelTableExtractor
from etl_defect_tracker.transform import DefectTrackerProcessor

# Konfigurasi logging (akan ditangkap Loki jika airflow_local_settings.py disiapkan)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# Konfigurasi path untuk ekstraksi (RAW ke BRONZE)
RAW_TO_BRONZE = [
    (
        '/app/onedrive/Quality Control Dept/Defect Tracker/Data/Data_Format_1/*.csv',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Metadata/format_1_metadata.json',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_1_bronze.xlsx'
    ),
    (
        '/app/onedrive/Quality Control Dept/Defect Tracker/Data/Data_Format_2/*.csv',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Metadata/format_2_metadata.json',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_2_bronze.xlsx'
    ),
    (
        '/app/onedrive/Quality Control Dept/Defect Tracker/Data/Data_Format_3/*.csv',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Metadata/format_3_metadata.json',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_3_bronze.xlsx'
    )
]

# Konfigurasi path untuk transformasi (BRONZE ke SILVER)
BRONZE_TO_SILVER = [
    (
        '/app/onedrive/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_1_bronze.xlsx',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_1_silver.xlsx'
    ),
    (
        '/app/onedrive/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_2_bronze.xlsx',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_2_silver.xlsx'
    ),
    (
        '/app/onedrive/Quality Control Dept/Defect Tracker/Bronze Data/kbu1_qc_defect_tracker_format_3_bronze.xlsx',
        '/app/onedrive/Quality Control Dept/Defect Tracker/Silver Data/kbu1_qc_defect_tracker_format_3_silver.xlsx'
    )
]

# Path ke referensi partlist
REFERENCE_CSV = '/app/onedrive/Data Storage from Fabric/Silver/KBU1/Engineering/kbu1_engineering_partlist_silver.csv'


def run_extraction():
    logger.info("🔍 Memulai proses ekstraksi data (RAW → BRONZE)...")
    for raw_glob_path, metadata_path, bronze_output_path in RAW_TO_BRONZE:
        try:
            extractor = ExcelTableExtractor(folder_path=raw_glob_path, metadata_path=metadata_path)
            extractor.load_and_combine()
            extractor.save_to_excel(output_path=bronze_output_path)
            logger.info(f"✅ Ekstraksi berhasil: {bronze_output_path}")
        except Exception as e:
            logger.error(f"❌ Gagal ekstraksi {bronze_output_path}: {e}")
    logger.info("🏁 Proses ekstraksi selesai.\n")


def run_transformation():
    logger.info("🔄 Memulai proses transformasi data (BRONZE → SILVER)...")
    for bronze_input_path, silver_output_path in BRONZE_TO_SILVER:
        try:
            processor = DefectTrackerProcessor(
                input_path=bronze_input_path,
                reference_csv=REFERENCE_CSV,
                output_path=silver_output_path
            )
            processor.run()
            logger.info(f"✅ Transformasi berhasil: {silver_output_path}")
        except Exception as e:
            logger.error(f"❌ Gagal transformasi {silver_output_path}: {e}")
    logger.info("🏁 Proses transformasi selesai.\n")


if __name__ == "__main__":
    run_extraction()
    run_transformation()
