import pandas as pd
from pathlib import Path

class MTCJigsExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path)
        return self

    def clean_data(self):
        # Hapus semua baris yang kosong
        self.df.dropna(how='all', inplace=True)

        # Hapus duplikat berdasarkan semua kolom
        self.df.drop_duplicates(inplace=True)

        return self

    def convert_dtypes(self):
        # Konversi tipe data
        self.df = self.df.astype({
            'model': 'string',
            'part_no': 'string',
            'part_name': 'string',
            'process': 'string',
            'stroke': 'string',
            'preventive_status': 'Int64',
            'finish_production_status': 'Int64',
            'laporan_kerusakan_status': 'Int64',
            'surat_permohonan_status': 'string',  # Awalnya object, bisa berisi "0"/"1" string
            'activity_status': 'Int64',
            'week': 'string',
            'problem': 'string',
            'category': 'string',
            'analisa': 'string',
            'action_plan': 'string',
            'pic_action': 'string',
            'trial': 'string',
            'qc_judgement_ok_or_nc': 'string',
            'qc_judgement_name': 'string',
            'qc_judgement_ttd': 'float64',
            'remarks': 'string'
        })

        # Konversi tanggal
        self.df['start_date'] = pd.to_datetime(self.df['start_date'], errors='coerce', dayfirst=True)
        self.df['end_date'] = pd.to_datetime(self.df['end_date'], errors='coerce', dayfirst=True)

        # Konversi waktu kerja ke float
        self.df['work_hour_per_menit'] = pd.to_numeric(self.df['work_hour_per_menit'], errors='coerce')

        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Data disimpan di: {self.output_path}")

    def run(self):
        return (
            self.load_data()
                .clean_data()
                .convert_dtypes()
        )