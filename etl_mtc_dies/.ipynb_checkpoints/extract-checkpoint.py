import pandas as pd
from pathlib import Path

class MTCDiesExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path, header=0)
        print(f"✅ Loaded data with shape: {self.df.shape}")
        return self

    def drop_duplicates(self):
        initial_shape = self.df.shape
        self.df.drop_duplicates(inplace=True)
        print(f"🧹 Dropped duplicates: {initial_shape[0] - self.df.shape[0]} rows removed")
        return self

    def convert_types(self):
        # Konversi ke datetime
        for col in ['start_date', 'end_date']:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce', dayfirst=True)

        # Konversi kolom status menjadi 0 / 1
        bool_cols = [
            'preventive_status', 'finish_production_status',
            'laporan_kerusakan_status', 'surat_permohonan_status',
            'activity_status'
        ]
        for col in bool_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')  # pastikan numerik
                self.df[col] = self.df[col].fillna(0)                        # isi NaN dengan 0 dulu
                self.df[col] = (self.df[col] != 0).astype(int)              # ubah ke 0 / 1


        # Opsional: pastikan kolom object yang penting tetap string
        object_cols = ['model', 'part_no', 'part_name', 'process', 'stroke', 
                       'problem', 'category', 'analysis', 'action_plan', 
                       'pic action', 'repair_status', 'shift', 'no_lk']
        for col in object_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str)

        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Cleaned data saved to: {self.output_path}")
        return self

    def extract(self):
        return (
            self.load_data()
                .drop_duplicates()
                .convert_types()
                .save_to_excel()
        )
    