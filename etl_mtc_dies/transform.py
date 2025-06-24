import pandas as pd
from pathlib import Path

class MTCDiesTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path)
        print(f"✅ Loaded data: {self.df.shape}")
        return self

    def drop_duplicates(self):
        before = self.df.shape[0]
        self.df.drop_duplicates(inplace=True)
        after = self.df.shape[0]
        print(f"🧹 Removed {before - after} duplicate rows")
        return self

    def convert_dates(self):
        default_date = pd.Timestamp("2000-01-01")

        for col in ['start_date', 'end_date']:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce', dayfirst=True)
                self.df[col] = self.df[col].fillna(default_date)

        return self

    def fill_missing_data(self):
        # Isi NaN untuk kolom object dengan "UNKNOWN"
        object_cols = self.df.select_dtypes(include='object').columns
        self.df[object_cols] = self.df[object_cols].fillna("UNKNOWN")

        # Isi NaN untuk kolom numerik dengan 0
        numeric_cols = self.df.select_dtypes(include=['int64', 'float64']).columns
        self.df[numeric_cols] = self.df[numeric_cols].fillna(0)

        return self

    def convert_status_to_binary(self):
        binary_cols = [
            'preventive_status', 'finish_production_status',
            'laporan_kerusakan_status', 'surat_permohonan_status',
            'activity_status'
        ]
        for col in binary_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
                self.df[col] = (self.df[col] != 0).astype(int)
        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Transformed data saved to: {self.output_path}")
        return self

    def transform(self):
        return (
            self.load_data()
                .drop_duplicates()
                .convert_dates()
                .fill_missing_data()
                .convert_status_to_binary()
                .save_to_excel()
        )
