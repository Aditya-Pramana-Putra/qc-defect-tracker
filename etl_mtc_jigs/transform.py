import pandas as pd
from pathlib import Path

class MTCJigsTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path, header=0)
        return self

    def clean_columns(self):
        # Drop kolom yang seluruh nilainya NaN
        self.df.dropna(axis=1, how='all', inplace=True)

        # Drop kolom tidak relevan (contoh: qc_judgement_ttd)
        if 'qc_judgement_ttd' in self.df.columns:
            self.df.drop(columns=['qc_judgement_ttd'], inplace=True)

        return self

    def convert_dates(self):
        default_date = pd.Timestamp("2000-01-01")

        if 'start_date' in self.df.columns:
            # Isi nilai kosong dengan tanggal default
            self.df['start_date'] = self.df['start_date'].fillna(default_date)

            # Parsing nilai ke datetime
            self.df['start_date'] = pd.to_datetime(
                self.df['start_date'], errors='coerce', dayfirst=True
            )

            # Isi NaT dengan tanggal default
            self.df['start_date'].fillna(default_date, inplace=True)

        if 'end_date' in self.df.columns:
            self.df['end_date'] = self.df['end_date'].fillna(default_date)
            self.df['end_date'] = pd.to_datetime(
                self.df['end_date'], errors='coerce', dayfirst=True
            )
            self.df['end_date'].fillna(default_date, inplace=True)

        return self


    def fill_missing_data(self):
        # Isi semua NaN untuk kolom object dengan "UNKNOWN"
        object_cols = self.df.select_dtypes(include=['object']).columns
        self.df[object_cols] = self.df[object_cols].fillna("UNKNOWN")

        # Isi semua NaN untuk kolom numerik dengan 0
        numeric_cols = self.df.select_dtypes(include=['int64', 'float64']).columns
        self.df[numeric_cols] = self.df[numeric_cols].fillna(0)

        return self

    def enforce_dtypes(self):
        # Paksa kolom tertentu agar bertipe int
        int_columns = [
            'preventive_status', 'finish_production_status',
            'laporan_kerusakan_status', 'activity_status'
        ]
        for col in int_columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(int)
        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Cleaned data saved to: {self.output_path}")
        return self

    def transform(self):
        return (
            self.load_data()
                .clean_columns()
                .convert_dates()
                .fill_missing_data()
                .enforce_dtypes()
                .save_to_excel()
        )
