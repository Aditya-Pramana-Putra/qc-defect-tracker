# transform.py

import pandas as pd
from pathlib import Path

class MTCTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_bronze_data(self):
        self.df = pd.read_excel(self.input_path)
        return self

    def clean_and_standardize(self):
        df = self.df

        # 1. Hapus duplikat
        df.drop_duplicates(inplace=True)

        # 2. Standarisasi teks: trim dan uppercase
        str_cols = [
            "section", "machine_no", "machine_name", "problem",
            "corrective_action", "downtime_y/n", "operator", "no_lk"
        ]
        for col in str_cols:
            df[col] = df[col].astype(str).str.strip().str.upper().replace("NAN", pd.NA)

        # 3. Parse tanggal
        df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce", dayfirst=True)
        df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce", dayfirst=True)

        # 4. Buang baris yang tidak punya start_date
        df = df[df["start_date"].notna()].copy()

        # 5. Konversi jam kerja ke numerik
        df["work_hours_per_minute"] = pd.to_numeric(df["work_hours_per_minute"], errors="coerce")

        # 6. Isi missing values
        #    - Object columns → "UNKNOWN"
        object_cols = df.select_dtypes(include='object').columns
        df[object_cols] = df[object_cols].astype(object).fillna("UNKNOWN")

        #    - Numeric columns → 0
        numeric_cols = df.select_dtypes(include='number').columns
        df[numeric_cols] = df[numeric_cols].fillna(0).astype(int)

        # 7. Optional: isi end_date jika masih kosong dengan start_date
        df["end_date"] = df["end_date"].fillna(df["start_date"])

        # Final assignment
        self.df = df
        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Silver file saved to: {self.output_path}")
        return self

if __name__ == "__main__":
    # Contoh path input & output
    input_file = r"C:\Users\AdityaPramanaPutra\OneDrive - KARYA BAHANA GROUP\KBU1 Data Gathering - KBU1\Maintenance Dept\Logbook LK\Extract Data\kbu1_mtc_lk_mesin_bronze.xlsx"
    output_file = r"C:\Users\AdityaPramanaPutra\OneDrive - KARYA BAHANA GROUP\KBU1 Data Gathering - KBU1\Maintenance Dept\Logbook LK\Silver Data\kbu1_mtc_lk_mesin_silver.xlsx"

    transformer = MTCTransformer(input_file, output_file)
    transformer.load_bronze_data()\
               .clean_and_standardize()\
               .save_to_excel()
