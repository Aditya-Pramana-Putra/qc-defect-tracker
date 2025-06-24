# extract.py

import pandas as pd
from pathlib import Path

class MTCExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path)
        return self

    def drop_full_duplicates(self):
        self.df.drop_duplicates(inplace=True)
        return self

    def convert_dtypes(self):
        self.df["section"] = self.df["section"].astype(str)
        self.df["machine_no"] = self.df["machine_no"].astype(str)
        self.df["machine_name"] = self.df["machine_name"].astype(str)

        # Konversi tanggal
        self.df["start_date"] = pd.to_datetime(self.df["start_date"], errors='coerce')
        self.df["end_date"] = pd.to_datetime(self.df["end_date"], errors='coerce')

        self.df["problem"] = self.df["problem"].astype(str)
        self.df["corrective_action"] = self.df["corrective_action"].astype(str)
        self.df["downtime_y/n"] = self.df["downtime_y/n"].astype(str)
        self.df["operator"] = self.df["operator"].astype(str)

        # Konversi ke numerik
        self.df["work_hours_per_minute"] = pd.to_numeric(self.df["work_hours_per_minute"], errors='coerce')

        self.df["no_lk"] = self.df["no_lk"].astype(str)
        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Bronze file saved to: {self.output_path}")
        return self

if __name__ == "__main__":
    # Contoh penggunaan
    input_file = r"C:\Users\AdityaPramanaPutra\OneDrive - KARYA BAHANA GROUP\KBU1 Data Gathering - KBU1\Maintenance Dept\Logbook LK\Excel Online\kbu1_mtc_lk_mesin_req.xlsx"
    output_file = r"C:\Users\AdityaPramanaPutra\OneDrive - KARYA BAHANA GROUP\KBU1 Data Gathering - KBU1\Maintenance Dept\Logbook LK\Extract Data\kbu1_mtc_lk_mesin_bronze.xlsx"

    extractor = MTCExtractor(input_file, output_file)
    extractor.load_data()\
             .drop_full_duplicates()\
             .convert_dtypes()\
             .save_to_excel()
