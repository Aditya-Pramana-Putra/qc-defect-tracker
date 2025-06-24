import pandas as pd
import os

class EksternalProblemExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.df = None

    def load_and_clean_data(self):
        print(f"📥 Membaca data dari: {self.input_path}")
        self.df = pd.read_excel(self.input_path)

        # Buang baris yang kosong seluruhnya
        self.df.dropna(how='all', inplace=True)

        # Bersihkan nama kolom
        self.df.columns = self.df.columns.str.strip()

        # Hapus karakter newline di kolom tertentu
        if 'claim_no/issue_no' in self.df.columns:
            self.df['claim_no/issue_no'] = self.df['claim_no/issue_no'].astype(str).str.replace(r'\n', '', regex=True)

        print(f"✅ Data berhasil dibaca: {len(self.df)} baris")

    def save_to_excel(self):
        if self.df is not None:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            self.df.to_excel(self.output_path, index=False)
            print(f"💾 Data disimpan ke: {self.output_path}")
        else:
            raise ValueError("Data belum tersedia.")