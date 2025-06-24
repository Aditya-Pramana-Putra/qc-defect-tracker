import pandas as pd
import os

class EksternalProblemTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.df = None

    def load_data(self):
        print(f"📥 Memuat data dari: {self.input_path}")
        self.df = pd.read_excel(self.input_path)
        print(f"📊 Data dimuat: {len(self.df)} baris")

    def clean_and_transform(self):
        if self.df is None:
            raise ValueError("Data belum dimuat. Jalankan load_data() terlebih dahulu.")

        # Hapus duplikat jika seluruh kolomnya sama persis
        initial_rows = len(self.df)
        self.df.drop_duplicates(inplace=True)
        removed_duplicates = initial_rows - len(self.df)
        print(f"🧹 {removed_duplicates} duplikat dihapus")

        # Strip whitespace dari semua kolom object (string)
        str_cols = self.df.select_dtypes(include=['object']).columns
        self.df[str_cols] = self.df[str_cols].apply(lambda x: x.str.strip())

        # Bersihkan nama kolom (jika diperlukan bisa rename juga di sini)
        self.df.columns = self.df.columns.str.strip()

        print("✅ Transformasi selesai")

    def save_to_excel(self):
        if self.df is not None:
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            self.df.to_excel(self.output_path, index=False)
            print(f"💾 Data disimpan ke: {self.output_path}")
        else:
            raise ValueError("Data belum tersedia untuk disimpan.")