import pandas as pd
from pathlib import Path

class MachineTransformer:
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

    def split_capacity_column(self):
        if 'capacity' in self.df.columns:
            # Bagi menjadi angka dan satuan
            self.df[['num_of_capacity', 'capacity_unit']] = self.df['capacity'].str.extract(r'(\d+)\s*(\D+)?')
            self.df['num_of_capacity'] = pd.to_numeric(self.df['num_of_capacity'], errors='coerce').fillna(0).astype(int)
            self.df['capacity_unit'] = self.df['capacity_unit'].fillna('UNKNOWN')
            self.df.drop(columns=['capacity'], inplace=True)
        return self

    def fill_missing_data(self):
        # Isi NaN untuk kolom object dengan 'UNKNOWN'
        object_cols = self.df.select_dtypes(include='object').columns
        self.df[object_cols] = self.df[object_cols].fillna('UNKNOWN')

        # Isi NaN untuk kolom numerik dengan 0, lalu ubah ke int
        numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
        self.df[numeric_cols] = self.df[numeric_cols].fillna(0).astype(int)

        return self

    def reorder_columns(self):
        # Optional: urutkan kolom supaya lebih rapi
        cols = [
            'line', 'machine_no', 'machine_name', 'input_power', 'capacity', 
            'num_of_capacity', 'capacity_unit', 'maker', 'qty', 
            'origine_source', 'kva', 'teoritis_kva', 'remark'
        ]
        self.df = self.df[[col for col in cols if col in self.df.columns]]
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
                .split_capacity_column()
                .fill_missing_data()
                .reorder_columns()
                .save_to_excel()
        )