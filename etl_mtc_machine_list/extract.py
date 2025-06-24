import pandas as pd
from pathlib import Path

class MachineExtractor:
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

    def convert_dtypes(self):
        # Tipe object/str
        str_cols = ['line', 'machine_no', 'machine_name', 'capacity', 'maker',  'remark']
        for col in str_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str)

        # Tipe float ke int (isi NaN dengan 0 dulu, lalu konversi ke int)
        float_cols = ['qty', 'input_power', 'kva','origine_source', 'teoritis_kva']
        for col in float_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0).astype(int)

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
                .convert_dtypes()
                .save_to_excel()
        )