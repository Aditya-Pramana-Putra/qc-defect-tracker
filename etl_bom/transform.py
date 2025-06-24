import pandas as pd
from pathlib import Path

class BOMDataTransformer:
    def __init__(self, input_path: str):
        self.input_path = Path(input_path)
        self.df = None

    def load_bronze_data(self):
        self.df = pd.read_excel(self.input_path)
        return self

    def filter_valid_rows(self):
        # Hanya ambil baris yang valid
        if "is_valid_bronze" in self.df.columns:
            self.df = self.df[self.df["is_valid_bronze"] == True]
        return self

    def save_to_excel(self, output_path: str):
        self.df.to_excel(output_path, index=False)
        return self
