import pandas as pd
from pathlib import Path

class StockSparePartExtractor:
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

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        print(f"✅ Cleaned data saved to: {self.output_path}")
        return self

    def extract(self):
        return (
            self.load_data()
                .drop_duplicates()
                .save_to_excel()
        )