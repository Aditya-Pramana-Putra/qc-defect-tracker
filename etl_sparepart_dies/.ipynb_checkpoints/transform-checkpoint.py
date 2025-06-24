import pandas as pd
from pathlib import Path

class StockSparePartTransformer:
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

    def fill_missing_data(self):
        # Mengisi NaN untuk kolom bertipe object/string dengan "UNKNOWN"
        object_cols = self.df.select_dtypes(include='object').columns
        self.df[object_cols] = self.df[object_cols].fillna("UNKNOWN")

        # Mengisi NaN untuk kolom numerik dengan 0, lalu ubah ke int
        numeric_cols = self.df.select_dtypes(include=['float64', 'int64']).columns
        self.df[numeric_cols] = self.df[numeric_cols].fillna(0).astype(int)

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
                .fill_missing_data()
                .save_to_excel()
        )