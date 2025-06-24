import pandas as pd
from pathlib import Path


class DeliveryDataExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path)
        return self

    def clean_data(self):
        # Drop all-empty rows
        self.df.dropna(how='all', inplace=True)
        return self

    def cast_float_to_int(self):
        float_cols = self.df.select_dtypes(include='float').columns
        self.df[float_cols] = self.df[float_cols].fillna(0).astype(int)
        return self

    def validate_bronze(self):
        def validate_row(row):
            required_cols = ['part_no', 'part_name', 'date', 'sheet', 'status']
            for col in required_cols:
                if pd.isna(row[col]) or str(row[col]).strip() == "":
                    return False
            if not isinstance(row['date'], pd.Timestamp):
                return False
            if row['total_plan'] < 0 or row['total_actual'] < 0:
                return False
            return True

        self.df['is_valid_bronze'] = self.df.apply(validate_row, axis=1)
        return self

    def save(self):
        self.df.to_excel(self.output_path, index=False)
        return self

    def run(self):
        return (self
                .load_data()
                .clean_data()
                .cast_float_to_int()
                .validate_bronze()
                .save()
                )
