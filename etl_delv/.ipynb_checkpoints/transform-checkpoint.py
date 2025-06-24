import pandas as pd
from pathlib import Path

class DeliveryDataTransformer:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path, parse_dates=["date"])
        return self

    def drop_empty_rows(self):
        self.df.dropna(how='all', inplace=True)
        return self

    def drop_duplicates(self):
        self.df.drop_duplicates(inplace=True)
        return self

    def fill_and_cast_numeric(self):
        numeric_cols = ["total_plan", "total_actual", "balance", "target_x_ft", "awal"]
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].fillna(0).astype(int)
        return self

    def sanitize_text_columns(self):
        text_cols = ["part_no", "part_name", "sheet", "status"]
        for col in text_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(str).str.strip()
        return self

    def add_shift_column(self):
        if "status" in self.df.columns:
            self.df["Shift"] = self.df["status"].apply(
                lambda x: "Shift 1" if "Shift 1" in x else (
                    "Shift 2" if "Shift 2" in x else "All"
                )
            )
        else:
            self.df["Shift"] = "All"
        return self

    def select_and_reorder_columns(self):
        expected_columns = [
            "part_no", "part_name", "value", "date", "status", "sheet",
            "total_plan", "total_actual", "balance", "target_x_ft", "awal", "Shift"
        ]
        available_columns = [col for col in expected_columns if col in self.df.columns]
        self.df = self.df[available_columns]
        return self

    def save_to_excel(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_excel(self.output_path, index=False)
        return self

    def transform(self):
        return (
            self.load_data()
                .drop_empty_rows()
                .drop_duplicates()
                .fill_and_cast_numeric()
                .sanitize_text_columns()
                .add_shift_column()
                .select_and_reorder_columns()
                .save_to_excel()
        )
