import pandas as pd
from pathlib import Path

class BOMDataExtractor:
    def __init__(self, input_path: str):
        self.input_path = Path(input_path)
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.input_path)
        return self

    def drop_empty_rows(self):
        self.df.dropna(how='all', inplace=True)
        return self

    def drop_full_duplicates(self):
        self.df.drop_duplicates(inplace=True)
        return self

    def fill_missing_values(self):
        text_cols = self.df.select_dtypes(include='object').columns
        num_cols = self.df.select_dtypes(include=['float64', 'int64']).columns

        self.df[text_cols] = self.df[text_cols].fillna("Unknown")
        self.df[num_cols] = self.df[num_cols].fillna(0)
        return self

    def cast_numeric_columns(self):
        int_cols = [
            "blank_size_cavity_pcs", "qty_part_strip", "qty_part_pcs",
            "qty_part_total", "production_volume_plan_car_per_month",
            "material_usage_sheet_per_month"
        ]
        for col in int_cols:
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(int)
        return self

    def validate_rows(self):
        def is_valid(row):
            numeric_checks = [
                row["supply_material_thickness_mm"] >= 0,
                row["supply_material_width_mm"] >= 0,
                row["supply material_length_mm"] >= 0,
                row["weight_kg"] >= 0,
                row["blank_size_weight_kg"] >= 0,
                row["qty_part_total"] > 0,
                row["production_volume_plan_car_per_month"] >= 0
            ]
            return all(numeric_checks)

        self.df["is_valid_bronze"] = self.df.apply(is_valid, axis=1)
        return self

    def save_to_excel(self, output_path: str):
        self.df.to_excel(output_path, index=False)
        return self
