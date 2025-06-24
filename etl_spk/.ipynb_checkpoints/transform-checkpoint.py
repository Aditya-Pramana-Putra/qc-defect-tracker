import pandas as pd

class SPKDataTransformer:
    def __init__(self, bronze_path: str, silver_path: str):
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.bronze_path)
        return self

    def drop_empty_rows(self):
        self.df.dropna(how='all', inplace=True)
        return self

    def cast_quantity_columns(self):
        qty_cols = [
            "shift1_qty_dom", "shift1_qty_kd",
            "act_shift1_qty_dom", "act_shift1_qty_kd",
            "shift2_qty", "act_shift2_qty"
        ]
        for col in qty_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors="coerce").fillna(0).astype("Int64")
        return self

    def drop_missing_required_columns(self):
        required_cols = ["production_date", "part_name", "line"]
        for col in required_cols:
            if col in self.df.columns:
                self.df = self.df[self.df[col].notna()]
        return self

    def fill_missing_values(self):
        for col in self.df.columns:
            if self.df[col].dtype in ["float64", "object"]:
                self.df[col] = self.df[col].fillna("UNKNOWN")
        return self

    def drop_unnecessary_columns(self):
        self.df.drop(columns=["is_valid_bronze"], errors="ignore", inplace=True)
        return self

    def transform(self):
        return (self
                .load_data()
                .drop_empty_rows()
                .cast_quantity_columns()
                .drop_missing_required_columns()
                .fill_missing_values()
                .drop_unnecessary_columns()
                .df)

    def run(self):
        """Run the full transform and save to the silver path"""
        print(f"🔁 Transformasi dimulai dari: {self.bronze_path}")
        self.transform()
        print(f"💾 Menyimpan hasil ke: {self.silver_path}")
        self.df.to_excel(self.silver_path, index=False)
        print("✅ Transformasi selesai.")
