import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path

class BaseExtractor(ABC):
    def __init__(self, file_path: str, date_column: str, part_name_column: str = "part_name"):
        self.file_path = Path(file_path)
        self.date_column = date_column
        self.part_name_column = part_name_column
        self.df = pd.DataFrame()

    def load_data(self):
        self.df = pd.read_excel(self.file_path, parse_dates=[self.date_column])
        self.df.dropna(how='all', inplace=True)
        self.df = self.df.dropna(subset=[self.date_column, self.part_name_column])
        self.df[self.part_name_column] = self.df[self.part_name_column].astype(str).str.strip()

    def convert_columns(self, columns):
        for col in columns:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
            self.df.loc[self.df[col] < 0, col] = pd.NA
            self.df[col] = self.df[col].astype("Int64")

        for col in columns:
            if pd.api.types.is_integer_dtype(self.df[col]):
                self.df[col] = self.df[col].fillna(0)

    @abstractmethod
    def process(self):
        pass

    def get_dataframe(self):
        return self.df


class WeldingDataExtractor(BaseExtractor):
    def __init__(self, file_path: str):
        super().__init__(file_path, date_column="production_date")
        self.quantity_columns = [
            'shift1_qty_dom', 
            'shift1_qty_kd', 
            'act_shift1_qty_dom', 
            'act_shift1_qty_kd', 
            'shift2_qty', 
            'act_shift2_qty'
        ]

    def process(self):
        self.load_data()
        self.convert_columns(self.quantity_columns)
        self.df["is_valid_bronze"] = True
        print(self.df.dtypes)
        print("Jumlah baris setelah validasi (Welding):", len(self.df))


class PressDataExtractor(BaseExtractor):
    def __init__(self, file_path: str):
        super().__init__(file_path, date_column="production_date")
        self.quantity_columns = ['target']

    def process(self):
        self.load_data()
        self.convert_columns(self.quantity_columns)
        self.df["is_valid_bronze"] = True
        print(self.df.dtypes)
        print("Jumlah baris setelah validasi (Press):", len(self.df))


def combine_extractors(extractors):
    combined_df = pd.concat([ext.get_dataframe() for ext in extractors], ignore_index=True)
    return combined_df


