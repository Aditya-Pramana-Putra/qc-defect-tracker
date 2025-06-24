import pandas as pd

class ExcelTableExtractor:
    def __init__(self, file_path, sheet_name, usecols="A:S"):
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.usecols = usecols
        self.df = None
        self._extract_table()

    def _extract_table(self):
        self.df = pd.read_excel(
            self.file_path,
            sheet_name=self.sheet_name,
            header=0,  
            usecols=self.usecols
        )

    def save_to_excel(self, output_path):
        self.df.to_excel(output_path, index=False)