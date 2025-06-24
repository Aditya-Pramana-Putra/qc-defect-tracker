# transform.py

import pandas as pd

class InternalProblemTransformer:
    def __init__(self, extracted_path, partlist_csv_path):
        self.extracted_path = extracted_path
        self.partlist_csv_path = partlist_csv_path
        self.df = None

    def load_data(self):
        self.df = pd.read_excel(self.extracted_path)

    def combine_date_columns(self):
        self.df['production_date'] = pd.to_datetime(
            self.df['dd'].astype(str).str.zfill(2) + '-' +
            self.df['mm'].astype(str).str.zfill(2) + '-' +
            ('20' + self.df['yy'].astype(str)),
            format='%d-%m-%Y'
        )
        self.df.drop(columns=['dd', 'mm', 'yy'], inplace=True)

    def fill_created_date(self):
        self.df['created_date'] = pd.to_datetime(self.df['created_date'], errors='coerce').ffill()

    def convert_column_types(self):
        for col in self.df.columns[12:16]:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('Int64')
        self.df['suplier_or_responsible'] = self.df['suplier_or_responsible'].astype(object)

    def join_partlist(self):
        partlist_df = pd.read_csv(self.partlist_csv_path)[['part_no', 'part_name']].rename(columns={'part_name': 'part_name1'})
        self.df['part_name_backup'] = self.df['part_name']
        merged_df = pd.merge(self.df, partlist_df, on='part_no', how='left')
        merged_df['part_name1'] = merged_df['part_name1'].fillna(merged_df['part_name_backup'])
        merged_df = merged_df[~merged_df['part_name1'].isna()].reset_index(drop=True)

        merged_df.drop(columns=['part_name', 'part_name_backup'], inplace=True)
        cols = list(merged_df.columns)
        idx = cols.index('part_no')
        cols.remove('part_name1')
        cols.insert(idx + 1, 'part_name1')
        merged_df = merged_df[cols]
        merged_df.rename(columns={'part_name1': 'part_name'}, inplace=True)

        self.df = merged_df

    def clean_and_enrich_data(self):
        self.df.drop_duplicates(inplace=True)
        self.df['month'] = self.df['production_date'].dt.year.astype(str) + ' - Month ' + self.df['production_date'].dt.month.astype(str)

        roman_to_int = {
            'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
            'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10,
            'XI': 11, 'XII': 12, 'XIII': 13, 'XIV': 14, 'XV': 15
        }
        self.df['week'] = self.df['week'].map(roman_to_int)
        self.df['shift'] = self.df['shift'].map(roman_to_int)

        self.df[['dop_repair', 'dop_scrap', 'dop_total', 'qty_check']] = self.df[
            ['dop_repair', 'dop_scrap', 'dop_total', 'qty_check']
        ].fillna(0).astype(int)

        self.df[['suplier_or_responsible', '4m_factor']] = self.df[
            ['suplier_or_responsible', '4m_factor']
        ].fillna('UNKNOWN')

    def save_to_excel(self, output_path):
        self.df.to_excel(output_path, index=False)
