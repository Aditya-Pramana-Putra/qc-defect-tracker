import pandas as pd
import numpy as np
import os
import re

class DefectTrackerProcessor:
    def __init__(self, input_path, reference_csv, output_path):
        self.input_path = input_path
        self.reference_csv = reference_csv
        self.output_path = output_path
        self.df_combined = None
        self.df_partlist = None

    def load_excel_file(self):
        self.df_combined = pd.read_excel(self.input_path)
        if 'Halaman' in self.df_combined.columns:
            self.df_combined.drop(columns=['Halaman'], inplace=True)

    def clean_unclear_characters(self):
        if 'TIDAK TEPAT' in self.df_combined.columns:
            self.df_combined['TIDAK TEPAT'] = (
                self.df_combined['TIDAK TEPAT']
                .astype(str)
                .str.upper()
                .str.replace('O', '0')
                .str.replace(r'[^A-Z0-9\s]', '', regex=True)
            )

    def drop_yield_production_column(self):
        if 'YIELD PRODUCTION' in self.df_combined.columns:
            self.df_combined.drop(columns=['YIELD PRODUCTION'], inplace=True)

    def clean_date_columns(self):
        for col in ['Tanggal', 'Bulan', 'Tahun']:
            self.df_combined[col] = self.df_combined[col].astype(str).str.extract(r'(\d+)', expand=False).fillna('00')

        self.df_combined['Production Date'] = pd.to_datetime(
            self.df_combined['Tanggal'].str.zfill(2) + '-' +
            self.df_combined['Bulan'].str.zfill(2) + '-' +
            ('20' + self.df_combined['Tahun'].str[-2:]),
            format='%d-%m-%Y',
            errors='coerce'
        )
        self.df_combined.drop(columns=['Tanggal', 'Bulan', 'Tahun'], inplace=True)

    @staticmethod
    def clean_part_no(part):
        if pd.isna(part): return part
        part = str(part).upper()
        part = re.sub(r'\bMO', 'M0', part)
        match = re.search(r'M0[^\s]*', part)
        return match.group(0) if match else ''

    def normalize_model_values(self):
        if 'Model' in self.df_combined.columns:
            self.df_combined['Model'] = self.df_combined['Model'].astype(str).str.upper().str.extract(r'(4L45W|5H45|5J45)', expand=False)

    def clean_columns(self):
        self.df_combined['Part No'] = self.df_combined['Part No'].apply(self.clean_part_no)
        # Tidak lagi membersihkan Part Name

    def drop_duplicates(self):
        self.df_combined = self.df_combined.drop_duplicates()


    def clean_welding_check_points(self):
        if 'Welding Check Points' in self.df_combined.columns:
            def extract_first_number(val):
                val = str(val)
                numbers = re.findall(r'\d+', val)
                return int(numbers[0]) if numbers else np.nan

            self.df_combined['Welding Check Points'] = self.df_combined['Welding Check Points'].apply(extract_first_number)
            self.df_combined['Welding Check Points'] = self.df_combined['Welding Check Points'].astype(float)
            self.df_combined.loc[self.df_combined['Welding Check Points'] == 0, 'Welding Check Points'] = np.nan

            def fill_missing_wcp(group):
                existing_values = group['Welding Check Points'].dropna().astype(int).tolist()
                full_range = list(range(1, 201))
                missing_values = [val for val in full_range if val not in existing_values]
                missing_idx = group[group['Welding Check Points'].isna()].index.tolist()
                for i, idx in enumerate(missing_idx):
                    if i < len(missing_values):
                        group.at[idx, 'Welding Check Points'] = missing_values[i]
                    else:
                        break
                return group

            self.df_combined = self.df_combined.groupby('Part Name', group_keys=False).apply(fill_missing_wcp)
            self.df_combined['Welding Check Points'] = self.df_combined['Welding Check Points'].astype(int)

    def fill_unknowns(self):
        columns_to_fill = ['NIK Operator', 'TTD Kasie', 'TTD Operator', 'Create Datetime']
        for col in columns_to_fill:
            if col in self.df_combined.columns:
                if pd.api.types.is_datetime64_any_dtype(self.df_combined[col]):
                    self.df_combined[col] = self.df_combined[col].astype(str)
                    self.df_combined['Create Datetime'] = self.df_combined['Create Datetime'].astype(str).replace("NaT", "UNKNOWN")
                else:
                    self.df_combined[col] = self.df_combined[col].fillna('UNKNOWN')

    def save_to_excel(self):
        self.df_combined.to_excel(self.output_path, index=False)

    def calculate_sum(self, df):
        numeric_values = df.apply(pd.to_numeric, errors='coerce')
        if numeric_values.isnull().values.any():
            return "Invalid Converted"
        return numeric_values.sum()

    def aggregate_defect_data(self, format1_path, target_columns, insert_after='Shift'):
        print(f"\n[INFO] Loading Format 1 Silver data from: {format1_path}")
        df_format1 = pd.read_excel(format1_path)

        if 'Halaman' in df_format1.columns:
            df_format1.drop(columns=['Halaman'], inplace=True)

        for col in ['Model', 'Part No', 'Part Name', 'Shift']:
            if col in df_format1.columns:
                df_format1[col] = df_format1[col].astype(str).str.strip().str.upper()

        df_format1['Production Date'] = pd.to_datetime(df_format1['Production Date'], errors='coerce')

        df_filtered = df_format1[['Model', 'Part No', 'Part Name', 'Shift', 'Production Date'] + target_columns].copy()
        df_grouped = df_filtered.groupby(
            ['Model', 'Part No', 'Part Name', 'Shift', 'Production Date'],
            dropna=False
        ).sum(numeric_only=True).reset_index()

        for col in ['Model', 'Part No', 'Part Name', 'Shift']:
            if col in self.df_combined.columns:
                self.df_combined[col] = self.df_combined[col].astype(str).str.strip().str.upper()

        self.df_combined['Production Date'] = pd.to_datetime(self.df_combined['Production Date'], errors='coerce')
        self.df_combined = self.df_combined.drop(columns=target_columns, errors='ignore')

        merge_keys = ['Model', 'Part No', 'Part Name', 'Shift', 'Production Date']
        self.df_combined = self.df_combined.merge(
            df_grouped[merge_keys + target_columns],
            on=merge_keys,
            how='left'
        )

        self.df_combined[target_columns] = self.df_combined[target_columns].fillna(0)

        insert_index = self.df_combined.columns.get_loc(insert_after) + 1
        for col in reversed(target_columns):
            col_data = self.df_combined.pop(col)
            self.df_combined.insert(insert_index, col, col_data)

        self.df_combined.to_excel(self.output_path, index=False)
        print(f"✅ Silver data tersimpan: {self.output_path}")

    def remove_all_zero_defect_rows(self):
        defect_columns = [
            'KEROPOS', 'KURANG', 'BOLONG', 'UNDERCUT', 'SPATTER', 'TIDAK TEPAT',
            'KASAR (BURRY)', 'RETAK (CRACK)', 'DEFORMASI (DEFORMATION)', 'PENYOK (DENTED)',
            'PAINT/COAT', 'TIDAK MASUK GONOGO', 'STEP PROBLEM', 'STEP LONCAT',
            'FOLDING HARD', 'RECLINING LOSS', 'KARAT (RUSTY)', 'SALAH PASANG',
            'GORESAN (SCRATCH)', 'NOISY (BISING)', 'TIDAK LENGKAP (UNCOMPLETE)'
        ]
        quantity_columns = [
            'QUANTITY CHECKED', 'QUANTITY OK', 'QUANTITY REPAIR', 'QUANTITY SCRAP'
        ]

        existing_defects = [col for col in defect_columns if col in self.df_combined.columns]
        existing_quantities = [col for col in quantity_columns if col in self.df_combined.columns]

        mask_defect_zero = (self.df_combined[existing_defects] == 0).all(axis=1) if existing_defects else False
        mask_quantity_zero = (self.df_combined[existing_quantities] == 0).all(axis=1) if existing_quantities else False
        mask_both_zero = mask_defect_zero & mask_quantity_zero
        combined_mask = mask_defect_zero | mask_quantity_zero

        before = len(self.df_combined)
        self.df_combined = self.df_combined[~combined_mask]
        after = len(self.df_combined)

        print(f"🧹 Total baris dihapus: {before - after}")
        print(f"   ↳ Termasuk yang defect=0 & quantity=0: {mask_both_zero.sum()}")
        print(f"   ↳ Hanya defect=0: {(mask_defect_zero & ~mask_quantity_zero).sum()}")
        print(f"   ↳ Hanya quantity=0: {(mask_quantity_zero & ~mask_defect_zero).sum()}")

    def run(self, defect_columns=None, format1_path=None):
        self.load_excel_file()
        self.clean_unclear_characters()
        self.drop_yield_production_column()
        self.clean_date_columns()
        self.clean_columns()
        self.drop_duplicates()
        self.merge_with_reference()
        self.clean_welding_check_points()
        self.normalize_model_values()
        self.fill_unknowns()
        if defect_columns and format1_path:
            self.aggregate_defect_data(format1_path, defect_columns)
            self.remove_all_zero_defect_rows()
        self.save_to_excel()
