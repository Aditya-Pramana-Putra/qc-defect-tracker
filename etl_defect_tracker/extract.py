import pandas as pd
import glob
import os
import json
from datetime import datetime
import pytz

class ExcelTableExtractor:
    def __init__(self, folder_path, metadata_path):
        self.folder_path = folder_path
        self.metadata_path = metadata_path
        self.df = pd.DataFrame()
        self.metadata = self._load_metadata()
        self.jakarta_tz = pytz.timezone('Asia/Jakarta')

    def _load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r') as f:
                raw_metadata = json.load(f)

                # Fallback untuk metadata format lama (nama file sebagai key)
                if "files" not in raw_metadata:
                    converted = {
                        "files": list(raw_metadata.keys()),
                        "last_created_datetime": max(raw_metadata.values()) if raw_metadata else None
                    }
                    return converted

                # Fallback jika tidak ada last_created_datetime
                if "last_created_datetime" not in raw_metadata:
                    raw_metadata["last_created_datetime"] = None

                return raw_metadata

        # Default metadata jika file tidak ditemukan
        return {
            "files": [],
            "last_created_datetime": None
        }

    def _save_metadata(self):
        with open(self.metadata_path, 'w') as f:
            json.dump(self.metadata, f, indent=2)

    def load_and_combine(self):
        files = glob.glob(self.folder_path)
        df_list = []

        for file in files:
            file_name = os.path.basename(file)

            if file_name in self.metadata["files"]:
                continue

            try:
                if file.endswith(".csv"):
                    try:
                        df = pd.read_csv(file, encoding='utf-8', sep=',')
                    except UnicodeDecodeError:
                        print(f"Encoding error on: {file}, trying latin1 instead.")
                        df = pd.read_csv(file, encoding='latin1', sep=',')
                elif file.endswith((".xlsx", ".xls")):
                    df = pd.read_excel(file, engine='openpyxl')  # use openpyxl for .xlsx
                else:
                    print(f"❌ Unsupported file format skipped: {file}")
                    continue
            except Exception as e:
                print(f"❌ Failed to read {file}: {e}")
                continue

            modified_timestamp = os.path.getmtime(file)
            modified_dt = datetime.fromtimestamp(modified_timestamp, tz=self.jakarta_tz)
            created_time = modified_dt.isoformat()

            df['file_name'] = file_name
            df['created_datetime'] = created_time

            df_list.append(df)
            self.metadata["files"].append(file_name)

            if (
                not self.metadata["last_created_datetime"]
                or created_time > self.metadata["last_created_datetime"]
            ):
                self.metadata["last_created_datetime"] = created_time

        if df_list:
            self.df = pd.concat(df_list, ignore_index=True)

    def transform_defect_data(self):
        if self.df.empty:
            print("DataFrame masih kosong.")
            return

        try:
            # Tentukan kolom-kolom id_vars dan value_vars
            id_vars = self.df.columns[:self.df.columns.get_loc("Welding Check Points") + 1].tolist()
            defect_start_idx = self.df.columns.get_loc("Welding Check Points") + 1
            defect_end_idx = self.df.columns.get_loc("file_name")
            value_vars = self.df.columns[defect_start_idx:defect_end_idx].tolist()

            # Melt (unpivot)
            df_melted = self.df.melt(
                id_vars=id_vars + ['file_name', 'created_datetime'],
                value_vars=value_vars,
                var_name='Defect Type',
                value_name='Amount'
            )

            self.df = df_melted

        except Exception as e:
            print(f"Gagal transformasi data defect: {e}")

    def save_to_excel(self, output_path):
        if not self.df.empty:
            if os.path.exists(output_path):
                try:
                    if output_path.endswith('.xlsx'):
                        existing_df = pd.read_excel(output_path, engine='openpyxl')
                    else:
                        existing_df = pd.read_csv(output_path, encoding='utf-8')
                except UnicodeDecodeError:
                    print(f"Encoding error on existing output file: {output_path}, trying latin1 instead.")
                    existing_df = pd.read_csv(output_path, encoding='latin1')
                except Exception as e:
                    print(f"Error reading existing output file: {e}")
                    existing_df = pd.DataFrame()

                self.df = pd.concat([existing_df, self.df], ignore_index=True)

            self.df.to_excel(output_path, index=False, engine='openpyxl')
            self._save_metadata()
            print(f"Data berhasil disimpan ke {output_path}")
        else:
            print("Tidak ada file baru untuk diproses.")
