import great_expectations as ge
from great_expectations.data_context import get_context
from etl_defect_tracker.extract import ExcelTableExtractor

class ExcelTableExtractorWithValidation(ExcelTableExtractor):
    def validate_with_great_expectations(self, expectation_suite_name="default_expectation_suite"):
        if self.df.empty:
            print("DataFrame kosong, tidak ada yang divalidasi.")
            return

        # Convert pandas DataFrame ke GE DataFrame
        ge_df = ge.from_pandas(self.df)

        # Ambil data context
        context = get_context()

        # Buat suite jika belum ada
        try:
            context.get_expectation_suite(expectation_suite_name)
        except Exception:
            context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

        # Tambahkan expectations
        ge_df.expect_column_to_exist("file_name")
        ge_df.expect_column_to_exist("created_datetime")
        ge_df.expect_column_values_to_not_be_null("file_name")
        ge_df.expect_column_values_to_not_be_null("created_datetime")

        # Validasi semua kolom numerik agar bernilai positif
        for col in self.df.select_dtypes(include='number').columns:
            ge_df.expect_column_values_to_be_between(col, min_value=0)

        # Simpan expectation suite
        suite = ge_df.get_expectation_suite(expectation_suite_name)
        context.save_expectation_suite(expectation_suite=suite)

        # Jalankan validasi menggunakan validation_operator (0.18.x)
        results = context.run_validation_operator(
            "action_list_operator",  # default operator di GE 0.18
            assets_to_validate=[ge_df]
        )

        # Tampilkan hasil validasi
        if results["success"]:
            print("✅ Validasi data berhasil!")
        else:
            print("❌ Validasi data gagal! Lihat detail berikut:")
            for result in results["results"]:
                if not result["success"]:
                    print(result["expectation_config"]["expectation_type"], ":", result["result"])
