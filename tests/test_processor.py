import tempfile
import pandas as pd
from processor import ExcelInvoiceLoader, make_diff_dataframes, process_mismatches


def test_excel_invoice_loader_sum():
    # Prepare test data
    df = pd.DataFrame(
        {
            "Invoice ID": ["A001", "A002", "A001", "A003", "A002"],
            "Amount": [100.123, 200.456, 300.789, 400.1, 99.999],
        }
    )

    # Create a temporary Excel file
    with tempfile.NamedTemporaryFile(suffix=".xlsx") as tmp:
        df.to_excel(tmp.name, index=False)

        # Initialize the loader
        loader = ExcelInvoiceLoader(
            file_path=tmp.name,
            columns=["Invoice ID", "Amount"],
            header_row=0,
            replace_z=False,
        )

        # Load and transform
        result_df = loader.load()

        # Define expected results manually
        expected_df = pd.DataFrame(
            {
                "id": ["A001", "A002", "A003"],
                "value": [
                    ExcelInvoiceLoader._excel_round(100.123 + 300.789),
                    ExcelInvoiceLoader._excel_round(200.456 + 99.999),
                    ExcelInvoiceLoader._excel_round(400.1),
                ],
            }
        )

    # Sort and compare
    result_df = result_df.sort_values(by="id").reset_index(drop=True)
    expected_df = expected_df.sort_values(by="id").reset_index(drop=True)

    pd.testing.assert_frame_equal(result_df[["id", "value"]], expected_df)


def test_make_diff_dataframes_returns_missing_rows():
    df_external = pd.DataFrame(
        {"id": ["A001", "A002", "A003"], "value": [100, 200, 300]}
    )
    df_internal = pd.DataFrame({"id": ["A002", "A003"], "value": [200, 300]})

    # Add _id columns to match processor.py expectations
    df_external["_id"] = df_external["id"]
    df_internal["_id"] = df_internal["id"]

    result = make_diff_dataframes(df_external, df_internal)

    expected = pd.DataFrame({"id": ["A001"], "value": [100]})

    pd.testing.assert_frame_equal(result.reset_index(drop=True), expected)


def test_process_mismatches_returns_differences_above_threshold():
    df_external = pd.DataFrame(
        {
            "id": ["A001", "A002", "A003", "A004"],
            "value": [100.00, 200.10, 300.00, 400.01],
        }
    )
    df_internal = pd.DataFrame(
        {
            "id": ["A001", "A002", "A003", "A004"],
            "value": [100.00, 200.00, 299.95, 400.08],
        }
    )

    # Add _id columns to match processor.py expectations
    df_external["_id"] = df_external["id"]
    df_internal["_id"] = df_internal["id"]

    result = process_mismatches(df_external, df_internal)

    expected = [
        {"id": "A002", "theirs": 200.10, "ours": 200.00},
        {"id": "A004", "theirs": 400.01, "ours": 400.08},
    ]

    assert result == expected