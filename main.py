import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
from typing import Literal, Optional, IO, List
from filters import FILTER_MAP, EXCLUDE_FILTER_MAP
from processor import ExcelInvoiceLoader, process_mismatches, make_diff_dataframes

app = FastAPI()


def load_files(invoice_type: str, external_invoice_file: Optional[IO] = None) -> tuple:
    """
    Load files and return the transformed data as dataframes.
    Our file: a local file with all the invoices in the hotel.
    External file: a file with the invoices processed by 3rd party.
    """
    internal_file_loader = ExcelInvoiceLoader(
        file_path="docs/invoices_ours.xlsx",
        columns=[
            "Document",
            "Val. neta RON",
            "Perioada",
            "Nume",
        ],
        header_row=5,
        replace_z=False,
        filters=FILTER_MAP.get(invoice_type, {}),
        exclude=EXCLUDE_FILTER_MAP.get("RECONCILIERI", {}),
    )
    df_internal = internal_file_loader.load()

    external_file_loader = ExcelInvoiceLoader(
        file_path=external_invoice_file,
        columns=["Nr. doc.", "Sume debitoare"],
        header_row=[7, 8],
        replace_z=invoice_type == "FB",
        filters={},
    )
    df_external = external_file_loader.load()

    return df_internal, df_external


@app.post("/read_data/", response_model=None)
async def read_data(
    external_invoice: UploadFile = File(...),
    invoice_type: Literal[*FILTER_MAP.keys()] = Form(...),
):
    try:
        data_ours, data_theirs = load_files(
            invoice_type,
            external_invoice.file if external_invoice else None,
        )
        if external_invoice:
            await external_invoice.close()
    except Exception as e:
        return {"Error processing the invoice files": str(e)}
    finally:
        if external_invoice:
            await external_invoice.close()

    return {
        "INVOICES_OURS": data_ours.to_dict(orient="records"),
        "INVOICES_THEIRS": data_theirs.to_dict(orient="records"),
    }


@app.post("/compare/file")
async def compare_data(
    external_invoice: UploadFile = File(...),
    invoice_type: Literal[*FILTER_MAP.keys()] = Form(...),
):
    try:
        df_internal, df_external = load_files(
            invoice_type,
            external_invoice.file if external_invoice else None,
        )

        internal_file_loader_bulk = ExcelInvoiceLoader(
            file_path="docs/invoices_ours.xlsx",
            columns=[
                "Document",
                "Val. neta RON",
            ],
            header_row=5,
            replace_z=False,
        )
        data_ours_bulk = internal_file_loader_bulk.load()

        missing_in_ours_df = make_diff_dataframes(df_external, data_ours_bulk)
        mismatches = process_mismatches(df_external, df_internal)

    except Exception as e:
        print(f"Error processing the invoice files {e}")
    finally:
        if external_invoice:
            await external_invoice.close()

    return {
        "EXTERNAL_INVOICE_FILE": external_invoice.filename,
        "MISSING": {
            "description": "Invoices that are present in the external file but missing in our records.",
            "invoices": {
                "id_view": ", ".join(item["id"] for item in missing_in_ours_df),
                "detail_view": missing_in_ours_df,
            },
            "total": len(missing_in_ours_df),
        },
        "MISMATCH": {
            "description": "Invoices that have mismatched values between the external file and our records.",
            "invoices": mismatches,
            "total": len(mismatches),
        },
    }


@app.post("/compare/multi")
async def compare_multi_data(
    external_invoices: List[UploadFile] = File(...),
):
    internal_file_loader = ExcelInvoiceLoader(
        file_path="docs/invoices_ours.xlsx",
        columns=[
            "Document",
            "Val. neta RON",
            "Nume",
        ],
        header_row=5,
        replace_z=False,
        exclude=EXCLUDE_FILTER_MAP.get("RECONCILIERI", {}),
    )
    df_internal = internal_file_loader.load()

    results = []
    file_names = []
    for file in external_invoices:
        try:
            external_file_loader = ExcelInvoiceLoader(
                file_path=file.file,
                columns=["Nr. doc.", "Sume debitoare"],
                header_row=[7, 8],
                replace_z=file.filename.startswith("fisa 461"),
            )
            file_names.append(file.filename)
            results.append(external_file_loader.load())

        except Exception as e:
            print(f"Error processing the invoice files {e}")
        finally:
            await file.close()

        if not results:
            return {"error": "No valid data files provided."}

        df_external = pd.concat(results, ignore_index=True)
        df_external["_id"] = df_external["id"].astype(str)
        df_external["id"] = df_external["id"].apply(ExcelInvoiceLoader.normalize_id)
        df_external = df_external.groupby("id", as_index=False).agg(
            {"value": "sum", "_id": "first"}
        )

        df_external["value"] = df_external["value"].apply(
            ExcelInvoiceLoader._excel_round
        )

        missing_in_ours_df = make_diff_dataframes(df_external, df_internal)
        mismatches = process_mismatches(df_external, df_internal)

    return {
        "EXTERNAL_INVOICE_FILE": ", ".join(file for file in file_names),
        "MISSING": {
            "description": "Invoices that are present in the external file but missing in our records.",
            "invoices": {
                "id_view": ", ".join(item["id"] for item in missing_in_ours_df),
                "detail_view": missing_in_ours_df,
            },
            "total": len(missing_in_ours_df),
        },
        "MISMATCH": {
            "description": "Invoices that have mismatched values between the external file and our records.",
            "invoices": mismatches,
            "total": len(mismatches),
        },
    }
