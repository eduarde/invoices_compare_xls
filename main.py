import pandas as pd
from fastapi import FastAPI, UploadFile, File, Form
from typing import Literal, Optional, IO
from filters import FILTER_MAP
from processor import ExcelInvoiceLoader

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
    )
    df_internal = internal_file_loader.load()

    external_file_loader = ExcelInvoiceLoader(
        file_path=external_invoice_file,
        columns=["Nr. doc.", "Sume debitoare"],
        header_row=[7,8],
        replace_z=invoice_type == "FB",
        filters={},
    )
    df_external = external_file_loader.load()

    return df_internal, df_external


@app.post("/read_data/", response_model=None)
async def read_data(
    external_invoice: UploadFile = File(...),
    invoice_type: Literal["FB", "SPA"] = Form(...),
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


@app.post("/compare_data/")
async def compare_data(
    external_invoice: UploadFile = File(...),
    invoice_type: Literal["FB", "SPA"] = Form(...),
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

        missing_in_ours_df = df_external[~df_external["id"].isin(data_ours_bulk["id"])]

        # Compare values with matching IDs and ignore small decimal differences
        merged_df = pd.merge(
            df_external, df_internal, on="id", suffixes=("_theirs", "_ours")
        )
        mismatched_values = merged_df[
            (merged_df["value_theirs"] - merged_df["value_ours"]).abs() >= 0.06
        ]
        mismatches = mismatched_values.apply(
            lambda row: {
                "id": row["id"],
                "theirs": row["value_theirs"],
                "ours": row["value_ours"],
            },
            axis=1,
        ).tolist()
    # except Exception as e:
    #     return {"Error processing the invoice files": str(e)}
    finally:
        if external_invoice:
            await external_invoice.close()

    return {
        "INVOICES_OURS": {
            "missing": missing_in_ours_df,
            "total_missing": len(missing_in_ours_df),
        },
        "MISMATCH_ANALYSIS": {
            "invoices": mismatches,
            "total_mismatches": len(mismatches),
        },
    }
