import os
import uuid
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, Request, Query
from fastapi.responses import FileResponse
from typing import Literal, Optional, IO, List
from filters import FILTER_MAP, EXCLUDE_FILTER_MAP
from processor import (
    ExcelInvoiceLoader,
    DataInvoiceLoader,
    process_mismatches,
    make_diff_dataframes,
)

app = FastAPI()


def load_files(
    external_invoice: UploadFile, filter: str | None, exclude: str | None
) -> tuple:
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
        filters=FILTER_MAP.get(filter, {}),
        exclude=EXCLUDE_FILTER_MAP.get(exclude, {}),
    )
    df_internal = internal_file_loader.load()

    filename = external_invoice.filename.lower()
    columns_map = {
        "fisa 709": ["Nr. doc.", "Cont  corespondent"],
        "default": ["Nr. doc.", "Sume debitoare"],
    }
    columns = (
        columns_map["fisa 709"]
        if filename.startswith("fisa 709")
        else columns_map["default"]
    )
    replace_z = filename.startswith("fisa 461")
    invert_sign = filename.startswith("fisa 709")

    external_file_loader = ExcelInvoiceLoader(
        file_path=external_invoice.file,
        columns=columns,
        header_row=[7, 8],
        replace_z=replace_z,
        invert_sign=invert_sign,
        filters={},
    )
    df_external = external_file_loader.load()

    return df_internal, df_external


async def _process_external_invoices(external_invoices: List[UploadFile]) -> tuple:
    """
    Process multiple external invoice files and return their loaded data.
    """
    results = []
    file_names = []
    for file in external_invoices:
        try:
            filename = file.filename.lower()
            columns_map = {
                "fisa 709": ["Nr. doc.", "Cont  corespondent"],
                "default": ["Nr. doc.", "Sume debitoare"],
            }
            columns = (
                columns_map["fisa 709"]
                if filename.startswith("fisa 709")
                else columns_map["default"]
            )
            replace_z = filename.startswith("fisa 461")
            invert_sign = filename.startswith("fisa 709")

            external_file_loader = ExcelInvoiceLoader(
                file_path=file.file,
                columns=columns,
                header_row=[7, 8],
                replace_z=replace_z,
                invert_sign=invert_sign,
            )
            file_names.append(file.filename)
            results.append(external_file_loader.load())

        except Exception as e:
            print(f"Error processing the invoice files {e}")
        finally:
            await file.close()
    return results, file_names


def write_output_to_excel(mismatches: list) -> str:
    """
    Write mismatches to an Excel file and return the filename.
    """
    unique_suffix = (
        datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]
    )
    df_excel = pd.json_normalize(mismatches)
    df_excel["observatii"] = ""
    output_xlsx = f"output_{unique_suffix}.xlsx"
    df_excel.to_excel(f"docs/output/{output_xlsx}", index=False)
    return output_xlsx


@app.get("/download-results")
def download_results(filename: str = Query(...)):
    file_path = f"docs/output/{filename}"
    if os.path.exists(file_path):
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    return {"error": "File not found"}


@app.post("/read_data/", response_model=None)
async def read_data(
    external_invoice: UploadFile = File(...),
    filter: Literal[*FILTER_MAP.keys()] = Form(None),
    exclude: Literal[*EXCLUDE_FILTER_MAP.keys()] = Form(None),
):
    try:
        data_ours, data_theirs = load_files(external_invoice, filter, exclude)
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
    request: Request,
    external_invoice: UploadFile = File(...),
    filter: Literal[*FILTER_MAP.keys()] = Form(None),
    exclude: Literal[*EXCLUDE_FILTER_MAP.keys()] = Form(None),
):
    try:
        df_internal, df_external = load_files(external_invoice, filter, exclude)

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

        output_xlsx = write_output_to_excel(mismatches)
        download_url = f"{request.base_url}download-results?filename={output_xlsx}"

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
        "DOWNLOAD_URL": download_url,
    }


@app.post("/compare/multi")
async def compare_multi_data(
    request: Request,
    external_invoices: List[UploadFile] = File(...),
    filter: Literal[*FILTER_MAP.keys()] = Form(None),
    exclude: Literal[*EXCLUDE_FILTER_MAP.keys()] = Form(None),
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
        filters=FILTER_MAP.get(filter, {}) if filter else None,
        exclude=EXCLUDE_FILTER_MAP.get(exclude, {}) if exclude else None,
    )
    df_internal = internal_file_loader.load()

    results, file_names = await _process_external_invoices(external_invoices)
    external_loader = DataInvoiceLoader(data=results)
    df_external = external_loader.load()

    missing_in_ours_df = make_diff_dataframes(df_external, df_internal)
    mismatches = process_mismatches(df_external, df_internal)

    output_xlsx = write_output_to_excel(mismatches)
    download_url = f"{request.base_url}download-results?filename={output_xlsx}"

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
        "DOWNLOAD_URL": download_url,
    }
