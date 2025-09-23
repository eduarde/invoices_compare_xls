import os
import re
import uuid
import pandas as pd
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, Request, Query
from fastapi.responses import FileResponse
from typing import Literal, List
from filters import FILTER_MAP, EXCLUDE_FILTER_MAP, FILES_FILTER_MAP
from processor import (
    ExcelInvoiceLoader,
    DataInvoiceLoader,
    process_mismatches,
    make_diff_dataframes,
)
from settings import (
    COLUMNS_INTERNAL_INVOICES,
    COLUMNS_INTERNAL_INVOICES_ALL,
    COLUMNS_INTERNAL_INVOICES_MULTI,
    HEADER_ROW_INTERNAL_INVOICES,
    COLUMNS_EXTERNAL_INVOICES,
    HEADER_ROW_EXTERNAL_INVOICES,
    COLUMNS_EXTERNAL_INVOICES_709,
)

app = FastAPI()


def _extract_invoice_number_file(text: str) -> str | None:
    """
    Extracts the invoice number from a given text string.
    Example: "saga 704.01 cazare iulie.xls 03.09.xls" -> "704.01"
            "fisa 462.01.01 something.xls" -> "462.01.01"
            "fisa 419 ceva.xls" -> "419"
    """
    match = re.search(r"\d+(?:\.\d+)+|\d+", text)
    if match:
        return match.group(0)
    return None


def _external_columns(filename: str) -> list:
    """
    Determine the correct columns for external invoices based on the filename.
    """
    external_columns = COLUMNS_EXTERNAL_INVOICES
    if filename.filename.lower().startswith("709"):
        external_columns = COLUMNS_EXTERNAL_INVOICES_709
    return list(external_columns)


async def close_resource(file: UploadFile):
    try:
        if file:
            await file.file.close()
    except Exception as e:
        print(f"Error closing file: {e}")


def load_dataframe(
    file: UploadFile,
    columns: tuple | list,
    headrow: int,
    filter: dict | None = None,
    exclude: dict | None = None,
) -> pd.DataFrame | None:
    """
    Load a DataFrame from an uploaded Excel file with specified columns and filters."""
    try:
        if file:
            replace_z = file.filename.lower().startswith("461")
            invert_sign = file.filename.lower().startswith("709")

            file_loader = ExcelInvoiceLoader(
                file_path=file.file,
                columns=columns,
                header_row=headrow,
                replace_z=replace_z,
                remove_serie=True,
                invert_sign=invert_sign,
                filters=filter,
                exclude=exclude,
            )
        return file_loader.load()
    except Exception as e:
        print(f"Error loading file: {e}")


async def _process_external_invoices(external_invoices: List[UploadFile]) -> tuple:
    """
    Process multiple external invoice files and return their loaded data.
    """
    results = []
    file_names = []

    for file in external_invoices:
        try:
            data_external = load_dataframe(
                file,
                _external_columns(file),
                HEADER_ROW_EXTERNAL_INVOICES,
            )
            file_names.append(file.filename)
            results.append(data_external)

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


def __debug_df(df: pd.DataFrame, label: str, id: str):
    print(f"{label} ALL")
    print(df)
    row = df[df["_id"] == id]
    print(f"{label}")
    print(row)


def _compare_generic(
    request: Request,
    internal_invoice: UploadFile,
    external_invoice: UploadFile,
    filter_key,
    filter_map,
    use_exclude=True,
    exclude_key=None,
):
    # Prepare filter and exclude
    filter_dict = filter_map.get(filter_key, {}) if filter_key else None
    print("Applying filter for file:", filter_dict)
    exclude_dict = (
        EXCLUDE_FILTER_MAP.get(exclude_key, {})
        if (use_exclude and exclude_key)
        else None
    )

    data_internal = load_dataframe(
        internal_invoice,
        COLUMNS_INTERNAL_INVOICES_MULTI,
        HEADER_ROW_INTERNAL_INVOICES,
        filter=filter_dict,
        exclude=exclude_dict,
    )

    # __debug_df(data_internal, "INTERNAL", "7980")

    data_external = load_dataframe(
        external_invoice,
        _external_columns(external_invoice),
        HEADER_ROW_EXTERNAL_INVOICES,
    )

    data_internal_all = load_dataframe(
        internal_invoice,
        COLUMNS_INTERNAL_INVOICES_ALL,
        HEADER_ROW_INTERNAL_INVOICES,
    )

    missing_in_ours_df = make_diff_dataframes(data_external, data_internal_all)
    mismatches = process_mismatches(data_external, data_internal)

    output_xlsx = write_output_to_excel(mismatches)
    download_url = f"{request.base_url}download-results?filename={output_xlsx}"

    return {
        "INFO": {
            "INTERNAL_INVOICE_FILE": internal_invoice.filename,
            "FILTER_APPLIED": filter_dict,
            "EXCLUDE_APPLIED": exclude_dict,
            "EXTERNAL_INVOICE_FILE": external_invoice.filename,
        },
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
            "invoices": {
                "id_view": ", ".join(item["id"] for item in mismatches),
                "detail_view": mismatches,
            },
            "total": len(mismatches),
        },
        "DOWNLOAD_URL": download_url,
    }


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
    internal_invoice: UploadFile = File(...),
    external_invoice: UploadFile = File(...),
    filter: Literal[*FILTER_MAP.keys()] = Form(None),
    exclude: Literal[*EXCLUDE_FILTER_MAP.keys()] = Form(None),
):
    try:
        filter_dict = FILTER_MAP.get(filter, {}) if filter else None
        exclude_dict = EXCLUDE_FILTER_MAP.get(exclude, {}) if exclude else None

        data_internal = load_dataframe(
            internal_invoice,
            COLUMNS_INTERNAL_INVOICES,
            HEADER_ROW_INTERNAL_INVOICES,
            filter=filter_dict,
            exclude=exclude_dict,
        )

        data_external = load_dataframe(
            external_invoice,
            _external_columns(external_invoice),
            HEADER_ROW_EXTERNAL_INVOICES,
        )

    except Exception as e:
        return {"Error processing the invoice files": str(e)}
    finally:
        close_resource(internal_invoice)
        close_resource(external_invoice)

    return {
        "INFO": {
            "INTERNAL_INVOICE_FILE": internal_invoice.filename,
            "FILTER_APPLIED": filter_dict,
            "EXCLUDE_APPLIED": exclude_dict,
            "EXTERNAL_INVOICE_FILE": external_invoice.filename,
        },
        "INVOICES_OURS": data_internal.to_dict(orient="records"),
        "INVOICES_THEIRS": data_external.to_dict(orient="records"),
    }


@app.post("/compare/file")
async def compare_data(
    request: Request,
    internal_invoice: UploadFile = File(...),
    external_invoice: UploadFile = File(...),
    filter: Literal[*FILTER_MAP.keys()] = Form(None),
    exclude: Literal[*EXCLUDE_FILTER_MAP.keys()] = Form(None),
):
    try:
        return _compare_generic(
            request,
            internal_invoice,
            external_invoice,
            filter,
            FILTER_MAP,
            use_exclude=True,
            exclude_key=exclude,
        )

    except Exception as e:
        print(f"Error processing the invoice files {e}")
    finally:
        close_resource(internal_invoice)
        close_resource(external_invoice)


@app.post("/compare/saga/softone/file")
async def compare_saga_file(
    request: Request,
    internal_invoice: UploadFile = File(...),
    external_invoice: UploadFile = File(...),
):
    try:
        filter = _extract_invoice_number_file(external_invoice.filename.lower())
        print(filter)

        return _compare_generic(
            request,
            internal_invoice,
            external_invoice,
            filter,
            FILES_FILTER_MAP,
            use_exclude=False,
        )

    except Exception as e:
        print(f"Error processing the invoice files {e}")
    finally:
        close_resource(internal_invoice)
        close_resource(external_invoice)


@app.post("/compare/multi")
async def compare_multi_data(
    request: Request,
    internal_invoice: UploadFile = File(...),
    external_invoices: List[UploadFile] = File(...),
    filter: Literal[*FILTER_MAP.keys()] = Form(None),
    exclude: Literal[*EXCLUDE_FILTER_MAP.keys()] = Form(None),
):
    try:
        filter_dict = FILTER_MAP.get(filter, {}) if filter else None
        exclude_dict = EXCLUDE_FILTER_MAP.get(exclude, {}) if exclude else None

        data_internal = load_dataframe(
            internal_invoice,
            COLUMNS_INTERNAL_INVOICES_MULTI,
            HEADER_ROW_INTERNAL_INVOICES,
            filter=filter_dict,
            exclude=exclude_dict,
        )

        results, file_names = await _process_external_invoices(external_invoices)
        external_loader = DataInvoiceLoader(data=results)
        df_external = external_loader.load()

        missing_in_ours_df = make_diff_dataframes(df_external, data_internal)
        mismatches = process_mismatches(df_external, data_internal)

        output_xlsx = write_output_to_excel(mismatches)
        download_url = f"{request.base_url}download-results?filename={output_xlsx}"

        return {
            "INFO": {
                "INTERNAL_INVOICE_FILE": internal_invoice.filename,
                "FILTER_APPLIED": filter_dict,
                "EXCLUDE_APPLIED": exclude_dict,
                "EXTERNAL_INVOICE_FILE": ", ".join(file for file in file_names),
            },
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

    except Exception as e:
        print(f"Error processing the invoice files {e}")
    finally:
        close_resource(internal_invoice)
        for file in external_invoices:
            close_resource(file)
