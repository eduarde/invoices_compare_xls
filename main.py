import pandas as pd
from fastapi import FastAPI
from decimal import Decimal, ROUND_HALF_UP


app = FastAPI()


def excel_round(value):
    return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def extract(
    file_path: str, columns: list, header_row: int = 0, filters: dict = None
) -> pd.DataFrame:
    """
    Extracts specified columns from an Excel file and applies optional filters.
    """
    df = pd.read_excel(
        file_path, usecols=columns, header=header_row
    )  # load only needed columns
    if filters:
        for col, val in filters.items():
            if col in df.columns:
                if isinstance(val, (list, tuple, set)):
                    df = df[df[col].isin(val)]
                else:
                    df = df[df[col] == val]
    return df


def transform(
    df: pd.DataFrame, id_column: str, value_column: str, replace_z: bool = False
) -> pd.DataFrame:
    """
    Transforms the DataFrame by renaming columns id and value
    and grouping by the id column to sum the values.
    If replace_z is True, it replaces 'Z 0x' with "BONF-000000x" in the id column.
    """
    df_transformed = df.rename(columns={id_column: "id", value_column: "value"})
    df_transformed = df_transformed.dropna(subset=["id", "value"])

    if replace_z:
        # Replace "Z 60" format first, then handle zero-padding conversion
        df_transformed["id"] = df_transformed["id"].astype(str)
        df_transformed["id"] = df_transformed["id"].str.replace(
            r"^z(?=\d)", "Z ", regex=True
        )
        df_transformed["id"] = df_transformed["id"].str.replace(
            r"^Z (\d+)$", lambda m: f"BONF-{int(m.group(1)):07d}", regex=True
        )

    df_grouped = df_transformed.groupby("id", as_index=False)["value"].sum()
    df_grouped["value"] = df_grouped["value"].apply(excel_round)
    return df_grouped


def load(
    output_path: str,
    columns: list,
    header_row: int = 0,
    replace_z: bool = False,
    filters: dict = None,
) -> list:
    """
    1. Extract data from an Excel file with specified columns
    3. Transforms the data by renaming and grouping
    """
    df_extracted = extract(output_path, columns, header_row, filters)
    df_transformed = transform(df_extracted, columns[0], columns[1], replace_z)
    return df_transformed


def load_files():
    """
    Load local files and return the transformed data as dataframes.
    Our file: a file with all the invoices in the hotel.
    Their file: a file with the invoices processed by 3rd party restaurant.

    """
    output_path_ours = "docs/invoices_ours.xlsx"
    columns_ours = [
        "Document",
        "Val. neta RON",
        "Perioada",
        "Nume",
        # "Cont vanzari",
        # "Categorie Lucrari",
        # "Faza proiect",
    ]
    filters = {
        "Perioada": ("Ianuarie", "Februarie", "Martie", "Aprilie", "Noiembrie"),
        "Nume": (
            "BQT Lunch Alcohol",
            "BQT Lunch Food(C)",
            "BQT Lunch Food (C)",
            "BQT Lunch NonAlcohol (A)",
            "BQT Lunch NonAlcohol(A)",
            "BQT Lunch NonAlcohol (C)",
            "BQT Lunch NonAlcohol(C)",
            "Restaurant 19%",
            "Restaurant 9%",
            "Restaurant Lunch Alcohol",
            "Restaurant Lunch Food (A)",
            "Restaurant Lunch Food(A)",
            "Restaurant Lunch Food (C)",
            "Restaurant Lunch Food(C)",
            "Restaurant Lunch NonAlcohol (A)",
            "Restaurant Lunch NonAlcohol(A)",
            "Restaurant Lunch NonAlcohol (C)",
            "Restaurant Lunch NonAlcohol(C)",
            "Room Service Lunch Food (C)",
            "Room Service Lunch Food(C)",
            "Room Service Lunch NonAlcohol (C)",
            "Room Service Lunch NonAlcohol(C)",
            "Tips Restaurant",
            "Tips"
        ),
        # "Categorie Lucrari": "FOOD & BEVERAGE",
        # "Faza proiect": "Venituri mancare&bautura plata pe camera - third p",
        # "Cont vanzari": 461,
    }
    data_ours = load(output_path_ours, columns_ours, header_row=5, filters=filters)

    output_path_theirs = "docs/invoices_theirs.xls"
    columns_theirs = ["ndp", "suma_c"]
    data_theirs = load(output_path_theirs, columns_theirs, replace_z=True)

    return data_ours, data_theirs


@app.get("/read/", response_model=None)
def read_data():
    data_ours, data_theirs = load_files()
    return {
        "INVOICES_OURS": data_ours.to_dict(orient="records"),
        "INVOICES_THEIRS": data_theirs.to_dict(orient="records"),
    }


@app.get("/compare/")
def compare_data():


    df_ours, df_theirs = load_files()


    output_path_ours = "docs/invoices_ours.xlsx"
    columns_ours = [
        "Document",
        "Val. neta RON",
    ]
    data_ours_bulk = load(output_path_ours, columns_ours, header_row=5)

    missing_in_ours_df = df_theirs[~df_theirs["id"].isin(data_ours_bulk["id"])]
    # missing_in_theirs_df = df_ours[~df_ours["id"].isin(df_theirs["id"])]

    # Compare values with matching IDs and ignore small decimal differences
    merged_df = pd.merge(df_theirs, df_ours, on="id", suffixes=("_theirs", "_ours"))
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

    return {
        "INVOICES_OURS": {
            "missing": missing_in_ours_df,
            "total_missing": len(missing_in_ours_df),
        },
        # "INVOICES_THEIRS": {
        #     "missing": missing_in_theirs_df,
        #     "total_missing": len(missing_in_theirs_df),
        # },
        "MISMATCH_ANALYSIS": {
            "invoices": mismatches,
            "total_mismatches": len(mismatches),
        },
    }
