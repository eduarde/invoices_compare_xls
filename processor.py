import pandas as pd
from abc import ABC, abstractmethod
from decimal import Decimal, ROUND_HALF_UP


def make_diff_dataframes(
    df_external: pd.DataFrame, df_internal: pd.DataFrame
) -> pd.DataFrame:
    """
    Compares two DataFrames containing invoice data and returns a DataFrame
    """
    return df_external[~df_external["id"].isin(df_internal["id"])]


def process_mismatches(df_external: pd.DataFrame, df_internal: pd.DataFrame) -> list:
    """
    Compares two DataFrames containing invoice data and identifies mismatches
    based on the 'id' and 'value' columns.
    It returns a list of dictionaries with the 'id', 'theirs' (external value),
    and 'ours' (internal value) for mismatched entries where the absolute difference in 'value'."""
    merged_df = pd.merge(
        df_external, df_internal, on="id", suffixes=("_theirs", "_ours")
    )

    mismatched_values = merged_df[
        (merged_df["value_theirs"] - merged_df["value_ours"]).abs() >= 0.1
    ]

    return mismatched_values.apply(
        lambda row: {
            "id": row["id"],
            "theirs": row["value_theirs"],
            "ours": row["value_ours"],
        },
        axis=1,
    ).tolist()


class ETL(ABC):
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a raw input file.
        """
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the raw DataFrame into a cleaned format.
        """
        pass

    @abstractmethod
    def load(self) -> pd.DataFrame:
        """
        Extracts and transforms the data into final format.
        """
        pass


class ExcelInvoiceLoader(ETL):
    def __init__(
        self,
        file_path: str,
        columns: list,
        header_row: int | list = 0,
        replace_z: bool = False,
        filters: dict = None,
    ):
        self.file_path = file_path
        self.columns = columns
        self.header_row = header_row
        self.replace_z = replace_z
        self.filters = filters

    @staticmethod
    def _excel_round(value):
        return float(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def _flatten_columns(self, cols):
        return [
            " ".join(
                level.strip()
                for level in col
                if isinstance(level, str) and not level.strip().startswith("Unnamed")
            ).strip()
            for col in cols
        ]

    def _read_excel_usecols(self):
        """
        Reads an Excel file using specified columns when header is a single row.
        """
        return pd.read_excel(
            self.file_path, usecols=self.columns, header=self.header_row
        )

    def _read_excel_full(self):
        """
        Reads an Excel file without using specified columns when header is a multi row.
        """

        df = pd.read_excel(self.file_path, header=self.header_row)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = self._flatten_columns(df.columns)
        else:
            df.columns = df.columns.str.strip()

        missing = [col for col in self.columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")

        return df[self.columns]

    def extract(self) -> pd.DataFrame:
        """
        Extracts specified columns from an Excel file and applies optional filters.
        """
        if isinstance(self.header_row, int):
            df = self._read_excel_usecols()
        else:
            df = self._read_excel_full()

        if self.filters:
            for col, val in self.filters.items():
                if col in df.columns:
                    if isinstance(val, (list, tuple, set)):
                        df = df[df[col].isin(val)]
                    else:
                        df = df[df[col] == val]
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the DataFrame by renaming columns id and value
        and grouping by the id column to sum the values.
        If replace_z is True, it replaces 'Z 0x' with "BONF-000000x" in the id column.
        """
        df = df.rename(columns={self.columns[0]: "id", self.columns[1]: "value"})
        df = df.dropna(subset=["id", "value"])

        if self.replace_z:
            df["id"] = df["id"].astype(str)
            df["id"] = df["id"].str.replace(r"^z(?=\d)", "Z ", regex=True)
            df["id"] = df["id"].str.replace(
                r"^Z (\d+)$", lambda m: f"BONF-{int(m.group(1)):07d}", regex=True
            )

        df = df.groupby("id", as_index=False)["value"].sum()
        df["value"] = df["value"].apply(self._excel_round)
        return df

    def load(self) -> pd.DataFrame:
        """
        1. Extract data from an Excel file with specified columns
        2. Transforms the data by renaming and grouping
        """
        df = self.extract()
        return self.transform(df)
