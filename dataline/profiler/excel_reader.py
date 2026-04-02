"""Excel file profiling with enriched column statistics."""

from __future__ import annotations

import logging
import os

import pandas as pd

from ..core.types import ManifestEntry
from .column_stats import compute_column_stats, compressed_value_repr, safe_scalar

logger = logging.getLogger(__name__)


def read_excel(file_path: str) -> ManifestEntry:
    """Profile an Excel file into a ManifestEntry."""
    size = os.path.getsize(file_path)

    try:
        xls = pd.ExcelFile(file_path)
        sheets = []
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, nrows=100)
            columns = []
            for col in df.columns:
                col_info: dict = {
                    "name": str(col),
                    "dtype": str(df[col].dtype),
                    "null_pct": round(float(df[col].isna().mean()), 3),
                    "sample": [safe_scalar(v) for v in df[col].dropna().head(3).tolist()],
                }
                # Enriched stats
                col_info.update(compute_column_stats(df[col], col_name=str(col)))
                col_info["value_repr"] = compressed_value_repr(df[col])
                columns.append(col_info)

            # Get row count without re-reading entire sheet: read header only
            try:
                full_df = pd.read_excel(xls, sheet_name=sheet_name, header=0)
                sheet_row_count = len(full_df)
            except Exception:
                sheet_row_count = len(df)

            sheets.append({
                "name": sheet_name,
                "row_count": sheet_row_count,
                "columns": columns,
                "sample_rows": df.head(3).to_dict(orient="records"),
            })

        return ManifestEntry(
            file_path=file_path, file_type="excel", size_bytes=size,
            summary={"sheets": sheets},
        )
    except Exception as e:
        return ManifestEntry(
            file_path=file_path, file_type="excel", size_bytes=size,
            summary={"error": str(e)},
        )


