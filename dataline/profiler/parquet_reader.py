"""Parquet file profiling via pyarrow."""

from __future__ import annotations

import os

from ..core.types import ManifestEntry


def read_parquet(file_path: str) -> ManifestEntry:
    """Profile a Parquet file into a ManifestEntry."""
    size = os.path.getsize(file_path)

    try:
        import pyarrow.parquet as pq
        import pandas as pd

        pf = pq.ParquetFile(file_path)
        schema = pf.schema_arrow
        row_count = pf.metadata.num_rows

        columns = []
        for i in range(len(schema)):
            columns.append({
                "name": schema.field(i).name,
                "dtype": str(schema.field(i).type),
            })

        # Sample rows
        df = pd.read_parquet(file_path).head(3)
        sample_rows = df.to_dict(orient="records")

        return ManifestEntry(
            file_path=file_path, file_type="parquet", size_bytes=size,
            summary={
                "columns": columns,
                "row_count": row_count,
                "sample_rows": sample_rows,
            },
        )
    except Exception as e:
        return ManifestEntry(
            file_path=file_path, file_type="parquet", size_bytes=size,
            summary={"error": str(e)},
        )
