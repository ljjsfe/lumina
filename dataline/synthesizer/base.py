"""Synthesizer: convert agent answer dict to prediction.csv."""

from __future__ import annotations

import pandas as pd

from .normalizer import normalize_value


def to_dataframe(answer: dict, normalize: bool = True) -> pd.DataFrame:
    """Convert answer dict {"col": [values]} to DataFrame."""
    if not answer:
        return pd.DataFrame()

    # Ensure all columns have same length
    max_len = max((len(v) if isinstance(v, list) else 1) for v in answer.values())
    clean = {}
    for col, values in answer.items():
        if not isinstance(values, list):
            values = [values]
        # Pad shorter columns
        if len(values) < max_len:
            values = values + [None] * (max_len - len(values))
        if normalize:
            values = [normalize_value(v) for v in values]
        clean[col] = values

    return pd.DataFrame(clean)


def save_prediction(answer: dict, output_path: str, normalize: bool = True) -> str:
    """Save answer as prediction.csv. Returns the path."""
    df = to_dataframe(answer, normalize=normalize)
    df.to_csv(output_path, index=False)
    return output_path
