"""Value-based join key validation.

Goes beyond column name overlap: checks if shared column names
have actual value overlap between two data sources.
Uses sample values already in manifest — no file re-read.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..core.types import ManifestEntry


@dataclass(frozen=True)
class JoinHint:
    """Validated join key between two sources."""
    column_name: str
    source_a: str
    source_b: str
    value_overlap_pct: float  # 0.0 to 1.0
    confidence: float         # combined confidence score


def validate_join_keys(
    entry_a: ManifestEntry,
    entry_b: ManifestEntry,
    shared_cols: set[str],
) -> list[JoinHint]:
    """For each shared column, check actual value overlap percentage.

    Uses sample values from manifest summaries (no file I/O).
    """
    hints: list[JoinHint] = []

    for col_name in sorted(shared_cols):
        values_a = _extract_values_for_column(entry_a, col_name)
        values_b = _extract_values_for_column(entry_b, col_name)

        if not values_a or not values_b:
            # No sample values to compare — fall back to name-only hint
            hints.append(JoinHint(
                column_name=col_name,
                source_a=entry_a.file_path,
                source_b=entry_b.file_path,
                value_overlap_pct=0.0,
                confidence=0.3,  # low confidence: name match only
            ))
            continue

        set_a = {str(v).lower().strip() for v in values_a}
        set_b = {str(v).lower().strip() for v in values_b}
        overlap = set_a & set_b

        smaller = min(len(set_a), len(set_b))
        overlap_pct = len(overlap) / smaller if smaller > 0 else 0.0

        # Confidence: name match (0.3) + value overlap bonus (up to 0.7)
        confidence = 0.3 + 0.7 * overlap_pct

        hints.append(JoinHint(
            column_name=col_name,
            source_a=entry_a.file_path,
            source_b=entry_b.file_path,
            value_overlap_pct=round(overlap_pct, 3),
            confidence=round(min(confidence, 1.0), 2),
        ))

    return hints


def _extract_values_for_column(entry: ManifestEntry, col_name: str) -> list:
    """Extract sample values for a specific column from manifest summary."""
    s = entry.summary
    col_lower = col_name.lower()

    # Flat columns (CSV, JSON, Parquet)
    for col in s.get("columns", []):
        if col.get("name", "").lower() == col_lower:
            return col.get("sample", []) + _extract_top_values(col)

    # SQLite tables
    for table in s.get("tables", []):
        for col in table.get("columns", []):
            if col.get("name", "").lower() == col_lower:
                return col.get("sample", []) + _extract_top_values(col)

    # Excel sheets
    for sheet in s.get("sheets", []):
        for col in sheet.get("columns", []):
            if col.get("name", "").lower() == col_lower:
                return col.get("sample", []) + _extract_top_values(col)

    return []


def _extract_top_values(col_info: dict) -> list:
    """Extract values from top_values if enriched stats are present."""
    top = col_info.get("top_values", [])
    return [entry["value"] for entry in top if "value" in entry]
