"""Auto-discover entity relations across data sources.

Enhanced with value-based join validation (not just column name overlap).
"""

from __future__ import annotations

from ..core.types import CrossSourceRelation, ManifestEntry
from .join_validator import validate_join_keys


def discover_relations(entries: list[ManifestEntry]) -> list[CrossSourceRelation]:
    """Find column name overlaps and value overlaps across structured sources."""
    structured = [e for e in entries if e.file_type in ("csv", "json", "sqlite", "excel", "parquet")]
    text_entries = [e for e in entries if e.file_type in ("markdown", "pdf", "docx")]

    relations: list[CrossSourceRelation] = []

    # 1. Column name overlap + value-based validation
    for i, a in enumerate(structured):
        cols_a = _get_column_names(a)
        if not cols_a:
            continue
        for b in structured[i + 1:]:
            cols_b = _get_column_names(b)
            if not cols_b:
                continue
            overlap = cols_a & cols_b
            if overlap:
                # Validate with actual value overlap
                hints = validate_join_keys(a, b, overlap)
                for hint in hints:
                    relations.append(CrossSourceRelation(
                        source_a=a.file_path,
                        source_b=b.file_path,
                        relation=(
                            f"Shared column '{hint.column_name}'"
                            f" (value overlap: {hint.value_overlap_pct:.0%})"
                        ),
                        confidence=hint.confidence,
                    ))

    # 2. Structured source values mentioned in text documents
    for struct_entry in structured:
        sample_values = _get_sample_values(struct_entry)
        if not sample_values:
            continue
        for text_entry in text_entries:
            text_preview = text_entry.summary.get("text_preview", "")
            if not text_preview:
                continue
            matches = [v for v in sample_values if str(v) in text_preview]
            if matches:
                relations.append(CrossSourceRelation(
                    source_a=struct_entry.file_path,
                    source_b=text_entry.file_path,
                    relation=f"Document mentions values from structured data: {matches[:5]}",
                    confidence=round(min(len(matches) / max(len(sample_values), 1), 1.0), 2),
                ))

    return relations


def _get_column_names(entry: ManifestEntry) -> set[str]:
    """Extract column names from a structured entry."""
    names: set[str] = set()
    s = entry.summary

    if "columns" in s:
        for col in s["columns"]:
            name = col.get("name", "")
            if name:
                names.add(name.lower())

    if "tables" in s:
        for table in s["tables"]:
            for col in table.get("columns", []):
                name = col.get("name", "")
                if name:
                    names.add(name.lower())

    if "sheets" in s:
        for sheet in s["sheets"]:
            for col in sheet.get("columns", []):
                name = col.get("name", "")
                if name:
                    names.add(name.lower())

    return names


def _get_sample_values(entry: ManifestEntry) -> list[str]:
    """Extract sample string values from a structured entry."""
    values: list[str] = []
    s = entry.summary

    if "columns" in s:
        for col in s["columns"]:
            for v in col.get("sample", []):
                if isinstance(v, str) and len(v) > 2:
                    values.append(v)

    if "tables" in s:
        for table in s["tables"]:
            for col in table.get("columns", []):
                for v in col.get("sample", []):
                    if isinstance(v, str) and len(v) > 2:
                        values.append(v)

    if "sheets" in s:
        for sheet in s["sheets"]:
            for col in sheet.get("columns", []):
                for v in col.get("sample", []):
                    if isinstance(v, str) and len(v) > 2:
                        values.append(v)

    return values[:50]
