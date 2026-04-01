Fix the following Python code that failed during data analysis execution.

## Code that failed
```python
{failed_code}
```

## Error (full traceback)
```
{full_traceback}
```

## Error Summary
{error_type}: {error_message}

## Error Category: {error_category}

### Fix strategy by category

**data** (KeyError, FileNotFoundError, JSONDecodeError, UnicodeDecodeError):
- The error is about WHAT data looks like, not code logic.
- FIRST: check exact column names, file paths, and data types in the data context below.
- Fix: match column names exactly (case-sensitive), use safe_read_csv/safe_read_json for path resolution, use describe_data() to inspect structure before accessing fields.

**type** (TypeError, ValueError, AttributeError):
- The error is about HOW data is being used.
- Common cause: mixed-type columns (e.g., list values in a DataFrame column), wrong dtype assumptions.
- Fix: check column dtypes in data context, cast explicitly, handle None/NaN before operations, avoid .nunique() on unhashable columns.

**resource** (MemoryError, TimeoutError):
- The error is about data SIZE.
- Fix: use chunked reading, sample data, or more efficient operations (e.g., SQL aggregation instead of loading full table).

**logic** (all other errors):
- The code logic is wrong.
- Re-read the error message carefully. Check if the algorithm matches the intended analysis.

## Retry context
{retry_context}

## Data context (column names, types, sample values, value distributions)
{data_context}

## Rules
1. Fix the SPECIFIC error shown in the traceback. Follow the strategy for error category "{error_category}".
2. Use the data context above to verify column names, types, and values BEFORE writing the fix.
3. If this is a retry, try a FUNDAMENTALLY DIFFERENT approach — do not just tweak the same failing logic.
4. Common fixes:
   - KeyError/column not found → check exact column names in data context; try case-insensitive matching
   - TypeError → check column dtypes and data structure (list vs dict vs DataFrame)
   - FileNotFoundError → use `safe_read_csv()` or `safe_read_json()` which auto-resolve paths
   - sqlite3 error → check table/column names via `PRAGMA table_info()`; use quotes around table/column names with spaces
   - "unhashable type: list" → don't use list columns as dict keys; iterate instead
   - JSONDecodeError → check if JSON is nested or has unexpected structure; use `describe_data()`
   - UnicodeDecodeError → use encoding="latin-1" or errors="replace"
5. Return the COMPLETE fixed code (all imports, all logic).

## Retry strategy (if this is attempt 2+)
- If the SAME error recurs: the approach is wrong. Try a completely different method.
  - If pandas failed on a CSV, try reading it with different parameters (delimiter, header).
  - If sqlite3 query failed, try loading the table into pandas first.
  - If JSON parsing failed, try reading as text and parsing manually.
- If a DIFFERENT error occurs: fix this new error while keeping the previous fix.
- NEVER repeat code from previous attempts that produced the same error.

## Output
Return the fixed Python code wrapped in ```python ... ```.
Add a brief comment at the top explaining what you changed.
