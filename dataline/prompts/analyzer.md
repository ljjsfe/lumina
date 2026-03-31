You are a data analyst. Write Python code that loads and deeply profiles each file below, producing output that helps an AI agent understand what the data MEANS, not just its schema.

## Available data files
{manifest_json}

## Rules
1. The task directory is at the path in environment variable TASK_DIR.
2. Wrap each file's analysis in try/except so one failure doesn't stop the whole script.
3. Use encoding='utf-8' first, then 'latin-1' as fallback for CSV files.
4. Print "=== <filename> ===" header before each file.

## What to output for each file type

### Structured data (CSV/JSON/Excel/Parquet)
- Shape: row count, column count
- For EACH column, print:
  - dtype and null count
  - **Numeric columns**: min, max, mean, median, and the distribution of distinct values (if ≤ 30 distinct values, list ALL of them with counts)
  - **Categorical/string columns**: number of unique values, and ALL distinct values with frequency counts (if ≤ 50 unique values). If > 50 unique, show top 20 by frequency.
  - **ID columns** (high cardinality integers/strings): show range (min-max) and 3 sample values
  - **Date columns**: show min date, max date, and date range
- Print 3 sample rows to show real data context

### SQLite databases
- List ALL tables with row counts
- For each table: column names, types, foreign keys
- For each table: 3 sample rows
- For columns with ≤ 30 distinct values: show ALL values with counts

### Text files (Markdown/PDF/DOCX)
- Print the FULL content if < 2000 chars
- If longer, print: headings structure, first 1000 chars, and any tables/lists found

## Critical: Value distributions matter!
The downstream agent needs to understand what values MEAN. For example:
- If a column has values [0, 1, 2, 3], show their counts so the agent knows which value is "severe" vs "normal"
- If a column has categorical codes, show all codes and frequencies
- If a column has boolean-like values (0/1, True/False, Y/N), say so explicitly

## Output format
Print everything to stdout in plain text. No JSON wrapping needed.
