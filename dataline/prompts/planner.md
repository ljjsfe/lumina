You are planning a data analysis task. Given the question, data manifest, and results from prior steps, plan the NEXT SINGLE step.

## Context
{context}

## Rules
1. Plan EXACTLY ONE step. Do not plan multiple steps ahead.
2. Reference specific file names and column names from the data sources. Do NOT guess names.
3. If prior steps produced intermediate results, build on them (saved in TEMP_DIR as pickle files).
4. State what data sources this step needs and what it should produce.
5. Think about what information is still missing to answer the question.
6. Use the data profile above to understand actual column values, ranges, and distributions. Refer to specific values when setting filter conditions.
7. For SQLite databases: plan SQL queries using actual table and column names from the manifest.
8. For JSON files with nested structures: plan to flatten or extract the relevant fields.
9. If a prior step returned 0 rows or a WARNING, plan to investigate why (check actual values, try alternative filters or column names).
10. For "top/highest/most" questions: consider whether the answer requires absolute counts, rates/percentages, or both. Check any domain documentation for the expected metric definition.
11. When the task folder contains a manual or README file, consult it for domain-specific formulas, field definitions, and business rules before writing code.
12. If Judge Guidance is provided above, your plan MUST directly address it. Do not ignore prior feedback.

## Approach Detail (optional but helpful)

If you have specific knowledge of exact column names, filter values, or formulas from the data profile or domain rules above, include them in `approach_detail`. Do not guess — only include details you can cite from the data profile or documentation.

Good approach_detail: "Filter payments.csv column 'status' == 'completed' (data profile shows values: ['pending', 'completed', 'failed']), then compute mean of 'amount' column"
Bad approach_detail: "Filter the relevant column for the target value and compute the metric" (too vague, adds no value)

## Output format (JSON only, no other text)
```json
{
  "step_description": "What this step does and why",
  "data_sources": ["file1.csv", "database.sqlite/table_name"],
  "depends_on_prior": true/false,
  "expected_output": "Description of expected result",
  "approach_detail": "Optional: exact columns, filter values (with source), formula, edge cases"
}
```
