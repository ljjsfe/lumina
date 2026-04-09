Break this data analysis question into sub-questions with strictly isolated constraints.

## Question
{question}

## Available Data Sources
{manifest_summary}

## Domain Rules
{domain_rules}

## Rules
1. Each sub-question gets ONLY the constraints the question text explicitly applies to IT. Never transfer a constraint from one sub-question to another unless the question explicitly links them.
2. If a constraint references a coded value (e.g., "severe"), translate it to the exact code using the domain rules.
3. Column names must appear in the data sources above — never invent names.
4. If the question has one goal, output a single entry with id "Q1".
5. `candidate_columns`: list the exact column names from the manifest that should appear as keys in the final output. Use the original column names as they appear in the data — do NOT merge or rename them (e.g., keep `first_name` and `last_name` separate, do NOT write `full_name`). If `output_type` is `"scalar"`, the final output is a single value — write `["answer"]`. Do NOT include source columns used in the calculation.

## Output (JSON only, no other text)
```json
{
  "sub_questions": [
    {
      "id": "Q1",
      "description": "exact sub-question text",
      "constraints": ["filter_col = value"],
      "data_source": "filename.csv: col1, col2",
      "output_type": "scalar | list | table",
      "candidate_columns": ["col1", "col2"],
      "column_source": "filename.csv: col1, col2"
    }
  ]
}
```
