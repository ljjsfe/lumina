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

## Output (JSON only, no other text)
```json
{
  "sub_questions": [
    {
      "id": "Q1",
      "description": "exact sub-question text",
      "constraints": ["filter_col = value"],
      "data_source": "filename.csv: col1, col2",
      "output_type": "scalar | list | table"
    }
  ]
}
```
