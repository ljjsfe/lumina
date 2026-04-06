Format the final answer from the completed analysis.

## Question
{question}

## Analysis results (all steps and their outputs)
{steps_summary}

## Rules
1. Extract the answer from the step results above.
2. Format as a JSON object representing a table: {"columns": {"col_name": [values]}}
3. Column names should match what the question asks for. Use the exact wording from the question when possible.
4. Include ALL requested data. If the question asks for ID, name, and value — include all three columns.
5. Clean numbers: no $ or % unless the question specifically asks for formatted values.
6. Remove extra whitespace from string values.
7. If the question asks for a single value, still format as: {"columns": {"answer": [value]}}
8. Use the LAST successful step's output as the primary data source. Earlier steps may have been exploratory.
9. CRITICAL: Preserve the EXACT precision of computed numbers. Copy the number exactly as it appears in the step output. Do NOT round, truncate, or reformat numbers. Example: if output says "0.31555732286030097", your answer must be "0.31555732286030097", NOT "0.32" or "0.316".
10. If a step produced a WARNING about 0 rows or empty results, do NOT use that step's output — look for earlier valid results.
11. If the question asks multiple sub-questions (e.g., "What is X and Y?", "Find A, B, and C"), your output MUST have a separate column or value for EACH sub-question. Never merge multiple answers into a single column.
12. All lists in the output MUST have the same length. A table with mismatched column lengths is invalid.
13. "Not Applicable" rules — distinguish two cases:
    - **Legitimate NA**: You successfully queried the data and confirmed the data genuinely does not contain the information needed (e.g., column exists but all values are null, the entity asked about does not appear in the dataset). "Not Applicable" is the correct answer.
    - **Code failure NA**: The code raised an error, timed out, returned 0 rows due to a wrong filter, or you are unsure. You MUST NOT output "Not Applicable" — instead output your best partial answer from earlier successful steps. Never disguise a code failure as "Not Applicable".

## Output (JSON only, no other text)
{"columns": {"column_name_1": [val1, val2, ...], "column_name_2": [val1, val2, ...]}}
