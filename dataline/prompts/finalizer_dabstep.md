Extract the final answer from the completed analysis.

## Question
{question}

## Answer format guidelines
{guidelines}

## Analysis results (all steps and their outputs)
{steps_summary}

## Rules
1. Extract the answer from the step results above.
2. Follow the answer format guidelines EXACTLY.
3. For comma-separated lists: use format "val1, val2, val3" (space after comma).
4. CRITICAL: For numbers, preserve FULL precision from the step output. Copy the exact number. Do NOT round unless the guidelines explicitly specify decimal places.
5. For multiple choice: match the exact format (e.g., "B. BE").
6. "Not Applicable" rules — distinguish two cases:
   - **Legitimate NA**: You successfully queried the data and confirmed the data genuinely does not contain the information needed (e.g., column exists but all values are null, the entity asked about does not appear in the dataset). In this case "Not Applicable" is the correct answer.
   - **Code failure NA**: The code raised an error, timed out, returned 0 rows due to a wrong filter, or you are unsure whether the data supports the answer. In this case you MUST NOT output "Not Applicable" — instead output your best partial answer from earlier successful steps, or an empty string. Never disguise a code failure as "Not Applicable".
7. Do NOT include column headers, units, or extra text — just the raw answer value.

## Output (JSON with single "answer" key, no other text)
{"answer": "your_answer_here"}
