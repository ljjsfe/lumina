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
6. If the data does not support answering the question, respond with "Not Applicable".
7. Do NOT include column headers, units, or extra text — just the raw answer value.

## Output (JSON with single "answer" key, no other text)
{"answer": "your_answer_here"}
