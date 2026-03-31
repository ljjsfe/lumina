You are verifying whether accumulated analysis results are sufficient to answer a question.

## Question
{question}

## Steps completed and their results
{steps_summary}

## Rules
1. Check if ALL parts of the question have been answered.
2. Check if the results contain actual data (not just schema/metadata).
3. Check if numbers are specific (not placeholders or approximations unless the question asks for that).
4. If the question asks for multiple items (e.g., "list X and Y"), verify both X and Y are present.

## Output (JSON only, no other text)
{
  "sufficient": true/false,
  "reasoning": "Why the results are or aren't enough to answer the question",
  "missing": "What specific information is still needed (empty string if sufficient)"
}
