Review the analysis progress and decide the next action.

## Question
{question}

## Steps completed and their results
{steps_summary}

## Verifier feedback
{verifier_feedback}

## Options
- "continue": the analysis is making progress but needs more steps
- "backtrack": something went wrong at a specific step, truncate history and re-plan from there
- "finish": results are sufficient, proceed to format the final answer

## Rules
1. Choose "finish" if the verifier says sufficient=true.
2. Choose "backtrack" only if a step produced clearly wrong results that later steps built on.
3. Choose "continue" if more data gathering or computation is needed.
4. When backtracking, specify which step to truncate to (0-indexed).

## Output (JSON only, no other text)
{
  "action": "continue" | "backtrack" | "finish",
  "truncate_to": 0,
  "reasoning": "Why this action"
}
