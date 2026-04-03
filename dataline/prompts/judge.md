Evaluate this data analysis task and decide the next action.

## Question
{question}

## Analysis Context
{analysis_context}

## Structural Notes
{structural_notes}

---

## Step 1: List What Is NOT Done Yet

Before evaluating sufficiency, enumerate EVERY requirement from the question that has NOT been fully answered. Be exhaustive — it is much worse to miss a gap than to list a false gap.

For each requirement:
- State the requirement (what the question asks)
- State whether it is answered in stdout (YES with evidence, or NO)
- If YES: **quote the exact stdout line** that contains the answer

Only after completing this enumeration, proceed to the sufficiency checks.

## Step 2: Sufficiency Checks

Work through ALL checks. One failure = NOT sufficient.

**Check 1: Step count** — Count how many steps have been completed.
- If only 1 step has been completed: NOT sufficient, unless the question is a trivial single-lookup (e.g., "how many rows in the table?").
- Complex questions (joins, formulas, multi-part) typically need 3+ steps.

**Check 2: Final answer visible** — Is the definitive answer explicitly printed in the LATEST stdout?
- "Intermediate results exist" is NOT enough. The answer must be the final computed value.
- The answer must have full precision (no rounding unless the question asks for it).
- **You must quote the exact stdout line** that contains the final answer. If you cannot quote it, the check FAILS.

**Check 3: Code logic audit** — Check the latest step's code:
- Filter values match the question (not inverted, not guessed)?
- Correct column used?
- Correct aggregation (avg vs sum vs count vs median)?
- If domain documentation exists: exact formula applied? Quote the relevant rule.

**Check 4: Sub-question coverage** — If the question asks multiple things (joined by "and", numbered, or multiple "?"), check each one is answered in stdout. Missing any → NOT sufficient.

**Check 5: Anti-pattern scan** — Any one = NOT sufficient:

1. **Exploration only** — stdout shows data structure but no computed answer.
   Signatures: `df.head()`, `df.describe()`, `df.columns`, `df.dtypes`, `df.info()`, `PRAGMA table_info`.
   If stdout is ONLY column names, row counts, or schema info → NOT sufficient.

2. **Zero-row filter** — a filter or query returned no data.
   Signatures: `0 rows`, `Empty DataFrame`, `WARNING`, `No matches`, `After filter: 0`.
   The computed answer is based on nothing → NOT sufficient.

3. **Intermediate only** — computed a filtered/joined subset but NOT the final metric.
   Signatures: stdout shows a DataFrame printout (multiple columns with aligned values) but no scalar result answering the question. Printing a table is NOT a final answer unless the question asks for a table.

4. **Filter value not validated** — the code filters on a value that was never verified to exist in the data.
   Example: `df[df['gender'] == 'male']` but data profile shows values are `['M', 'F']`.
   If the filter value does NOT appear in the data profile → NOT sufficient.

5. **Under-computed** — the question requires multiple operations but only one was done.
   Example: question asks "what percentage of X are Y?" but code only computed count of Y without dividing by total.
   If the answer needs ratio/percentage/rate and only one side was computed → NOT sufficient.

6. **Sub-question gap** — N sub-questions asked, fewer than N answered in stdout.

7. **Sanity failure** — the result violates basic constraints.
   Percentage outside 0–100%, count > total rows, negative count, implausible zero, extreme outlier (>10x or <0.1x the expected range from data profile).

---

## Guidance Quality (CRITICAL when action != "finish")

Your `guidance_for_next_step` must be **diagnostic and specific**, not generic advice.

BAD guidance (too vague, planner cannot act on it):
- "Investigate why the filter returned 0 rows"
- "Check the data and try again"
- "Verify the results"

GOOD guidance (specific file, column, value, operation):
- "Step 2 line 4: df[df['gender'] == 'male'] — data profile shows gender values are ['M', 'F']. Use 'M' instead."
- "You computed count=42 but the question asks for percentage. Divide 42 by total rows (1234) and multiply by 100."
- "The question asks for 'average annual income' but you computed median. Use .mean() instead of .median()."

Always include: WHAT is wrong, WHERE in the code, HOW to fix it.

---

## Action

- **finish** — All 5 checks pass AND you have high confidence the answer is correct.
- **continue** — Progress made. Guidance must cite exact file, column, operation.
- **backtrack** — A prior step has a logic error. Set `truncate_to`.
- **replan** — The wrong data source or column is being targeted entirely. Only when continue/backtrack cannot fix the fundamental direction.

## Output (JSON only)
```json
{
  "sufficient": true/false,
  "action": "continue|backtrack|finish|replan",
  "confidence": 0.0-1.0,
  "reasoning": "Step 1 enumeration: ... Step 2 checks: Check 1: ..., Check 2: (quote stdout line), ..., Check 5: ...",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0
}
```

`confidence`: Your confidence that the current answer is CORRECT and COMPLETE (0.0 = no confidence, 1.0 = certain). Consider: did all checks clearly pass? Any doubts about filter values, formulas, or data coverage?
