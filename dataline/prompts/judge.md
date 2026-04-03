Evaluate this data analysis task and decide the next action.

## Question
{question}

## Analysis Context
{analysis_context}

---

## Sufficiency Check

Work through ALL checks. One failure = NOT sufficient.

**Check 1: Step count** — Count how many steps have been completed.
- If only 1 step has been completed: NOT sufficient, unless the question is a trivial single-lookup (e.g., "how many rows in the table?").
- Complex questions (joins, formulas, multi-part) typically need 3+ steps.

**Check 2: Final answer visible** — Is the definitive answer explicitly printed in the LATEST stdout?
- "Intermediate results exist" is NOT enough. The answer must be the final computed value.
- The answer must have full precision (no rounding unless the question asks for it).

**Check 3: Code logic audit** — Check the latest step's code:
- Filter values match the question (not inverted, not guessed)?
- Correct column used?
- Correct aggregation (avg vs sum vs count vs median)?
- If domain documentation exists: exact formula applied? Quote the relevant rule.

**Check 4: Sub-question coverage** — If the question asks multiple things (joined by "and", numbered, or multiple "?"), check each one is answered in stdout. Missing any → NOT sufficient.

**Check 5: Anti-pattern scan** — Any one = NOT sufficient:
1. Exploration only (printed dtypes / head / describe / column names, no computed answer)
2. Zero-row filter (filter returned 0 rows, or stdout contains "WARNING" about empty results)
3. Intermediate only (computed a filtered subset or join, but did NOT compute the final metric)
4. Filter value absent from data profile (the filter will silently match nothing)
5. Under-computed (question needs filter→aggregate→divide but only one operation was done)
6. Sub-question gap (N sub-questions asked, fewer than N answered)
7. Sanity failure (percentage outside 0–100%, count > total rows, implausible zero, extreme outlier)

---

## Action

- **finish** — All 5 checks pass. Final answer is visible and correct.
- **continue** — Progress made. Guidance must be specific: exact file, column, operation.
- **backtrack** — A prior step has a logic error. Set `truncate_to`.
- **replan** — The wrong data source or column is being targeted entirely. Only when continue/backtrack cannot fix the fundamental direction.

## Output (JSON only)
```json
{
  "sufficient": true/false,
  "action": "continue|backtrack|finish|replan",
  "reasoning": "Check 1: ..., Check 2: ..., Check 3: ..., Check 4: ..., Check 5: ...",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0
}
```
