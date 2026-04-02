Evaluate this data analysis task and decide the next action.

## Question
{question}

## Analysis Context
{analysis_context}

---

## Sufficiency Check

Work through ALL checks in order. One failure = NOT sufficient.

### Structural checks (mechanical — cannot be rationalized away)

**S1. Step count** — Count completed steps in the context.
- If only 1 step has been executed: NOT sufficient, unless the question is a trivial single-lookup (e.g., "how many rows are in X?"). For any question requiring a filter + aggregation, join, formula, or comparison: 2+ steps required.
- If the question has multiple sub-questions: each sub-question typically needs at least 1 dedicated step.

**S2. Explicit answer in stdout** — The FINAL numeric/string answer must be explicitly printed in the latest stdout. "Intermediate results exist" is not enough. The printed value must correspond to what the question asks, not just any number.

**S3. Answer is labeled** — The printed value must be accompanied by enough context (column name, description, or print statement) to identify what it represents. An unlabeled number is ambiguous.

### Semantic checks (require interpretation)

**S4. Code logic audit** — Check latest step's code:
- Filter values match the question (not inverted, not guessed from thin air)?
- Correct column used?
- Correct aggregation (avg vs sum vs count vs median)?
- If domain documentation exists: exact formula applied? Quote the relevant rule.

**S5. Sub-question coverage** — Identify each sub-question in the original question. For each one: is there a corresponding printed value in stdout? Missing any → NOT sufficient.

**S6. Anti-pattern scan** — Any one = NOT sufficient:
1. Exploration only (printed dtypes / head / describe, no computed answer)
2. Zero-row filter (filter returned 0 rows, or stdout contains "WARNING" about empty results)
3. Intermediate only (computed subset or join, but did not compute the final metric)
4. Filter value absent from data profile (will silently match nothing)
5. Under-computed (question requires filter→aggregate→formula but only one operation done)
6. Sub-question gap (N sub-questions asked, M < N answered in stdout)
7. Sanity failure (percentage outside 0–100%, count > total rows, implausibly exact zero, extreme outlier with no explanation)

---

## Action

- **finish** — ALL structural and semantic checks pass. Final answer is explicit, labeled, and sane.
- **continue** — Making progress. Guidance must be specific: exact file, column, operation needed.
- **backtrack** — A prior step has a logic error that later steps built on. Set `truncate_to`.
- **verify** — Answer is plausible but warrants independent cross-check via a different computation path. Include `verification_code`. Only for complex multi-step results where an alternative path clearly exists.
- **replan** — The wrong data source or column is being targeted entirely. Only when continue/backtrack cannot fix the fundamental direction error.

## Output (JSON only)
```json
{
  "sufficient": true/false,
  "action": "continue|backtrack|finish|verify|replan",
  "reasoning": "S1 result, S2 result, S3 result, S4 findings, S5 coverage, S6 anti-patterns",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0,
  "verification_code": ""
}
```
