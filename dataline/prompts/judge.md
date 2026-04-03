Evaluate this data analysis task and decide the next action.

## Question
{question}

## Analysis Context
{analysis_context}

---

## Sufficiency Check

Work through ALL checks in order. One failure = NOT sufficient.

### Structural checks (mechanical — cannot be rationalized away)

**S1. Step type gate** — Look at the `[step_type]` tag in each completed step.
- If NO step has `[final_answer]` type: NOT sufficient. The final answer has not been produced yet.
- If the Answer Schema specifies `required_steps_min`, count completed steps. Fewer than that → NOT sufficient.
- Exception: a trivial single-lookup question (e.g., "how many rows?") may finish in 1 step.

**S2. [ANSWER] marker** — The latest stdout MUST contain a line starting with `[ANSWER]`. This is the explicit final answer.
- If no `[ANSWER]` marker in stdout: NOT sufficient — only intermediate results exist.
- The value after `[ANSWER]` must directly answer the question, not just show data.

**S3. Answer labeled** — The `[ANSWER]` line must include enough context to identify what it represents (column name, description, or print label). An unlabeled bare number is ambiguous.

### Semantic checks (require interpretation)

**S4. Code logic audit** — Check latest step's code:
- Filter values match the question (not inverted, not guessed)?
- Correct column used?
- Correct aggregation (avg vs sum vs count vs median)?
- If domain documentation exists: exact formula applied? Quote the relevant rule.

**S5. Sub-question coverage** — If the Answer Schema lists sub-questions, check each one: is it answered in stdout? Count matched vs total. Missing any → NOT sufficient.

**S6. Anti-pattern scan** — Any one = NOT sufficient:
1. Exploration only (printed dtypes / head / describe, no computed answer)
2. Zero-row filter (filter returned 0 rows, or stdout contains "WARNING")
3. Intermediate only (computed subset or join, but not the final metric)
4. Filter value absent from data profile (silent no-match)
5. Under-computed (question requires filter→aggregate→formula but only one operation done)
6. Sub-question gap (N asked, M < N answered)
7. Sanity failure (percentage outside 0–100%, count > total rows, implausible zero or extreme)

---

## Action

- **finish** — ALL structural and semantic checks pass.
- **continue** — Progress made. Guidance must be specific: exact file, column, operation.
- **backtrack** — Prior step has logic error. Set `truncate_to`.
- **verify** — Answer plausible, want cross-check via different computation path. Include `verification_code`. Only for complex multi-step results.
- **replan** — Wrong data source or column entirely. Only when continue/backtrack cannot fix.

## Output (JSON only)
```json
{
  "sufficient": true/false,
  "action": "continue|backtrack|finish|verify|replan",
  "reasoning": "S1:..., S2:..., S3:..., S4:..., S5:..., S6:...",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0,
  "verification_code": ""
}
```
