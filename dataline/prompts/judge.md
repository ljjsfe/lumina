Evaluate this data analysis task and decide the next action.

## Question
{question}

## Analysis Context
{analysis_context}

---

## Audit Checklist

**A. Code Logic** — scan the latest step's code:
- Filter values match the question (not inverted, not guessed)?
- Operating on the correct column?
- Correct aggregation (avg/sum/count/median)?
- Domain rule formula applied exactly? (Quote the rule if docs exist.)

**B. Sub-question Coverage** — count sub-questions in the original question, then verify each has a printed answer in stdout. Missing any → NOT sufficient.

**C. 7 Anti-Patterns** — any one means NOT sufficient:
1. Exploration only (printed dtypes/head/describe, no computed answer)
2. Zero-row filter (filter returned 0 rows or stdout has "WARNING")
3. Intermediate only (computed subset/join but not the final metric)
4. Filter value absent from data profile (silent no-match)
5. Under-computed (2+ operations needed, only 1 done)
6. Sub-question gap (N questions asked, M < N answered)
7. Sanity failure (percentage outside 0–100%, count > total rows, implausible zero or extreme value)

---

## Action

Choose one:
- **finish** — All checks pass, final answer is in stdout, sanity OK.
- **continue** — Making progress. Give specific guidance: exact file, column, computation.
- **backtrack** — Prior step has a logic error. Set `truncate_to` to revert.
- **verify** — Answer is plausible but needs independent cross-check via a different computation path. Include `verification_code`. Use only for complex multi-step results where an alternative path exists.
- **replan** — Wrong data source or column entirely. Only when continue/backtrack cannot fix it.

## Output (JSON only)
```json
{
  "sufficient": true/false,
  "action": "continue|backtrack|finish|verify|replan",
  "reasoning": "Checklist findings",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0,
  "verification_code": ""
}
```
