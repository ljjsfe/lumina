You are evaluating a data analysis task. Audit the code logic, then decide the next action.

## Question
{question}

## Analysis Context
{analysis_context}

---

## Step 1: Audit Code Logic

Check the latest step's code against the question:

1. **Filters** — Does the filter match what the question asks? Watch for inversions (`>` vs `<`), wrong values, wrong columns.
2. **Columns** — Is the code operating on the column the question actually asks about?
3. **Aggregation** — Average vs sum vs count vs median — does it match the question?
4. **Row counts** — If filtering returns 0 rows or a WARNING appears, the logic is likely wrong.
5. **Domain rules** — If domain documentation exists, verify the code uses the **exact formula** and **correct field semantics** (e.g., coded values like `1=positive, 2=severe`). Quote the relevant rule in your reasoning.

If you find a logic error → "backtrack" or "continue" with specific correction.

## Step 2: Assess Sufficiency

The answer is sufficient ONLY when ALL of these are true:
- The **final answer value** is explicitly printed in the latest stdout (not intermediate)
- ALL parts of the question are answered with **full precision** (no rounding unless asked)
- The answer passes a **sanity check**: reasonable magnitude, within data profile ranges, percentages between 0-100%, counts ≤ total rows
- If cross-validation was possible and reveals >5% discrepancy → NOT sufficient

Do NOT allow "finish" if:
- The step only explored/described data (column names, dtypes, samples)
- The step only showed intermediate results (filtered but didn't compute the final metric)
- A computed metric required 2+ steps but only 1 was executed
- The code used a filter value not present in the data profile

## Step 3: Decide Action

- **"finish"** — Final answer is visible in stdout, code logic is correct, sanity check passes.
- **"continue"** — Progress is being made but more work needed. Provide **specific** guidance: which file, which column, what computation. If cross-validation is feasible, suggest it.
- **"backtrack"** — A previous step has a logic error that later steps built on. Specify which step to truncate to.
- **"verify"** — You believe the answer might be correct but want to independently verify it. Write a SHORT Python verification script that checks the answer via a **different computation path** (e.g., count from raw data, recompute using SQL instead of pandas, spot-check a subset). Use this when:
  - The answer seems plausible but you want to cross-validate
  - The computation was complex and an independent check would increase confidence
  - You suspect an edge case (nulls, duplicates, type coercion) may have affected the result
- **"replan"** — The fundamental analysis direction is wrong, not just a code bug. The question is being interpreted incorrectly, or the wrong data sources/columns are being used entirely. This triggers a full strategic re-analysis. Use sparingly — only when continuing or backtracking cannot fix the problem because the entire approach needs rethinking.

If the agent seems stuck (repeating similar approaches), suggest a fundamentally different strategy.

## Output (JSON only)

For most actions:
```json
{
  "sufficient": true/false,
  "action": "continue" | "backtrack" | "finish" | "replan",
  "reasoning": "Audit findings + sufficiency assessment + sanity check result",
  "missing": "What is still needed (empty if sufficient)",
  "guidance_for_next_step": "Specific instruction for next step (empty if finish)",
  "truncate_to": 0
}
```

For "verify" action, include verification code:
```json
{
  "sufficient": false,
  "action": "verify",
  "reasoning": "Why verification is needed",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0,
  "verification_code": "import pandas as pd\nimport os\n# Independent verification script\n# ... cross-check the answer via different method\nprint('VERIFICATION RESULT:', result)"
}
```

The verification script has access to the same environment (TASK_DIR, TEMP_DIR, data_helpers). Keep it SHORT and focused — it should verify, not redo the entire analysis.
