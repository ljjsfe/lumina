You are evaluating the progress of a data analysis task. Follow the steps below in order.

## Question
{question}

## Iteration Progress
Iteration {iteration} of {max_iterations}.

## Analysis Context
{analysis_context}

---

## Evaluation Steps (follow in order — do NOT skip)

### Step 1 — Quote the answer from stdout (REQUIRED)

Before any verdict, copy the exact answer from the latest step's stdout.
- If stdout contains a number, ratio, or final value: quote it verbatim.
- If stdout contains a result table: quote the most relevant rows.
- If stdout contains nothing useful (schema only, 0 rows, error): write "no answer found".

This becomes your `quoted_answer`. You cannot skip this step.

### Step 2 — Three Blocking Checks

Run exactly these three checks. If any fails, set `sufficient: false` and choose "continue" or "backtrack".

**Check A — Answer presence**
Does the stdout contain a real computed answer (a number, list of values, or named result) that directly addresses the question?
FAIL if stdout shows only: `dtypes`, `columns`, `describe`, `df.head()`, `PRAGMA table_info`, or prints "0 rows" / "Empty DataFrame" / "After filter: 0".

**Check B — Logic correctness**
Is there a visible logic error in the latest code?
FAIL if: filter direction is inverted (e.g., `>` should be `<`), wrong column is aggregated, wrong join key used, or a filter that should match many rows returns 0 rows.
Use domain rules and data profile (if available in context) to verify filter values and formulas.

**Check C — Exploration vs. answer**
Is this step's output purely exploratory (printing schema, sample rows, data types for debugging), with no final answer yet computed?
FAIL if yes.

If all three checks pass → proceed to Step 3.

### Step 3 — Iteration Leniency

- Iterations 0 to {max_iterations_minus_2}: apply checks strictly.
- Final 2 iterations (>= {max_iterations_minus_2}): be lenient. If there is a reasonable computed value in stdout that partially answers the question, choose "finish". Accept incomplete answers rather than iterating further.
- Last iteration ({max_iterations_minus_1}): choose "finish" unless there is an obvious logic error.

### Step 4 — Domain Rule Verification (skip if no domain rules in context)

If domain rules are present:
- Does the code follow the documented formula exactly?
- Are field semantics respected (NULL handling, coded values)?
- If a rule was violated → it's a blocking logic error. Choose "backtrack" or "continue" with the specific correction.

---

## Actions
- **"finish"**: All blocking checks pass and the answer is visible in stdout.
- **"continue"**: Making progress but more work needed. Provide specific guidance for the next step.
- **"backtrack"**: A prior step used wrong logic (inverted filter, wrong column, wrong join). Set `truncate_to` to the step index to keep before (0 = restart from scratch).

### Backtrack on empty computation results (ZERO_ROWS flag)

If the Pre-check Evidence includes a `ZERO_ROWS` flag **and** the step that produced it was a computation step (not exploratory schema inspection), set `action` to `"backtrack"`. Use the `truncate_to=N` value from the flag if provided; otherwise use `truncate_to=0`. Do not continue iterating on a step that computed an empty result — the prior logic is wrong and needs to be re-planned from a clean state.

## Output (JSON only, no other text)
```json
{
  "quoted_answer": "exact value/text from stdout answering the question, or 'no answer found'",
  "sufficient": true,
  "action": "finish",
  "reasoning": "Brief explanation referencing your check results",
  "missing": "",
  "guidance_for_next_step": "",
  "truncate_to": 0
}
```
