# dataline — Design Specification v2

> Single source of truth. Written for Claude Code to execute.
> Informed by SOTA analysis: DS-STAR, NVIDIA KGMON, Smolagents DR.

---

## 1. What is dataline

A command-line tool. You give it a folder of mixed-format data files and a natural language question. It reasons across those files and returns a structured answer table.

```
python main.py --task ./data/demo/input/task_11 --output ./results/task_11
```

Output: `prediction.csv`, `trace.json` (reasoning steps + diagnostics).

### Dual goals

- **Open-source framework**: Clean, extensible architecture for data analysis agents.
- **Competition validation**: KDD Cup 2026 + DABstep as proving grounds.

### Evaluation sources

| Benchmark | Domain | Tasks | Key Challenge |
|-----------|--------|-------|---------------|
| KDD Cup 2026 | Multi-domain, per-task data | 50 demo + Phase 2 | Multi-format, cross-source joins |
| DABstep | Financial payments (Adyen) | 10 dev + full test | Scalar answers, domain-specific reasoning |

---

## 2. Architecture

### Core Loop: Incremental Plan-Code-Verify

Inspired by DS-STAR's iterative architecture. NOT one-shot ReAct or upfront full plan.

```
Input (task dir)
    │
    ▼
Profiler (deterministic, zero LLM cost)
    │ produces Manifest
    ▼
Analyzer (deep data understanding via code generation)
    │ produces DataProfile (semantic summaries per file)
    ▼
Incremental Loop:
    ┌─────────────────────────────────────────┐
    │ Planner → plan NEXT step (not full plan)│
    │     ▼                                   │
    │ Coder → step → Python code              │
    │     ▼                                   │
    │ Sandbox → execute (persistent state)    │
    │     ▼                                   │
    │ Verifier → "sufficient to answer?"      │
    │     ├─ yes → Finalizer → Output         │
    │     ├─ no  → loop (plan next step)      │
    │     └─ error → Debugger → retry/backtrack│
    │                                         │
    │ Router → backtrack? truncate to step N? │
    └─────────────────────────────────────────┘
    │
    ▼
Synthesizer → prediction.csv + trace.json
```

### Agent Roles (from DS-STAR + our extensions)

| Agent | Role | LLM Tier |
|-------|------|----------|
| Analyzer | Generate + execute profiling scripts per file. Produce semantic summaries. | Main |
| Planner | Given question + manifest + prior results → plan ONE next step | Main |
| Coder | Convert plan step → executable Python | Main |
| Verifier | Judge: are accumulated results sufficient to answer the question? | Main |
| Router | Decide: continue / add step / backtrack to step N | Main |
| Debugger | Fix code using traceback + data context (column names, types, samples) | Main |
| Finalizer | Format accumulated results → prediction.csv | Main |

All agents share the same LLM client. No separate "cheap model" for verification in v1 — simplify first, optimize later.

---

## 3. File Structure

```
dataline/
├── main.py                      # CLI entry point
├── config.yaml                  # Model, retries, timeouts
│
├── core/
│   ├── __init__.py
│   ├── types.py                 # Frozen dataclasses
│   ├── llm_client.py            # Unified LLM adapter (Kimi/Moonshot primary)
│   └── sandbox.py               # Python execution (subprocess, persistent temp)
│
├── profiler/
│   ├── __init__.py
│   ├── manifest.py              # Scan task dir → Manifest
│   ├── csv_reader.py
│   ├── json_reader.py
│   ├── sqlite_reader.py
│   ├── markdown_reader.py
│   ├── pdf_reader.py
│   ├── docx_reader.py
│   ├── excel_reader.py
│   ├── image_reader.py
│   ├── parquet_reader.py
│   └── cross_source.py          # Auto-discover entity relations
│
├── agents/
│   ├── __init__.py
│   ├── orchestrator.py          # Main loop: profile → analyze → plan-code-verify
│   ├── analyzer.py              # Deep data profiling via code execution
│   ├── planner.py               # Incremental: plan ONE next step
│   ├── coder.py                 # Plan step → Python code
│   ├── verifier.py              # Sufficiency check
│   ├── router.py                # Continue / backtrack / add step
│   ├── debugger.py              # Fix with traceback + data context
│   └── finalizer.py             # Results → prediction.csv
│
├── synthesizer/
│   ├── __init__.py
│   ├── base.py                  # Results → DataFrame → CSV
│   ├── normalizer.py            # Strip $/%, decimal precision, etc.
│   └── kdd_mode.py              # Extra columns for KDD scoring
│
├── prompts/
│   ├── analyzer.md
│   ├── planner.md
│   ├── coder.md
│   ├── verifier.md
│   ├── router.md
│   ├── debugger.md
│   ├── finalizer.md
│   └── error_context.md
│
├── eval/
│   ├── __init__.py
│   ├── scorer.py                # KDD column-vector matching
│   ├── run_eval.py              # Batch eval → EvalReport with diagnostics
│   ├── failure_analysis.py      # Per-task failure categorization
│   ├── compare.py               # Two-run comparison
│   ├── kdd_adapter.py           # KDD data format adapter
│   ├── dabstep_eval.py          # DABstep evaluation adapter
│   └── diagnostics.py           # Bottleneck analysis + actionable suggestions
│
├── data/
│   ├── demo/                    # KDD Cup Phase 1 demo (50 tasks) ✓ exists
│   └── dabstep/                 # DABstep benchmark data
│
├── docs/
│   ├── DEVELOPMENT.md
│   ├── DECISIONS.md
│   └── BENCHMARKS.md
│
├── tests/
│   ├── test_profiler.py
│   ├── test_scorer.py
│   ├── test_sandbox.py
│   └── test_normalizer.py
│
├── requirements.txt
└── README.md
```

---

## 4. Component Specifications

### 4.1 `core/llm_client.py`

Primary backend: **Kimi (Moonshot AI)**. OpenAI-compatible API.

```python
class LLMClient:
    def __init__(self, provider: str, model: str, api_key: str, base_url: str = None):
        """
        provider: 'moonshot' | 'anthropic' | 'openai' | 'google' | 'deepseek'
        Moonshot uses OpenAI-compatible endpoint: https://api.moonshot.cn/v1
        """

    def chat(self, system: str, user: str, images: list[bytes] = None) -> str:
        """Single-turn chat. Returns text response."""

    def chat_with_cost(self, system: str, user: str, images: list[bytes] = None) -> tuple[str, dict]:
        """Returns (response, {input_tokens, output_tokens, cost_usd, latency_ms})."""
```

Provider config:
- **moonshot**: base_url=https://api.moonshot.cn/v1, models: moonshot-v1-8k/32k/128k, kimi-latest
- **anthropic**: native SDK
- **openai/deepseek**: OpenAI-compatible

Requirements:
- Retry with exponential backoff (3 retries, 1s/2s/4s)
- Log every call: timestamp, provider, model, tokens, cost, latency
- All providers via single interface

### 4.2 `core/sandbox.py`

```python
class Sandbox:
    def __init__(self, task_dir: str, timeout: int = 120, max_memory_mb: int = 1024):
        """
        task_dir: read-only data access via TASK_DIR env var.
        temp_dir: persistent across steps within one task.
        """

    def execute(self, code: str, step_id: str = None) -> SandboxResult:
        """Execute code. Results persist in temp_dir for later steps."""
```

Key design (from IBM OpenDsStar):
- Completed step outputs persist in `temp/` — never re-execute.
- Each step can read prior step outputs from `temp/step_N_result.pkl`.
- Pre-installs: pandas, numpy, pdfplumber, python-docx, openpyxl, Pillow, sqlite3.

### 4.3 `profiler/`

Deterministic, zero LLM cost. Each reader → `ManifestEntry`.

```python
@dataclass(frozen=True)
class ManifestEntry:
    file_path: str
    file_type: str       # csv | sqlite | json | markdown | pdf | docx | excel | image | parquet
    size_bytes: int
    summary: dict        # Type-specific: columns, row_count, sample_rows, etc.
```

`cross_source.py`: column name overlap + value overlap detection across all structured sources.

### 4.4 `agents/orchestrator.py`

The main loop:

```python
def run_task(task_dir: str, question: str, config: dict) -> TaskResult:
    # 1. Profile
    manifest = profiler.scan(task_dir)

    # 2. Analyze (deep profiling via code execution)
    data_profile = analyzer.analyze(manifest, sandbox)

    # 3. Incremental plan-code-verify loop
    steps_done = []
    for iteration in range(config['max_iterations']):  # default 20
        # Plan next step
        plan_step = planner.plan_next(question, manifest, data_profile, steps_done)

        # Generate code
        code = coder.generate(plan_step, manifest, steps_done)

        # Execute
        result = sandbox.execute(code, step_id=f"step_{iteration}")

        if result.return_code != 0:
            # Debug: fix with data context
            for retry in range(config['max_retries']):  # default 3
                fix = debugger.fix(code, result.stderr, manifest, data_profile)
                result = sandbox.execute(fix, step_id=f"step_{iteration}_retry_{retry}")
                if result.return_code == 0:
                    code = fix
                    break

        steps_done.append(StepRecord(plan_step, code, result))

        # Verify sufficiency
        verdict = verifier.check(question, steps_done)
        if verdict.sufficient:
            break

        # Route: continue or backtrack?
        route = router.decide(question, steps_done, verdict)
        if route.action == 'backtrack':
            steps_done = steps_done[:route.truncate_to]

    # 4. Finalize
    answer = finalizer.format(question, steps_done)
    return synthesizer.to_csv(answer)
```

### 4.5 `synthesizer/`

`normalizer.py` rules:
- Strip whitespace, $, %, commas from numbers
- Standardize decimal precision
- Boolean normalization

`kdd_mode.py`: include multiple numeric representations as extra columns (no penalty for extras in KDD scoring).

### 4.6 `eval/diagnostics.py` — Actionable Diagnostic Output

This is critical for understanding WHERE to improve. The eval output must answer:

**Per-task diagnostics:**
```
task_id: task_11
score: 0 (FAIL)
difficulty: easy
failure_category: code_error
failed_at_agent: coder (step 3)
error_type: KeyError
error_detail: Column 'diagnosis' not found. Available: ['Diagnosis', 'Disease']
tokens_used: 12,450
cost_usd: 0.024
time_seconds: 45
steps_executed: 5 / 20 max
suggestion: "Case-sensitive column matching. Profiler should normalize column names."
```

**Aggregate diagnostics (the dashboard):**
```
=== EVAL REPORT: 50 tasks ===

Overall: 14/50 (28.0%)
  Easy:    8/15 (53.3%)
  Medium:  4/23 (17.4%)
  Hard:    2/10 (20.0%)
  Extreme: 0/2  (0.0%)

=== FAILURE BREAKDOWN ===
  code_error:      12 tasks (33%) ← FIX THIS FIRST
  wrong_direction:  8 tasks (22%)
  format_error:     6 tasks (17%) ← quick win
  partial_result:   5 tasks (14%)
  timeout:          3 tasks (8%)
  hallucination:    2 tasks (6%)

=== BOTTLENECK BY AGENT ===
  Coder failures:    12 tasks (most common failure point)
  Verifier false-positive: 3 tasks (said "sufficient" too early)
  Planner misdirection:   8 tasks (wrong analysis strategy)
  Debugger gave up:       4 tasks (exhausted retries)

=== TOKEN ECONOMICS ===
  Total: 623,000 tokens / $1.24
  Avg per task: 12,460 tokens / $0.025
  Most expensive: task_42 (89,000 tokens, 18 steps, backtracked 3x)

=== TOP 3 ACTIONABLE IMPROVEMENTS ===
  1. [HIGH] Fix column name case sensitivity (affects 8 tasks, est. +16% accuracy)
  2. [HIGH] Improve SQL generation for JOIN queries (affects 6 tasks, est. +12%)
  3. [MED]  Fix number formatting in synthesizer (affects 6 tasks, est. +12%)

=== PER-DIFFICULTY FAILURE PATTERNS ===
  Easy:  mostly code_error (Python bugs) → improve coder prompt
  Medium: mostly wrong_direction (bad SQL) → improve planner with schema context
  Hard:  mostly partial_result (missed doc info) → improve analyzer for .md files
  Extreme: timeout (too many steps) → need summarization strategy
```

This diagnostic output drives the eval-first development loop.

---

## 5. Prompt Specifications

### 5.1 `prompts/analyzer.md`

```markdown
You are a data analyst. For each file described below, write Python code that:
1. Loads the file
2. Prints a concise summary: column names, data types, row count, null counts, value ranges, sample values
3. For text files (.md): print headings, key terms, and any structured data (tables, lists)
4. For databases: print all table schemas, foreign keys, and sample rows

Available data files:
{manifest_json}

The task directory is available at the path in environment variable TASK_DIR.
Print results to stdout in a structured format.
```

### 5.2 `prompts/planner.md`

```markdown
You are planning a data analysis task. Given the question, data manifest, and results from prior steps, plan the NEXT SINGLE step.

## Question
{question}

## Data manifest
{manifest_json}

## Data profile (semantic summaries)
{data_profile}

## Steps completed so far
{steps_done_summary}

## Rules
1. Plan EXACTLY ONE step. Do not plan ahead.
2. Reference specific file names and column names from the manifest.
3. If prior steps produced intermediate results, build on them.
4. State what data sources this step needs and what it should produce.

## Output format
{
  "step_description": "What this step does and why",
  "data_sources": ["file1.csv", "database.sqlite/table"],
  "depends_on_prior": true/false,
  "expected_output": "Description of result"
}
```

### 5.3 `prompts/coder.md`

```markdown
You are a Python code generator for data analysis. Convert the plan step into executable Python code.

## Plan step
{plan_step}

## Data manifest
{manifest_json}

## Prior step results
{prior_results_summary}

## Rules
1. The task data is at the path in environment variable TASK_DIR.
2. Prior step results are saved as pickle files in the temp/ directory (e.g., temp/step_0_result.pkl).
3. Print your result to stdout. This is the ONLY output captured.
4. Save intermediate DataFrames to temp/ for later steps.
5. Use pandas, numpy, sqlite3, json, re. All are available.
6. Do NOT guess column names. Use the manifest.
7. Handle encoding issues (try utf-8 first, then latin-1).
```

### 5.4 `prompts/verifier.md`

```markdown
You are verifying whether accumulated analysis results are sufficient to answer a question.

## Question
{question}

## Steps completed and their results
{steps_summary}

## Output
{
  "sufficient": true/false,
  "reasoning": "Why the results are/aren't enough",
  "missing": "What information is still needed (if insufficient)"
}
```

### 5.5 `prompts/router.md`

```markdown
Review the analysis progress and decide the next action.

## Question
{question}

## Steps and results
{steps_summary}

## Verifier feedback
{verifier_feedback}

## Options
- "continue": plan and execute the next step
- "backtrack": something went wrong at step N, truncate history and re-plan from there
- "finish": results are sufficient, proceed to finalize

## Output
{
  "action": "continue" | "backtrack" | "finish",
  "truncate_to": N (only if backtrack),
  "reasoning": "Why this action"
}
```

### 5.6 `prompts/debugger.md`

```markdown
Fix the following Python code that failed during execution.

## Code that failed
```python
{failed_code}
```

## Error
{error_type}: {error_message}

## Data context
{data_context}
(Column names, data types, sample values from the relevant files)

## Rules
1. Fix the SPECIFIC error. Do not rewrite from scratch.
2. Use the data context to fix column name mismatches, type errors, etc.
3. Return the complete fixed code.
```

### 5.7 `prompts/finalizer.md`

```markdown
You have completed the data analysis. Format the final answer.

## Question
{question}

## Analysis results
{steps_summary}

## Output format
Return a JSON object representing the answer table:
{"columns": {"col_name_1": [val1, val2, ...], "col_name_2": [val1, val2, ...]}}

## Rules
1. Column names should match what the question asks for.
2. Include ALL requested information.
3. Numbers should be clean (no $ or % unless the question implies them).
4. If in doubt, include more columns rather than fewer.
```

---

## 6. Build Order

### Phase 0 — Minimal Working System

```
Step 1: Scaffolding + core/types.py
  - All directories + __init__.py files
  - requirements.txt
  - config.yaml (Kimi/Moonshot as primary)
  - Frozen dataclasses: ManifestEntry, Manifest, SandboxResult, StepRecord, etc.

Step 2: core/llm_client.py
  - Moonshot (Kimi) provider first (OpenAI-compatible)
  - Anthropic provider
  - Cost tracking, retry logic
  - Test: client.chat("What is 1+1?") returns response

Step 3: core/sandbox.py
  - subprocess execution with timeout
  - Persistent temp/ directory
  - Step result saving (pickle)
  - Tests: execute, error capture, timeout

Step 4: profiler/ (all readers)
  - Focus on KDD formats first: CSV, JSON, SQLite, Markdown
  - Then: PDF, DOCX, Excel, Image, Parquet
  - cross_source.py
  - Tests with demo data

Step 5: prompts/ (all agent prompts)

Step 6: agents/ (full incremental loop)
  - analyzer.py, planner.py, coder.py
  - verifier.py, router.py, debugger.py, finalizer.py
  - orchestrator.py (main loop)

Step 7: synthesizer/
  - base.py, normalizer.py, kdd_mode.py

Step 8: main.py CLI
  - Wire everything, test on task_11

Step 9: eval/ with diagnostics
  - scorer.py (KDD column-vector matching)
  - run_eval.py with full diagnostic output
  - failure_analysis.py (categorize failures by agent, error type)
  - diagnostics.py (bottleneck analysis + suggestions)
  - kdd_adapter.py

Step 10: Set up DABstep data + dabstep_eval.py

Step 11: Baseline eval on KDD 50 tasks
  - Record in docs/BENCHMARKS.md
  - Run failure_analysis → drive next decisions
```

---

## 7. Configuration

`config.yaml`:
```yaml
llm:
  provider: moonshot
  model: kimi-latest
  base_url: https://api.moonshot.cn/v1
  api_key_env: MOONSHOT_API_KEY
  max_tokens: 8192
  temperature: 0.0

agent:
  max_iterations: 20
  max_retries: 3
  backtrack_limit: 3

sandbox:
  timeout_seconds: 120
  max_memory_mb: 1024

synthesizer:
  mode: kdd
  normalize_numbers: true

eval:
  kdd_gold_dir: data/demo
  dabstep_dir: data/dabstep
```

---

## 8. Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Incremental planning, not upfront plan | DS-STAR proves 15-20% accuracy gain over one-shot plan |
| 2 | Multi-agent with specialized roles | DS-STAR 7-agent loop is proven; each role has focused prompt |
| 3 | Verifier + Router, not just smoke test | Sufficiency checking >> plausibility checking |
| 4 | Debugger uses data context | DS-STAR shows fix rate improves significantly with schema info |
| 5 | Persistent sandbox state | IBM OpenDsStar: never re-execute completed steps |
| 6 | Kimi (Moonshot) as primary LLM | User requirement; OpenAI-compatible API simplifies integration |
| 7 | No framework dependency | ~3000 lines of Python. No LangChain overhead. |
| 8 | Profiler is biggest investment | Deterministic, testable, zero token cost |
| 9 | Eval diagnostics drive everything | Per-agent bottleneck + actionable suggestions |
| 10 | Dual benchmark eval | KDD (multi-format) + DABstep (financial payments) cover complementary skills |
| 11 | Immutable data types | Frozen dataclasses prevent hidden side effects |
