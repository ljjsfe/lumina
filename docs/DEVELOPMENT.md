# dataline Development Plan

> A general-purpose data analysis agent framework, validated on KDD Cup 2026.
> Open-source project with competition as proving ground.

---

## 1. Project Vision

**dataline** is a command-line tool and library. You give it a folder of mixed-format data files and a natural language question. It reasons across those files and returns a structured answer table.

**Dual goals:**
- **Open-source framework**: Clean, extensible architecture for building data analysis agents. Designed for the community to adopt, extend, and contribute.
- **KDD Cup 2026 validation**: The competition serves as a rigorous benchmark to prove the framework works on real, complex multi-source analysis tasks.

**Target users:** Data analysts and data scientists who are tired of spending 60-70% of their time on glue work: reading schemas, extracting numbers from PDFs, matching entities across CSVs and databases.

---

## 2. Architecture

### System Flow (Phase 0)

```
Input (task dir)
    |
    v
Profiler (deterministic, zero LLM cost)
    | produces Manifest (file metadata + cross-source relations)
    v
ReAct Agent (LLM + Sandbox loop)
    | think -> code -> execute -> observe -> repeat
    | Haiku smoke test after each step
    v
Synthesizer
    | step results -> normalized answer table
    v
Output: prediction.csv + trace.json
```

### Component Overview

```
dataline/
├── main.py                      # CLI entry point
├── config.yaml                  # Model, retries, timeouts, mode
│
├── core/
│   ├── types.py                 # Immutable dataclasses (frozen=True)
│   ├── llm_client.py            # Unified LLM adapter (Anthropic/OpenAI/Google/DeepSeek)
│   ├── sandbox.py               # Python code execution (subprocess + timeout)
│   └── agent.py                 # ReAct loop (Phase 0) + plan-execute mode (Phase 1+)
│
├── profiler/                    # Deterministic data profiling, zero LLM cost
│   ├── manifest.py              # Scan task dir -> Manifest
│   ├── csv_reader.py            # CSV -> ManifestEntry
│   ├── json_reader.py           # JSON -> ManifestEntry (incl. KDD {table,records} format)
│   ├── sqlite_reader.py         # SQLite -> ManifestEntry (tables, FKs, samples)
│   ├── pdf_reader.py            # PDF -> ManifestEntry (pdfplumber)
│   ├── docx_reader.py           # DOCX -> ManifestEntry (python-docx)
│   ├── excel_reader.py          # Excel -> ManifestEntry (openpyxl)
│   ├── image_reader.py          # Image -> ManifestEntry (Pillow metadata)
│   ├── markdown_reader.py       # Markdown (.md) -> ManifestEntry (headings, key terms)
│   ├── parquet_reader.py        # Parquet -> ManifestEntry (pyarrow)
│   └── cross_source.py          # Auto-discover entity relations across sources
│
├── synthesizer/
│   ├── base.py                  # Agent output -> pd.DataFrame -> prediction.csv
│   ├── normalizer.py            # Value normalization ($/%/commas/precision)
│   └── kdd_mode.py              # KDD-specific: extra columns, format variants
│
├── validator/
│   └── haiku_check.py           # Cheap per-step smoke test (~$0.001/call)
│
├── prompts/
│   ├── react_system.md          # ReAct loop system prompt
│   ├── synthesize.md            # Answer assembly prompt
│   └── error_context.md         # Structured error formatting template
│
├── eval/
│   ├── scorer.py                # KDD Cup column-vector matching
│   ├── run_eval.py              # Batch eval -> EvalReport
│   ├── failure_analysis.py      # Failure categorization
│   └── compare.py               # Two-run comparison (accuracy delta)
│
├── data/demo/                   # KDD Cup Phase 1 demo dataset (50 tasks)
│   ├── input/task_*/            # Task data + task.json
│   └── output/task_*/           # gold.csv answers
│
├── docs/
│   ├── DEVELOPMENT.md           # This file
│   ├── DECISIONS.md             # Design decision log
│   └── BENCHMARKS.md            # Accuracy tracking over time
│
└── tests/
    ├── test_profiler.py
    ├── test_scorer.py
    ├── test_sandbox.py
    └── test_normalizer.py
```

---

## 3. KDD Cup Demo Dataset Analysis

**Source:** https://dataagent.top/ (Phase 1 Demo Dataset)

### Summary

| Metric | Value |
|--------|-------|
| Total tasks | 50 |
| Easy | 15 |
| Medium | 23 |
| Hard | 10 |
| Extreme | 2 |
| Gold answers | 50/50 (all tasks have gold.csv) |
| Total size | 1.8 GB |

### Data Format Per Task

```
task_<id>/
├── task.json              # {task_id, difficulty, question}
└── context/
    ├── csv/               # 37 tasks have CSV files
    ├── db/                # 27 tasks have SQLite databases
    ├── json/              # 30 tasks have JSON files
    ├── doc/               # 12 tasks have Markdown documents (.md format)
    └── knowledge.md       # All 50 tasks have this domain knowledge doc
```

### Context Patterns by Difficulty

| Difficulty | Typical Context Combo | Core Challenge |
|------------|----------------------|----------------|
| Easy (15) | csv + json + knowledge.md | Code generation for data analysis |
| Medium (23) | csv + db + json + knowledge.md | Text-to-SQL, multi-source joins |
| Hard (10) | csv + doc + knowledge.md | Reasoning over unstructured docs |
| Extreme (2) | doc + knowledge.md (large) | Ultra-long context (>128K tokens) |

### Gold Answer Characteristics

- Most answers are small: 1-3 columns, 1-20 rows
- Single-value answers common (e.g., COUNT=1)
- Multi-row tables also appear (up to ~17 rows)
- Column names come from the data (SQL column names, field names)

### Key Observations for Our Solution

1. **No PDF/DOCX/Excel/Image in Phase 1 demo** — but design spec mandates support and Phase 2 adds these modalities. We implement all readers for open-source completeness.
2. **doc/ contains .md files, not .docx** — need a `markdown_reader.py` (not just docx_reader)
3. **knowledge.md is universal** — every task has it, contains entity definitions, metric formulas, SQL examples. Critical context for the agent.
4. **JSON uses KDD wrapper format** — `{table: "name", records: [...]}` not plain arrays
5. **Easy tasks have no SQLite** — just CSV + JSON, pure Python analysis
6. **Hard/Extreme rely on doc/ reasoning** — unstructured text comprehension is key differentiator

---

## 4. Supported Input Formats

| Format | Reader | Status | Used in Demo |
|--------|--------|--------|-------------|
| CSV | csv_reader.py | Phase 0 | Yes (37 tasks) |
| SQLite | sqlite_reader.py | Phase 0 | Yes (27 tasks) |
| JSON | json_reader.py | Phase 0 | Yes (30 tasks) |
| Markdown (.md) | markdown_reader.py | Phase 0 | Yes (12 doc/ + 50 knowledge.md) |
| PDF | pdf_reader.py | Phase 0 | No (Phase 2 likely) |
| DOCX | docx_reader.py | Phase 0 | No (Phase 2 likely) |
| Excel (.xlsx/.xls) | excel_reader.py | Phase 0 | No (Phase 2 likely) |
| Image (PNG/JPG) | image_reader.py | Phase 0 | No (Phase 2 likely) |
| Parquet | parquet_reader.py | Phase 0 | No |

All readers are implemented in Phase 0 for open-source completeness, even if only CSV/SQLite/JSON/Markdown are exercised by the demo dataset.

---

## 5. Evaluation

### KDD Cup Scoring Rule

Binary column-matching accuracy:
- Each gold column is treated as an unordered value vector
- Column names are ignored — only values matter
- Score = 1 if ALL gold columns are matched in prediction (with float tolerance 0.001)
- Score = 0 if any gold column is missing or mismatched
- Extra prediction columns are OK (no penalty)

### Evaluation Framework (eval/)

| Component | Purpose |
|-----------|---------|
| `scorer.py` | Single-task scoring: prediction vs gold column-vector matching |
| `run_eval.py` | Batch eval over all tasks -> EvalReport (overall + per-difficulty accuracy) |
| `failure_analysis.py` | Categorize failures: profiler_miss, wrong_direction, code_error, partial_result, format_error, hallucination, timeout |
| `compare.py` | Compare two runs: accuracy delta, improved/regressed task lists |

### Eval-Driven Development Rules

1. Every new component must prove its value via eval before/after comparison
2. Track accuracy over time in `docs/BENCHMARKS.md`
3. Run failure_analysis before adding features — data drives decisions
4. Per-difficulty breakdown is mandatory (a change helping Hard but regressing Easy must be understood)
5. Every regression must be explained before committing

### Benchmark Tracking Format

```
| Date | Change | Easy | Medium | Hard | Extreme | Overall |
|------|--------|------|--------|------|---------|---------|
| YYYY-MM-DD | Phase 0 baseline | ?% | ?% | ?% | ?% | ?% |
```

---

## 6. Build Order (Phase 0)

### Step 1: Project scaffolding + core/types.py
- Create all directories
- requirements.txt with all dependencies
- config.yaml with sensible defaults
- Frozen dataclasses: ManifestEntry, Manifest, SandboxResult, HaikuVerdict, StepTrace, EvalReport

### Step 2: core/llm_client.py
- Unified LLM adapter (Anthropic first, then OpenAI)
- Retry with exponential backoff on rate limits
- Cost tracking per call (tokens, USD)
- Haiku client shortcut for cheap smoke tests

### Step 3: core/sandbox.py
- subprocess.run with timeout and memory limit
- TASK_DIR env var for generated code to access data
- Shared temp/ directory across steps within one task
- Clean stdout/stderr capture

### Step 4: profiler/ (all readers)
- manifest.py: scan context/ subdirectories recursively
- One reader per format (CSV, JSON, SQLite, PDF, DOCX, Excel, Image, Markdown, Parquet)
- cross_source.py: auto-discover entity relations (column name overlap, value overlap)
- Unit tests per reader using demo data

### Step 5: prompts/
- react_system.md: ReAct system prompt adapted for context/ subdirectory layout
- error_context.md: structured error formatting template
- synthesize.md: answer assembly prompt

### Step 6: core/agent.py (ReAct mode)
- ReAct loop: LLM -> extract code -> sandbox execute -> observe -> repeat
- FINAL ANSWER detection and extraction
- Step logging to trace.json
- Max steps (15) and retry limit (3) per step

### Step 7: validator/haiku_check.py
- Cheap smoke test after each code execution
- Checks: empty/null, plausible range, relevance to question

### Step 8: synthesizer/
- base.py: FINAL ANSWER JSON -> DataFrame -> prediction.csv
- normalizer.py: strip $/%/commas, decimal precision
- kdd_mode.py: multiple numeric representation columns

### Step 9: main.py CLI
- --task, --question (optional, fallback to task.json), --config, --output-dir
- Wire all components together
- End-to-end test

### Step 10: eval/
- scorer.py with KDD column-vector matching
- run_eval.py batch evaluation
- failure_analysis.py failure categorization
- compare.py two-run comparison

### Step 11: Baseline eval
- Run on all 50 demo tasks
- Record baseline in docs/BENCHMARKS.md
- Failure analysis to drive Phase 1 decisions

---

## 7. Phase Roadmap

### Phase 0: Minimal Backbone (current)
- ReAct agent with all format readers
- Eval framework
- Baseline accuracy on 50-task demo set

### Phase 1: Planning for Complex Tasks
- Difficulty router (easy->ReAct, complex->Plan)
- Planner + critic (same-model second pass)
- A/B eval: ReAct vs Plan per difficulty level

### Phase 2: Full System
- Knowledge store + retriever
- Deep validator (independent LLM evaluator)
- Cross-task error pattern memory

---

## 8. Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| 1 | ReAct-first, not Plan-first | Simpler, proven effective. Plan tested in Phase 1. |
| 2 | Code execution, not tool calling | Intermediate data stays in sandbox. 50 tokens returned, not 5000. |
| 3 | No framework dependency | ~2000 lines of Python. No LangChain/LangGraph overhead. |
| 4 | Profiler is biggest investment | Everything downstream depends on manifest quality. Deterministic, testable, zero token cost. |
| 5 | cross_source.py is differentiator | Auto-discovering entity relations across heterogeneous sources. |
| 6 | Haiku smoke test always on | $0.001 per check catches obvious garbage early. |
| 7 | Eval drives everything | No component stays unless eval proves its value. |
| 8 | Open-source first | All readers implemented even if not needed for competition. Clean API, good docs. |
| 9 | Immutable data types | Frozen dataclasses prevent hidden side effects. |
| 10 | KDD data format as primary | Profiler designed around context/ subdirectory layout. |
