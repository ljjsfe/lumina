# dataline — Project Context for Claude

## What This Is

`dataline` is a command-line data analytics agent. Given a folder of mixed-format data files and a natural language question, it reasons across those files and returns a structured answer table.

```bash
python main.py --task ./data/demo/input/task_11 --output ./results/task_11
```

Output: `prediction.csv` + `trace.json`

**Dual goals:**
1. Open-source framework for data analysis agents
2. Competition: KDD Cup 2026 + DABstep (proving grounds)

---

## Architecture: Incremental Plan-Code-Verify Loop

NOT one-shot ReAct. NOT upfront full plan. Inspired by DS-STAR.

```
Input (task dir)
    │
Profiler (deterministic, zero LLM cost) → Manifest
    │
Analyzer (deep profiling via code execution) → DataProfile
    │
Loop (max 13 iterations):
    Planner → plan ONE next step (receives Judge guidance)
    Coder   → step → Python code
    Sandbox → execute (persistent state across steps)
        └─ error → Debugger → retry (max 3)
    Judge   → sufficiency + routing + guidance (single LLM call)
        ├─ finish    → Finalizer → Output
        ├─ continue  → loop (guidance passed to next Planner call)
        └─ backtrack → truncate to step N, re-plan
    │
Synthesizer → prediction.csv + trace.json
```

---

## Agent Roles

| Agent | File | Role |
|-------|------|------|
| Analyzer | `dataline/agents/analyzer.py` | Generates + executes profiling scripts per file |
| Planner | `dataline/agents/planner.py` | Plans ONE next step (receives Judge guidance) |
| Coder | `dataline/agents/coder.py` | Converts plan step → executable Python |
| Judge | `dataline/agents/judge.py` | Sufficiency + routing + guidance (single call, replaces Verifier+Router) |
| Debugger | `dataline/agents/debugger.py` | Fixes code using traceback + data context |
| Finalizer | `dataline/agents/finalizer.py` | Formats results → prediction.csv |
| Orchestrator | `dataline/agents/orchestrator.py` | Main loop wiring all agents |

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Incremental planning | DS-STAR proves 15-20% accuracy gain over one-shot |
| Persistent sandbox state | Never re-execute completed steps (IBM OpenDsStar) |
| Debugger uses data context | Fix rate improves significantly with schema info |
| Profiler is zero LLM cost | Deterministic, testable, saves tokens |
| No framework (no LangChain) | ~3000 lines of Python, no overhead |
| Immutable data types | Frozen dataclasses only, no mutation |
| Kimi (Moonshot) primary LLM | OpenAI-compatible API |

---

## LLM Configuration

- **Primary**: Moonshot/Kimi — `kimi-latest`, `https://api.moonshot.cn/v1`
- **API key env**: `MOONSHOT_API_KEY`
- **Fallback providers**: anthropic, openai, deepseek (all via same `LLMClient` interface)
- **Config file**: `config.yaml`

---

## Development Workflow: Eval-First

The improvement loop is:
```
run eval → read diagnostics → identify bottleneck agent → fix → re-run eval
```

**Never** optimize without running eval first. The diagnostic output (`eval/diagnostics.py`) tells you exactly which agent is failing and why.

Key eval commands:
```bash
# Run single task
python main.py --task ./data/demo/input/task_11 --output ./results/task_11

# Run full KDD eval (50 tasks)
python eval/run_eval.py --data data/demo --output results/eval_$(date +%Y%m%d)

# Compare two runs
python eval/compare.py results/eval_A results/eval_B
```

---

## File Structure

```
dataline/
├── core/          # types.py, llm_client.py, sandbox.py
├── profiler/      # manifest.py + readers (csv, sqlite, json, md, pdf, docx, excel, image, parquet)
├── agents/        # orchestrator + 7 agent roles
├── synthesizer/   # base.py, normalizer.py, kdd_mode.py
├── prompts/       # .md prompt templates per agent
├── eval/          # scorer, run_eval, diagnostics, failure_analysis
└── tests/

data/
├── demo/          # KDD Cup Phase 1, 50 tasks (gold answers in demo/output/)
└── dabstep/       # DABstep benchmark (Adyen payments)

config.yaml        # LLM + agent + sandbox + eval config
main.py            # CLI entry point
```

---

## Evaluation Benchmarks

| Benchmark | Tasks | Key Challenge |
|-----------|-------|---------------|
| KDD Cup 2026 | 50 demo + Phase 2 | Multi-format, cross-source joins |
| DABstep | 10 dev + full test | Financial payments, scalar answers |

Scoring: `Score = Recall − λ × (Extra Columns / Predicted Columns)`. Extra columns ARE penalized. Column names ignored; values matched by content (sorted), case-sensitive, ROUND_HALF_UP 2dp.

---

## Coding Standards for This Project

- **Immutable only**: frozen dataclasses, never mutate in-place
- **Small files**: 200-400 lines typical, 800 max
- **No silent errors**: every agent logs its reasoning to trace.json
- **Test coverage**: `dataline/tests/` — run with `pytest dataline/tests/`
- **Python 3.11+**
