# Lumina

An autonomous agent that answers natural language questions over multi-format data files.

Give it a folder of CSVs, databases, JSON, Excel, PDFs, and Markdown — ask a question in plain English — get a structured answer table.

```bash
python main.py --task ./data/my_dataset --question "What is the average revenue per region?"
```

> **Status:** Work in progress. Core pipeline is functional; APIs may change.

---

## How It Works

Argos uses an incremental plan-code-verify loop rather than a single prompt:

```
Profiler → Analyzer → QuestionAnalyzer → Loop(Planner → Coder → Sandbox → Judge) → Finalizer
```

1. **Profiler** — scans all files, builds a manifest (zero LLM cost)
2. **Analyzer** — runs profiling code per file, extracts domain rules from docs
3. **QuestionAnalyzer** — analyzes the question against data before any execution
4. **Planner** — plans ONE next step at a time (receives Judge feedback)
5. **Coder** — generates Python for that step
6. **Sandbox** — executes with persistent state; Debugger retries on failure
7. **Judge** — checks sufficiency + code logic; routes to finish / continue / backtrack
8. **Finalizer** — formats the answer as a structured table

---

## Supported File Formats

| Format | Support |
|--------|---------|
| CSV | ✅ |
| SQLite | ✅ |
| JSON / JSONL | ✅ |
| Excel (.xlsx) | ✅ |
| Parquet | ✅ |
| Markdown / Text | ✅ |
| PDF | ✅ |
| DOCX | ✅ |
| Images | ✅ (metadata) |

---

## Setup

```bash
git clone https://github.com/ljjsfe/lumina.git
cd lumina
pip install -r requirements.txt

# Set your LLM API key
cp .env.example .env
# edit .env: MOONSHOT_API_KEY=your_key_here
```

**Default LLM:** Moonshot/Kimi (`kimi-k2.5`). Other providers (OpenAI, Anthropic, DeepSeek) work via `config.yaml`.

---

## Usage

```bash
# Single task (task dir with data files + task.json)
python main.py --task ./data/my_task --output ./results/my_task

# With explicit question
python main.py --task ./data/my_task --question "Which products had the highest return rate?"
```

Output: `prediction.csv` (answer table) + `trace.json` (full execution trace)

See `DATA.md` for benchmark dataset download instructions.

---

## Configuration

Key settings in `config.yaml`:

```yaml
llm:
  provider: moonshot
  model: kimi-k2.5
  api_key_env: MOONSHOT_API_KEY

agent:
  max_iterations: 8
  max_retries: 2
```

---

## Architecture Notes

- **No framework** — ~3000 lines of plain Python, no LangChain/LlamaIndex
- **Immutable state** — frozen dataclasses throughout, no in-place mutation
- **File-based workspace** — every step's code and output persisted for observability
- **Domain rules** — documentation files (MD/PDF/DOCX) get their own high-priority channel, not mixed with data profiles

---

## Project Structure

```
dataline/
├── core/        # types, llm_client, sandbox, state, workspace
├── profiler/    # manifest scanner + per-format readers
├── agents/      # orchestrator + 7 agent roles
├── prompts/     # .md prompt templates
└── tests/

main.py          # CLI entry point
config.yaml      # LLM + agent + sandbox settings
DATA.md          # Dataset download instructions
```

---

## License

MIT
