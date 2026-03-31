# 改进路线图

## 问题排序

### 🔴 P0 - 立即修复 (2-3 小时)

#### P0.1: Profiler 扫进 task.json
**现象**: task 的问题描述被当作数据源分析
**修复位置**: `dataline/profiler/manifest.py:46`
```python
# 改前
for root, _dirs, files in os.walk(task_dir):
    for fname in sorted(files):
        fpath = os.path.join(root, fname)
        # ... 扫描 task.json

# 改后
context_dir = os.path.join(task_dir, 'context')
if not os.path.exists(context_dir):
    context_dir = task_dir  # fallback
for root, _dirs, files in os.walk(context_dir):
    for fname in sorted(files):
        if fname == 'task.json':  # skip metadata
            continue
```

#### P0.2: Analyzer 只输出 schema，不输出语义
**现象**: Agent 不知道 "Thrombosis=2" 是什么意思
**修复位置**: `dataline/prompts/analyzer.md`

当前 prompt:
```
- Structured data: column names, dtypes, row count, null counts, value ranges, 3 sample values per column
```

改进为:
```
- Structured data:
  * Column names, dtypes, row count, null counts
  * **For numeric columns**: min, max, mean, stddev, and sorted list of distinct values
  * **For categorical columns**: all distinct values with frequency counts
  * **For ID columns**: show sample IDs and ID range
  * Sample rows showing real data context

Example output format:
```

#### P0.3: Verifier 不验证答案正确性
**现象**: Verifier 说 "sufficient=True"，但答案里有 18 行而应该只有 3 行
**修复位置**: `dataline/agents/verifier.py` + `dataline/prompts/verifier.md`

改前:
```python
def check(...):
    # 只问: "有答案吗?"
    return VerifierVerdict(sufficient=has_output, ...)
```

改后:
```python
def check(question, steps_done, llm):
    # Step 1: 基本充分性
    prompt = planner.prompts/verifier_basic.md
    basic_verdict = llm.chat(prompt, ...)

    if not basic_verdict.sufficient:
        return basic_verdict

    # Step 2: 答案合理性检查 (新增)
    if "number of" in question.lower() or "list" in question.lower():
        # 从步骤输出推断预期的行数
        estimated_rows = _estimate_expected_rows(question, steps_done)
        actual_rows = _count_rows(steps_done[-1].result.stdout)

        if actual_rows > estimated_rows * 3:  # 多出 3 倍红旗
            return VerifierVerdict(
                sufficient=False,
                reasoning="Too many results for this question",
                missing=f"Expected ~{estimated_rows} rows, got {actual_rows}"
            )

    return basic_verdict
```

改进的 prompt (`dataline/prompts/verifier_enhanced.md`):
```markdown
## Additional Checks (NEW)

Check if the answer size is reasonable:
- If question asks "list X" and result has 100 rows, that's suspicious
- If question asks "count" and result has a table, that's wrong
- If question asks "who has feature Y" and only 1-2 results, verify against data

Answer these:
1. basic sufficiency: true/false
2. size_reasonable: "correct" | "too_many" | "too_few"
3. confidence: "high" | "medium" | "low"
```

---

### 🟡 P1 - 高优先级 (1 周内)

#### P1.1: Planner 支持探索模式
**现象**: Planner 直接跳到"获取最终答案"，没有先验证假设
**修复**: 新增 2 种规划模式

```python
# dataline/agents/planner.py

def plan_next(..., steps_done, llm):
    # 新增: 检查是否需要探索
    if len(steps_done) == 0:
        # 这是第一步 — 可能需要探索
        return _plan_exploratory(question, manifest_json, data_profile, llm)
    else:
        return _plan_analytical(question, manifest_json, data_profile, steps_done, llm)

def _plan_exploratory(question, manifest_json, data_profile, llm):
    """对于第一步，生成探索性查询"""
    prompt = """
    The user asked: {question}

    Before jumping to the answer, we need to explore the data.
    Generate ONE exploratory query that:
    - Discovers key columns (e.g., if question mentions "severe", find which column/value represents it)
    - Shows data distribution (e.g., "how many rows have Thrombosis=2?")
    - Validates assumptions

    Output format: {...}
    """
    # ... call LLM with exploratory prompt

def _plan_analytical(question, manifest_json, data_profile, steps_done, llm):
    """后续步骤，基于探索结果继续"""
    # 当前的 plan_next 逻辑
    pass
```

#### P1.2: 统一 JSON/代码解析
**现象**: 4 个地方重复的 `_extract_json` / `_extract_code` regex，容易出 bug
**修复**: 新增 `dataline/core/parsing.py`

```python
# dataline/core/parsing.py

import re
import json

def extract_json(response: str) -> dict:
    """Robust JSON extraction from LLM response."""
    # Try: markdown code block
    match = re.search(r'```(?:json)?\s*\n(.*?)\n```', response, re.DOTALL)
    if match:
        text = match.group(1).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    # Try: raw JSON (find first { and last })
    text = response.strip()
    start = text.find('{')
    if start >= 0:
        end = text.rfind('}') + 1
        if end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                pass

    raise ValueError(f"No valid JSON found in response")

def extract_code(response: str) -> str:
    """Robust Python code extraction."""
    # Try: ```python ... ```
    match = re.search(r'```python\s*\n(.*?)\n```', response, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try: ``` ... ```
    match = re.search(r'```\s*\n(.*?)\n```', response, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Return as-is (might be raw code)
    return response.strip()
```

然后在 planner/coder/verifier/router/finalizer 中都改成:
```python
from ..core.parsing import extract_json, extract_code

# 改前
plan_data = json.loads(_extract_json(response))

# 改后
plan_data = extract_json(response)
```

#### P1.3: Debugger 更聪明
**现象**: 代码第一次错就靠 debugger 修，说明 coder 不够聪明
**修复**: Coder 的 prompt 增强

```markdown
# dataline/prompts/coder_enhanced.md

## Common Pitfalls to Avoid

1. **Column name case sensitivity**:
   - CSV files might have "SEX" but you load with "sex"
   - Always check manifest for exact column names

2. **Data type mismatches**:
   - If manifest says column is "string", don't do numeric operations
   - Use pd.to_numeric(..., errors='coerce') for ambiguous columns

3. **File paths**:
   - Always use TASK_DIR environment variable
   - Never hardcode paths like "/home/user/..."

4. **Null handling**:
   - Check manifest for "null_count" — don't assume no nulls
   - Use df.dropna() or df.fillna() explicitly

5. **Joins**:
   - Verify the join column name exists in BOTH tables
   - Check for case sensitivity
   - Show the join result count to verify it worked
```

---

### 🟢 P2 - 中期改进 (2-4 周)

#### P2.1: 写测试

```bash
# pytest dataline/tests/
# 目标: 80%+ coverage

tests/
├── test_profiler.py          # Test all 9 file readers
├── test_sandbox.py           # Test code execution + timeout + cleanup
├── test_scorer.py            # Test column matching logic
├── test_synthesizer.py       # Test DataFrame → CSV conversion
├── test_llm_client.py        # Test all 5 providers + retry logic
└── integration/
    ├── test_simple_task.py   # End-to-end: easy task_11
    └── test_complex_task.py   # End-to-end: hard task
```

#### P2.2: 缓存 Analyzer 结果

```python
# dataline/agents/analyzer.py

def analyze(manifest, llm, sandbox, cache_dir=None):
    """Generate and execute profiling code, with optional caching."""

    # Hash manifest to detect changes
    manifest_hash = hash(str(manifest))
    cache_file = f"{cache_dir}/analyzer_{manifest_hash}.pkl" if cache_dir else None

    if cache_file and os.path.exists(cache_file):
        with open(cache_file, 'rb') as f:
            return pickle.load(f)

    # ... existing analyzer logic ...

    if cache_file:
        with open(cache_file, 'wb') as f:
            pickle.dump(data_profile, f)

    return data_profile
```

---

## 改进的优先顺序和预期效果

| 优先级 | 改进项 | 预期效果 | 工作量 |
|--------|--------|---------|--------|
| P0.1 | 排除 task.json | +5% accuracy (减少数据污染) | 30 min |
| P0.2 | Analyzer 增强 | +15% accuracy (Agent 理解数据更好) | 1 hour |
| P0.3 | Verifier 增强 | +10% accuracy (错误答案被过滤) | 1.5 hours |
| P1.1 | Planner 探索 | +10% accuracy (验证假设) | 2 hours |
| P1.2 | 统一解析 | +2% accuracy (减少解析 bug) | 1 hour |
| P1.3 | Coder 增强 | +5% accuracy (更少 debug retries) | 1 hour |
| P2.1 | 写测试 | +0% accuracy (but +confidence) | 3 hours |
| P2.2 | 缓存 | -30% wall time (性能) | 1 hour |

**预测**: 按顺序完成 P0 + P1，整体准确率从 5% → **50-60%**（easy 从 0% → 80%+）

---

## 验证计划

每个改进后：
```bash
# 清理旧结果
rm -rf results/dev_kdd_* results/dev_dabstep_*

# 重新跑 dev set
MOONSHOT_API_KEY=... python main.py batch --benchmark kdd --sample dev --output results/dev_test_v1
MOONSHOT_API_KEY=... python main.py batch --benchmark dabstep --sample dev --output results/dev_dabstep_v1

# 评分并对比
python main.py eval --benchmark kdd --results results/dev_test_v1 --gold data/demo
python main.py eval --benchmark dabstep --results results/dev_dabstep_v1 --gold data/dabstep
```

对比改进前后的指标：
- Overall accuracy
- Per-difficulty accuracy breakdown
- Failure category distribution
- Average tokens/cost per task
- Debug retry counts

---

## 成功标准

| Milestone | Target | Timeline |
|-----------|--------|----------|
| P0 修复完 | Easy: ≥50% | This week |
| P1 完成 | Easy: ≥80%, Medium: ≥30% | 1-2 weeks |
| Full coverage | Easy: ≥90%, Medium: ≥60%, Hard: ≥20% | 3-4 weeks |

