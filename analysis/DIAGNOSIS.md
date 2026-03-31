# Data Analytics Agent 性能诊断报告

## 执行情况

| 指标 | 值 |
|------|-----|
| KDD dev set | 1/10 完成 (task_11) |
| DABstep dev set | 0/10 完成 |
| 总完成率 | 1/20 (5%) |

---

## Task_11 (Easy) 详细分析

### 基本信息
- **难度**: Easy ⭐
- **题目**: 找出"severe degree of thrombosis"的患者，列出 ID、sex、disease
- **执行结果**: ❌ **失败（虽然代码跑通了）**

### 执行轨迹

```
1. Profiler     → 找到 4 个数据文件 + 1 个关系
2. Analyzer     → 生成 3189 字符的数据 profile
3. Iteration 0:
   - Planner    → "filter Thrombosis=2, join with Patient, get ID/sex/disease"
   - Coder      → 生成 1230 字符代码
   - Sandbox #1 → ❌ KeyError (列名不存在)
   - Debugger   → 修复并重试
   - Sandbox #2 → ✓ 成功
   - Verifier   → "sufficient=True" (过早判断！)
   - Router     → "finish"
   - Finalizer  → 格式化答案
4. 耗时: 215s (3.5 分钟)
```

### 为什么失败？

**Gold 答案** (正确):
```csv
ID,SEX,Diagnosis
163109,F,SLE
2803470,F,SLE
4395720,F,SLE
```

**Prediction** (错误):
```csv
ID,sex,disease
163109.0,F,       ← ID 对但缺 disease
1430760.0,,SLE    ← 完全错的患者
2803470.0,F,SLE+Psy  ← ID 对但 disease 加了额外信息
... (总共 18 行，其中只有 3 个 ID 正确)
```

**根本问题分析**：

1. **Agent 理解错了数据**
   - Planner 假设 "Thrombosis=2 表示 severe"，但这可能错了
   - 没有验证假设 → 查出错的患者集合

2. **Analyzer 未充分揭露数据**
   - 应该展示: "Examination 表中 Thrombosis 列的值有: 0,1,2,3 分别代表什么?"
   - 应该展示: "这些值在数据中的分布和实际含义"
   - 目前只给了 schema，没有给 domain knowledge

3. **Verifier 太乐观**
   - 第一次迭代就说 "sufficient=True"
   - 完全没有验证输出的正确性
   - 应该检查: "返回的患者数是否合理? ID 范围是否在数据中?"

4. **Planner 没有探索性查询**
   - 应该先跑一个查询看看 Thrombosis=2 有多少患者
   - 对比 gold 答案的 3 个患者，18 个显然太多了

---

## 核心问题总结

### P0 - 架构级问题

| 问题 | 影响 | 根因 |
|------|------|------|
| **Analyzer 仅输出 schema，不输出 semantics** | Agent 猜数据含义 | Analyzer prompt 设计不足 |
| **Verifier 不检查答案合理性** | 错误答案直接通过 | Verifier logic 只看"是否有输出"，不看"输出对不对" |
| **Planner 一步到位，无探索** | 错误方向直接执行 | Planner 应该支持"探索式"规划 |
| **无中间结果验证** | 错误积累到最后 | 缺少 "spot check" 或 "sample validation" 步骤 |

### P1 - 代码质量问题

1. **Debugger 过度依赖** — task_11 还需要 debug 一次，说明 Coder 生成的代码脆弱
2. **Finalizer 列名不匹配** — gold: `SEX/Diagnosis`, pred: `sex/disease` (大小写问题)
3. **NaN 值在输出中** — normalizer 没有处理缺失值

### P2 - 数据问题

1. **Profiler 扫进了 task.json** — task 元数据不应该被当作分析数据源
2. **Cross-source relations 没用上** — manifest 中发现的列名关系未被利用

---

## 改进方向（按优先级）

### 立即修复 (2-3小时)

1. **排除 task.json 从扫描** (`profiler/manifest.py:46-64`)
   ```python
   # 只扫描 context/ 子目录，不要扫描 task.json
   for root, dirs, files in os.walk(os.path.join(task_dir, 'context')):
   ```

2. **Analyzer 增强** (`dataline/prompts/analyzer.md`)
   - 添加规则: 对数值列，展示 min/max/distinct values
   - 对枚举列，展示所有可能的值和出现频率
   - 示例: `"Thrombosis 列: values=[0,1,2,3], counts=[100,50,30,5], likely_meaning=severity_scale"`

3. **Verifier 增强** (`dataline/agents/verifier.py`)
   - 不仅问"是否有答案"，还要问"答案的量级是否合理?"
   - 对于 easy 题目，如果返回 18 行但应该只有 3 行，标记为 insufficient

4. **Planner 改进** — 添加"探索模式"
   ```python
   # Step 0: Exploratory query
   "First, explore: SELECT COUNT(*) FROM Examination WHERE Thrombosis = 2"
   # Step 1: Once confirmed, do the full query
   ```

### 中期改进 (1-2 周)

5. **统一 JSON 解析** — 提取到 `core/parsing.py`，避免 4 个地方重复的 regex
6. **Scorer 修复** — 处理多列重复匹配问题
7. **写测试** — profiler, scorer, sandbox, synthesizer 的单元测试 (80%+ coverage)

### 长期改进 (2-4 周)

8. **实现自验证循环** — 每个步骤后抽样验证输出
9. **支持多策略规划** — 让 Planner 生成多个可能的方案，Verifier 选择最可能的
10. **性能优化** — 缓存 analyzer 结果，减少重复分析

---

## 诊断数据位置

- **详细 trace 日志**: `analysis/logs/dev_kdd_002_backup/task_11/trace.json`
- **Prediction 输出**: `analysis/logs/dev_kdd_002_backup/task_11/prediction.csv`
- **Gold 答案**: `data/demo/output/task_11/gold.csv`

可以对比这三个文件理解失败模式。

---

## 下一步建议

1. **先修复 P0 问题** (Analyzer + Verifier + Planner 探索模式)
   - 预期效果: task_11 的准确率提升到 80%+

2. **修复 profiler 的 task.json 问题**
   - 预期效果: 避免数据污染，整体准确率 +5-10%

3. **重新跑 dev set 对比**
   - 如果改进有效，应该看到 easy 任务的准确率明显上升

4. **分析失败的 hard/extreme 任务模式**
   - 收集 10 个任务的失败原因，优先改进最常见的失败类型

---

## 关键观察

> **"Agent 能够执行代码、调试代码、格式化答案，但根本上不理解数据和题意。"**

这不是工程问题（代码写得很好），而是**信息问题**：
- Analyzer 没有给 Planner 足够的信息
- Verifier 没有有效的验证机制
- 整个流程缺少"暂停和检查"的机制

改进方向应该聚焦于**信息流和反馈环**，而不是加更多的代码。
