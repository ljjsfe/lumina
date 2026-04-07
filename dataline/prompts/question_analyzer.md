You are analyzing a data analysis question BEFORE any code is executed. The question has already been decomposed into sub-questions with isolated constraints. Your job is to operationalize that decomposition into a concrete execution strategy.

## Question
{question}

## Sub-Question Decomposition (pre-committed — do NOT re-derive)
{decomposition}

## Domain Rules (from documentation)
{domain_rules}

## Data Sources
{manifest_summary}

## Data Profile (column statistics, value distributions)
{data_profile}

---

## Your Analysis

### 1. Domain Rule Application (CRITICAL — prevents wrong direction)

For EACH domain rule relevant to this question, write a precise mapping:
- **Rule**: Quote the exact rule from the documentation
- **Applies to**: Which sub-question(s) and which column(s)
- **Concrete meaning**: Translate to exact code terms:
  - Coded values: "column `Thrombosis`: 1=mild, 2=moderate, 3=severe → filter `df[df['Thrombosis'] == 3]`"
  - Formulas: "prevalence = affected_count / total_count × 100 (manual.md §5)"
  - Field semantics: "NULL in `Diagnosis` means 'not yet diagnosed', not 'healthy'"

If NO domain rules exist, state: "No documentation — all filter values must be discovered from the data profile."

### 2. Data Source Selection

For each sub-question:
- State EXACTLY which file and which columns provide the answer
- Justify WHY — cite column names and value distributions from the data profile
- Call out sources you are EXCLUDING and why

**NEVER guess column names. Use only columns that appear in the data profile or manifest.**

### 3. Step-by-Step Strategy

Describe the execution plan (2–5 steps):
- For each step: what to load, what to filter/join/aggregate, what to print
- Specify exact filter values from data profile or domain rules
- Mark which step produces the answer for each sub-question
- Apply ONLY the constraints listed per sub-question in the decomposition above

### 4. Self-Verification Checklist

Before finalizing, verify:
- [ ] Every column name I reference exists in the data profile or manifest
- [ ] Every filter value I propose appears in the value distribution of that column
- [ ] My strategy answers ALL sub-questions in the decomposition
- [ ] Each sub-question uses ONLY its own constraints (no bleed-over)
- [ ] If using a ratio/percentage: both numerator and denominator sources identified
- [ ] Domain rules section is complete — no relevant rule was missed

### 5. Red Flags to Watch For

List the top 2–3 failure modes most likely for THIS specific question:
- e.g., "Column 'gender' has values 'M'/'F' not 'male'/'female'"
- e.g., "Question says 'average' but data only has counts — need to compute from raw"

