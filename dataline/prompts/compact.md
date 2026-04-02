You are summarizing the progress of a data analysis task. Your summary will replace the detailed step outputs in the agent's context window to save space while preserving all critical information.

## Question Being Analyzed
{question}

## Steps to Summarize
{steps_text}

## Instructions

Produce a structured summary that preserves:
1. **Key Findings** — exact numbers, computed values, specific data points discovered. Copy numbers verbatim with full precision.
2. **Data Insights** — patterns, distributions, data quality issues, column value ranges discovered during exploration.
3. **Errors and Lessons** — what failed, why it failed, what approach worked instead. These prevent repeating mistakes.
4. **Current State** — what has been accomplished so far, what variables/intermediate results are available, what remains to be done.

## Rules
- Preserve ALL exact numbers — never round or approximate
- Preserve ALL column names, file names, and table names exactly as they appear
- Preserve any domain-specific formulas or definitions that were discovered
- If a step produced a WARNING or 0-row result, note it explicitly
- Keep the summary concise but complete — aim for 30-50% of the original size
- Use markdown formatting with clear section headers

## Output Format

### Key Findings
- [finding with exact numbers]

### Data Insights
- [patterns and distributions discovered]

### Errors and Lessons
- [what failed and what worked]

### Current State
- [what is accomplished, what intermediate results exist]
