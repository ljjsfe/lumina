You are extracting structured business rules from domain documentation. These rules will guide data analysis agents to compute correct answers.

## Documentation Content
{domain_rules_text}

## Instructions

Extract every rule, formula, definition, and convention from the documentation above. For each rule, provide:

1. **Rule Name**: A short identifier (e.g., "thrombosis_severity_scale", "fee_calculation_formula")
2. **Exact Quote**: The verbatim text from the document — do NOT paraphrase
3. **Context**: When and how this rule applies

## Rules for Extraction
- Extract ALL rules, not just the obvious ones
- Preserve exact numbers, thresholds, and formulas verbatim — never round or simplify
- Include field/column value mappings (e.g., "0=negative, 1=positive, 2=severe")
- Include null/empty value handling conventions
- Include unit definitions and conversion factors
- Include any business logic or decision criteria
- If the document references specific data fields or columns, note them; if not, omit — do NOT guess column names

## Output Format

For each rule, use this exact format:

### Rule: [rule_name]
- **Quote**: "exact verbatim text from document"
- **Context**: explanation of when this applies

---
