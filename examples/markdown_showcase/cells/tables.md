## Plain table

| Provider | Model | Pricing tier |
|----------|-------|--------------|
| OpenAI | gpt-5.4 | Premium |
| Anthropic | claude-sonnet-4-6 | Standard |
| Google | gemini-3-flash | Cheap |

## With column alignment

| Eval | Score | Notes |
|:-----|------:|:------:|
| MMLU-redux | 74.2% | macro |
| GSM8K | 88.1% | exact-match |
| HumanEval | 71.0% | pass@1 |
| TruthfulQA | 62.3% | mc1 |

## Table with inline formatting

| Field | Type | Required | Description |
|-------|------|:--------:|-------------|
| `model_id` | `str` | ✓ | Identifier from the **registry** |
| `score` | `float` | ✓ | Numeric in `[0, 1]` |
| `provenance_hash` | `str` | ✓ | SHA-256 of the methodology chain |
| `notes` | `str` | | Optional *human-readable* commentary |
