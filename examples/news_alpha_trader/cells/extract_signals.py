# @name signals_batch
# @temperature 0.0
# @max_tokens 4096
# @system You are a quantitative-trading analyst extracting market-impact signals from news headlines. Be precise and conservative — err toward lower confidence on ambiguous headlines.
# @output_schema {"type": "object", "properties": {"signals": {"type": "array", "items": {"type": "object", "properties": {"article_id": {"type": "integer"}, "ticker": {"type": "string"}, "sentiment": {"type": "number", "minimum": -1, "maximum": 1}, "confidence": {"type": "number", "minimum": 0, "maximum": 1}, "theme": {"type": "string", "enum": ["earnings", "guidance", "product", "regulatory", "macro", "m_and_a", "legal", "people", "other"]}, "reasoning": {"type": "string"}}, "required": ["article_id", "ticker", "sentiment", "confidence", "theme", "reasoning"]}}}, "required": ["signals"]}
# @validate_retries 3
#
# Extracts structured sentiment for each article in the unprocessed
# batch. The schema enums are deliberately narrow — a tighter enum
# means cleaner aggregation SQL downstream. The validate-and-retry
# loop kicks in when the provider falls back to json_object mode
# (Anthropic, Gemini, Mistral) and the response doesn't quite match.

For each article below, return a sentiment signal. Rules:

- `sentiment` is in [-1, 1]: -1 = clearly bearish for THIS ticker,
  +1 = clearly bullish, 0 = neutral.
- `confidence` is in [0, 1]. Use < 0.5 for ambiguous / rumor / weak
  headlines. Use > 0.8 only for clear-cut material news.
- `theme` must be one of the enum values — pick the single best fit.
- `reasoning` is one short sentence explaining the signal.
- Copy `article_id` and `ticker` verbatim from the input row.

Do NOT infer signals for tickers other than the one on each row.

Articles:

{{ unprocessed }}
