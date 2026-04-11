# @name research_themes
# @worker local
# Prompt cell: use an LLM to identify research themes from the aggregated stats.
# The {{ category_stats }} template variable is injected from the upstream cell.
Given these arXiv paper counts per category and year:

{{ category_stats }}

Identify 3 research themes that cut across the categories above. For each theme,
return a short name (2-4 words) and a one-sentence description. Return them as a
numbered list.
