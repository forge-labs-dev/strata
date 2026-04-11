# @worker df-cluster
# DataFusion aggregation: count papers per (category, year).
# Day 1 placeholder: a plain pandas groupby in lieu of a real DataFusion query.
# The @worker df-cluster annotation sends this cell to a DataFusion-capable
# worker at execution time.
import time

import pandas as pd

time.sleep(0.5)  # pretend to fan out to a cluster
category_stats = (
    papers.groupby(["category", "year"], as_index=False)
    .size()
    .rename(columns={"size": "paper_count"})
)
print(f"Aggregated into {len(category_stats)} (category, year) buckets")
category_stats
