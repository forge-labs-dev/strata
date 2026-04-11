# @worker df-cluster
# Stratified sample of papers, balanced across categories.
# Stays on the DataFusion worker to avoid moving the full table back to the client.
# Day 1 placeholder: in-memory sample of the tiny loaded frame.
import time

import pandas as pd

time.sleep(0.3)
sampled_papers = (
    papers.groupby("category", group_keys=False)
    .apply(lambda df: df.sample(n=min(len(df), 2), random_state=0))
    .reset_index(drop=True)
)
print(f"Sampled {len(sampled_papers)} papers across {sampled_papers['category'].nunique()} categories")
sampled_papers
