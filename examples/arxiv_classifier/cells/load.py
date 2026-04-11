# @worker local
# Load arXiv metadata from a hosted Parquet file.
# Day 1 placeholder: simulate the load and assign a tiny DataFrame.
import time

import pandas as pd

time.sleep(0.3)  # pretend to download
papers = pd.DataFrame(
    {
        "id": [f"arxiv:{i:04d}" for i in range(8)],
        "category": ["cs.LG", "cs.CL", "cs.CV", "cs.LG", "cs.CL", "cs.CV", "cs.LG", "cs.CL"],
        "year": [2023, 2024, 2024, 2025, 2025, 2023, 2024, 2025],
        "title": [f"Paper {i}" for i in range(8)],
    }
)
print(f"Loaded {len(papers)} arXiv papers (placeholder)")
papers
