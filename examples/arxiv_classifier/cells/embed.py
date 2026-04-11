# @worker gpu-fly
# Generate sentence-transformer embeddings for each paper's title.
# This is the expensive step — in the real workload this is sentence-transformers
# on a GPU, taking seconds instead of the minutes it would take on CPU.
# Day 1 placeholder: produce a numpy array of fake embeddings.
import time

import numpy as np

time.sleep(0.8)  # pretend to load a model and embed
np.random.seed(42)
embeddings = np.random.randn(len(sampled_papers), 16).astype("float32")
print(f"Generated embeddings: {embeddings.shape}")
embeddings
