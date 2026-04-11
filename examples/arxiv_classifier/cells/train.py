# @worker gpu-fly
# Train a classifier: embeddings → category label.
# Day 1 placeholder: a scikit-learn logistic regression against the fake embeddings.
# In the real workload this is replaced with a small MLP on the GPU.
import time

from sklearn.linear_model import LogisticRegression

time.sleep(0.4)
X = embeddings
y = sampled_papers["category"].to_list()
classifier = LogisticRegression(max_iter=500, multi_class="auto")
classifier.fit(X, y)
print(f"Trained logistic regression on {len(X)} samples")
classifier
