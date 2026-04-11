# @worker local
# Evaluation stays local — small data, cheap to compute.
from sklearn.metrics import accuracy_score, classification_report

predictions = classifier.predict(embeddings)
y_true = sampled_papers["category"].to_list()
accuracy = accuracy_score(y_true, predictions)
report = classification_report(y_true, predictions, output_dict=True, zero_division=0)
print(f"Accuracy: {accuracy:.3f}")
print(f"Classes: {sorted(set(y_true))}")
{"accuracy": accuracy, "per_class": report}
