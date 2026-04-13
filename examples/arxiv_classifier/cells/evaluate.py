# @worker local
# Evaluation on the held-out test set. Small data, runs locally.
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

X = embeddings
y = sampled_papers["topic"].to_numpy()
_, X_test, _, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

predictions = classifier.predict(X_test)
accuracy = accuracy_score(y_test, predictions)
report = classification_report(y_test, predictions, output_dict=True, zero_division=0)

print(f"Test accuracy: {accuracy:.3f}")
print()
print(classification_report(y_test, predictions, zero_division=0))

eval_results = {"accuracy": round(accuracy, 4), "per_class": report}
eval_results
