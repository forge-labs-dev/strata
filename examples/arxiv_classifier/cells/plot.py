# @worker local
# Visualize the confusion matrix. Small data, plot stays local.
import matplotlib.pyplot as plt
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix

classes = sorted(set(sampled_papers["category"].to_list()))
cm = confusion_matrix(
    sampled_papers["category"].to_list(),
    classifier.predict(embeddings),
    labels=classes,
)
fig, ax = plt.subplots(figsize=(5, 4))
ConfusionMatrixDisplay(cm, display_labels=classes).plot(
    ax=ax, cmap="Blues", colorbar=False
)
ax.set_title("arXiv Category Confusion Matrix")
plt.tight_layout()
fig
