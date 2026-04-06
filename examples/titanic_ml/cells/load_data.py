# Load Titanic dataset (bundled with seaborn)
import seaborn as sns
import pandas as pd

df = sns.load_dataset("titanic")

print(f"Loaded {len(df)} passengers")
print(f"Survival rate: {df['survived'].mean():.1%}")
print(f"\nMissing values:\n{df.isnull().sum()[df.isnull().sum() > 0]}")
df.head()
