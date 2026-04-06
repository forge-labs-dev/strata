# Create a sample sales dataset
import pandas as pd
import numpy as np

np.random.seed(42)
n = 200

sales = pd.DataFrame({
    "date": pd.date_range("2025-01-01", periods=n, freq="D"),
    "region": np.random.choice(["North", "South", "East", "West"], n),
    "product": np.random.choice(["Widget", "Gadget", "Doohickey"], n),
    "units": np.random.randint(1, 50, n),
    "price": np.round(np.random.uniform(5, 100, n), 2),
})

print(f"Created {len(sales)} rows")
sales.head(10)
