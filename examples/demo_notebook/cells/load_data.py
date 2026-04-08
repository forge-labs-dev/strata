import pandas as pd

df = pd.DataFrame({"id": range(100), "value": [i * 1.5 for i in range(100)]})
