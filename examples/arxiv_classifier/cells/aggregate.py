# @worker df-cluster
# DataFusion aggregation: count papers per (category, year).
# Dispatched to the `df-cluster` worker, which has DataFusion's Python
# bindings installed. The same code would run on a real Ballista cluster
# against Iceberg or Parquet tables; for the demo we register the
# upstream pandas frame as an in-memory DataFusion table.
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_record_batches("papers", [[pa.RecordBatch.from_pandas(papers)]])

category_stats = ctx.sql(
    """
    SELECT category, year, COUNT(*) AS paper_count
    FROM papers
    GROUP BY category, year
    ORDER BY category, year
    """
).to_pandas()

print(f"Aggregated into {len(category_stats)} (category, year) buckets via DataFusion SQL")
category_stats
