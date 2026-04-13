# @name Aggregate by Topic
# @worker df-cluster
# Aggregate paper counts per topic using DataFusion SQL.
# Dispatched to the df-cluster worker which has DataFusion installed.
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
table = pa.Table.from_pandas(papers)
ctx.register_record_batches("papers", [table.to_batches()])

category_stats = ctx.sql(
    """
    SELECT
        topic,
        COUNT(*) AS paper_count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM papers
    GROUP BY topic
    ORDER BY paper_count DESC
    """
).to_pandas()

print("Topic distribution (DataFusion SQL):")
print(category_stats.to_string(index=False))
category_stats
