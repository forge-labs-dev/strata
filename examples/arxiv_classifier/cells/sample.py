# @worker df-cluster
# Stratified sample of papers, balanced across categories.
# Stays on the DataFusion worker to avoid moving the full table back to the
# client. Uses ROW_NUMBER() window functions partitioned by category — the
# classic SQL pattern for "take N rows per group" without a pandas round-trip.
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
ctx.register_record_batches("papers", [[pa.RecordBatch.from_pandas(papers)]])

PER_CATEGORY = 2

sampled_papers = ctx.sql(
    f"""
    WITH ranked AS (
        SELECT
            id,
            category,
            year,
            title,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY id) AS rn
        FROM papers
    )
    SELECT id, category, year, title
    FROM ranked
    WHERE rn <= {PER_CATEGORY}
    ORDER BY category, id
    """
).to_pandas()

print(
    f"Sampled {len(sampled_papers)} papers across "
    f"{sampled_papers['category'].nunique()} categories via DataFusion SQL"
)
sampled_papers
