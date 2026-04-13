# @name Stratified Sample
# @worker df-cluster
# Stratified sample: take up to 500 papers per topic so the embedding
# step runs in seconds, not minutes. Uses DataFusion window functions.
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()
table = pa.Table.from_pandas(papers)
ctx.register_record_batches("papers", [table.to_batches()])

PER_TOPIC = 500

sampled_papers = ctx.sql(
    f"""
    WITH ranked AS (
        SELECT
            title,
            abstract,
            topic,
            ROW_NUMBER() OVER (PARTITION BY topic ORDER BY title) AS rn
        FROM papers
    )
    SELECT title, abstract, topic
    FROM ranked
    WHERE rn <= {PER_TOPIC}
    ORDER BY topic, title
    """
).to_pandas()

print(
    f"Sampled {len(sampled_papers):,} papers across "
    f"{sampled_papers['topic'].nunique()} topics (DataFusion SQL)"
)
print(sampled_papers["topic"].value_counts().to_string())
sampled_papers
