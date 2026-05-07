# @name min_amount
"""Threshold for the orders SQL query.

Edit this number and the downstream SQL cell will re-execute on the
next run. ``top_orders`` folds ``min_amount`` into its provenance
hash, so 50 → 100 produces a different artifact; running again with
the same value cache-hits.
"""

min_amount = 50
min_amount
