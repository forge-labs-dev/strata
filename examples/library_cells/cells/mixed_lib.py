# @name Mixed: setup + helpers in one cell
#
# Slicing keeps:
#   - the import (math)
#   - the literal constant (CLAMP_MIN, CLAMP_MAX)
#   - the def (clamp)
#
# and drops the runtime statements (raw_min, raw_max, the print). The
# runtime variables are still visible to downstream cells via the
# regular artifact path; the def + constants ride the synthetic module.

import math

# --- Runtime setup, dropped from the synthetic module ----------------
# (Pre-slicing, these three lines would have blocked the whole cell.)
raw_min = round(-math.tau * 7, 2)  # function call → not a literal
raw_max = round(math.tau * 16, 2)
print(f"loaded raw bounds: [{raw_min}, {raw_max}]")

# --- Library code, kept in the slice ---------------------------------
CLAMP_MIN = 0.0
CLAMP_MAX = 100.0


def clamp(value: float) -> float:
    """Pin *value* into ``[CLAMP_MIN, CLAMP_MAX]``."""
    return max(CLAMP_MIN, min(CLAMP_MAX, value))
