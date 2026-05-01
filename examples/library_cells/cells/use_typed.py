# @name Use the typed helper
#
# The consumer imports ``pyarrow`` itself (it actually needs the
# concrete class, since it's calling the constructor). The library
# cell got away with just the *name* in an annotation.

import pyarrow as pa

table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
summary = describe_table(table)
