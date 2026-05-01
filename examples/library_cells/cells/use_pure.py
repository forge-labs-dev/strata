# @name Use the pure-cell helpers
#
# ``area`` and ``perimeter`` ride the synthetic-module path; running
# this cell re-executes the slice in a fresh module and pulls out
# the requested symbols.

circle = {
    "radius": 7.5,
    "area": area(7.5),
    "perimeter": perimeter(7.5),
    "precision_used": CIRCLE_PRECISION,
}
