# @name Downstream consumer (would call is_outlier)
#
# Without this reference, the producing cell would just be a private
# helper and the planner would let it slide. Naming ``is_outlier``
# here is what tells Strata "this is meant to be shared," and is
# what makes the export-blocked diagnostic fire on the cell above.

would_be_outliers = is_outlier(7)
