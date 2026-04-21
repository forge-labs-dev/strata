"""Tests for notebook module-export planning."""

from strata.notebook.module_export import build_module_export_plan


def test_build_module_export_plan_reports_top_level_non_literal_assignment() -> None:
    """Non-literal assignments (function calls, attribute access, etc.)
    still taint the cell — only compile-time literals are exportable.
    """
    plan = build_module_export_plan(
        """
x = compute()

def add(y):
    return x + y
""".strip()
    )

    assert plan.is_exportable is False
    assert "top-level runtime state" in plan.format_error()
    assert "x" in plan.unsupported_symbols
    assert "x" not in plan.blocking_symbols
    # `add` is a proper function definition — exportable, not unsupported
    assert "add" not in plan.unsupported_symbols
    assert "add" in plan.exported_symbols


def test_build_module_export_plan_reports_top_level_lambda() -> None:
    plan = build_module_export_plan("add = lambda y: y + 1")

    assert plan.is_exportable is False
    assert "top-level lambdas are not shareable across cells" in plan.format_error()
    assert "add" in plan.unsupported_symbols
    assert "add" in plan.blocking_symbols


def test_build_module_export_plan_reports_top_level_control_flow() -> None:
    plan = build_module_export_plan(
        """
if True:
    x = 1

def add(y):
    return y + 1
""".strip()
    )

    assert plan.is_exportable is False
    assert "top-level control flow is not shareable across cells" in plan.format_error()
    assert "x" in plan.unsupported_symbols
    # `add` is a proper function definition — exportable, not unsupported
    assert "add" not in plan.unsupported_symbols
    assert "add" in plan.exported_symbols


# ---------------------------------------------------------------------------
# Literal-constant assignments are exportable alongside defs/classes
# ---------------------------------------------------------------------------


def test_literal_int_assignment_is_exportable() -> None:
    plan = build_module_export_plan("STEP_SIZE = 5")
    assert plan.is_exportable is True
    assert plan.exported_symbols["STEP_SIZE"].kind == "constant"


def test_literal_float_string_bool_none_are_exportable() -> None:
    plan = build_module_export_plan("RATE = 0.5\nLABEL = 'prod'\nDEBUG = True\nFALLBACK = None")
    assert plan.is_exportable is True
    assert set(plan.exported_symbols) == {"RATE", "LABEL", "DEBUG", "FALLBACK"}


def test_negative_literal_is_exportable() -> None:
    plan = build_module_export_plan("OFFSET = -1\nBITS = ~0")
    assert plan.is_exportable is True
    assert set(plan.exported_symbols) == {"OFFSET", "BITS"}


def test_literal_container_is_exportable() -> None:
    plan = build_module_export_plan(
        """
CLASSES = ["cat", "dog", "fish"]
CONFIG = {"lr": 1e-3, "batch": 32, "shuffle": True}
DIMS = (128, 128)
""".strip()
    )
    assert plan.is_exportable is True
    assert set(plan.exported_symbols) == {"CLASSES", "CONFIG", "DIMS"}


def test_literal_constants_coexist_with_defs_and_classes() -> None:
    plan = build_module_export_plan(
        """
import math

RATE = 0.5
CLASSES = ("a", "b")

def scale(x):
    return x * RATE

class Config:
    debug = True
""".strip()
    )
    assert plan.is_exportable is True
    assert plan.exported_symbols["RATE"].kind == "constant"
    assert plan.exported_symbols["CLASSES"].kind == "constant"
    assert plan.exported_symbols["scale"].kind == "function"
    assert plan.exported_symbols["Config"].kind == "class"


def test_annotated_literal_assignment_is_exportable() -> None:
    plan = build_module_export_plan("STEP_SIZE: float = 0.5")
    assert plan.is_exportable is True
    assert plan.exported_symbols["STEP_SIZE"].kind == "constant"


def test_annotated_without_value_is_not_exportable_but_does_not_crash() -> None:
    plan = build_module_export_plan("x: int")
    # A bare annotation doesn't bind a value; we classify it the same
    # as other unsupported runtime statements rather than silently
    # swallowing it.
    assert plan.is_exportable is False


def test_non_literal_rhs_still_blocks() -> None:
    """The restriction is "compile-time constants only" — references to
    other names, attribute access, and function calls fall through.
    """
    plan = build_module_export_plan("STEP = some_fn()")
    assert plan.is_exportable is False

    plan = build_module_export_plan("PI = math.pi")
    assert plan.is_exportable is False

    plan = build_module_export_plan("DOUBLE = x * 2")
    assert plan.is_exportable is False


def test_nested_non_literal_in_container_blocks() -> None:
    """A list containing a name reference isn't a literal."""
    plan = build_module_export_plan("ITEMS = [1, 2, x]")
    assert plan.is_exportable is False


def test_augmented_assignment_still_blocks() -> None:
    """``x += 1`` reads x from module state; that's runtime behavior."""
    plan = build_module_export_plan("x = 1\nx += 1")
    assert plan.is_exportable is False


def test_tuple_unpacking_of_literals_is_exportable() -> None:
    plan = build_module_export_plan("MIN, MAX = 0, 100")
    assert plan.is_exportable is True
    assert set(plan.exported_symbols) == {"MIN", "MAX"}
