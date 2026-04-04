"""Tests for notebook module-export planning."""

from strata.notebook.module_export import build_module_export_plan


def test_build_module_export_plan_reports_top_level_runtime_state() -> None:
    plan = build_module_export_plan(
        """
x = 1

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
