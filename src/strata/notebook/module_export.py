"""Source-backed export of reusable top-level notebook code.

This module validates whether a cell can be treated as a synthetic Python
module for cross-cell reuse of top-level ``def``/``class`` definitions.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field


@dataclass(frozen=True)
class ExportedSymbol:
    """One exportable top-level symbol."""

    name: str
    kind: str


@dataclass(frozen=True)
class ModuleExportPlan:
    """Validated module-export plan for a cell source string."""

    module_source: str
    exported_symbols: dict[str, ExportedSymbol] = field(default_factory=dict)
    unsupported_reasons: list[str] = field(default_factory=list)

    @property
    def is_exportable(self) -> bool:
        return not self.unsupported_reasons

    def format_error(self) -> str:
        """Return a user-facing reason string for unsupported module export."""
        if not self.unsupported_reasons:
            return ""
        return "; ".join(self.unsupported_reasons)


def build_module_export_plan(source: str) -> ModuleExportPlan:
    """Validate source for synthetic module export.

    V1 supports only:
    - optional module docstring
    - top-level imports
    - top-level defs / async defs / classes
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return ModuleExportPlan(
            module_source=source,
            unsupported_reasons=[f"invalid syntax: {exc.msg}"],
        )

    exported_symbols: dict[str, ExportedSymbol] = {}
    unsupported_reasons: list[str] = []

    for index, node in enumerate(tree.body):
        if _is_module_docstring(node, index):
            continue

        if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
            unsupported_reasons.append("star imports are not supported for cross-cell code export")
            continue

        if not isinstance(
            node,
            (
                ast.Import,
                ast.ImportFrom,
                ast.FunctionDef,
                ast.AsyncFunctionDef,
                ast.ClassDef,
            ),
        ):
            unsupported_reasons.append(
                "only top-level imports, defs, async defs, and classes can be shared across cells"
            )
            continue

        if isinstance(node, ast.FunctionDef):
            exported_symbols[node.name] = ExportedSymbol(node.name, "function")
        elif isinstance(node, ast.AsyncFunctionDef):
            exported_symbols[node.name] = ExportedSymbol(node.name, "async function")
        elif isinstance(node, ast.ClassDef):
            exported_symbols[node.name] = ExportedSymbol(node.name, "class")

    module_source = source if source.endswith("\n") else f"{source}\n"
    return ModuleExportPlan(
        module_source=module_source,
        exported_symbols=exported_symbols,
        unsupported_reasons=unsupported_reasons,
    )


def _is_module_docstring(node: ast.stmt, index: int) -> bool:
    """Return whether *node* is the module docstring expression."""
    if index != 0 or not isinstance(node, ast.Expr):
        return False
    value = node.value
    return isinstance(value, ast.Constant) and isinstance(value.value, str)
