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
    unsupported_symbols: set[str] = field(default_factory=set)
    blocking_symbols: set[str] = field(default_factory=set)
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

    Supports:
    - optional module docstring
    - top-level imports
    - top-level defs / async defs / classes
    - top-level assignments whose RHS is a literal (``int``, ``float``,
      ``str``, ``bool``, ``None``, ``bytes``, negated/positive literals,
      and nested tuples/lists/sets/dicts of literals)

    Plain-data constants like ``STEP_SIZE = 0.5`` are the most common
    sibling of defs in module cells, so blocking them forces users to
    split cells for no payoff. The module source is reconstructed
    verbatim and re-executed at deserialize time, so any expression we
    accept here must be side-effect-free and context-independent — that
    means no function calls, no attribute access, no names.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return ModuleExportPlan(
            module_source=source,
            unsupported_reasons=[f"invalid syntax: {exc.msg}"],
        )

    exported_symbols: dict[str, ExportedSymbol] = {}
    unsupported_symbols: set[str] = set()
    blocking_symbols: set[str] = set()
    unsupported_reasons: list[str] = []

    for index, node in enumerate(tree.body):
        if _is_module_docstring(node, index):
            continue

        if isinstance(node, ast.ImportFrom) and any(alias.name == "*" for alias in node.names):
            unsupported_reasons.append("star imports are not supported for cross-cell code export")
            continue

        if _is_literal_constant_assignment(node):
            for name in _target_names_for_assignment(node):
                exported_symbols[name] = ExportedSymbol(name, "constant")
            continue

        unsupported_reason = _unsupported_reason_for_node(node)
        if unsupported_reason is not None:
            unsupported_symbols.update(_defined_names_for_node(node))
            blocking_symbols.update(_blocking_names_for_node(node))
            unsupported_reasons.append(unsupported_reason)
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
        unsupported_symbols=unsupported_symbols,
        blocking_symbols=blocking_symbols,
        unsupported_reasons=unsupported_reasons,
    )


def _is_literal_constant_assignment(node: ast.AST) -> bool:
    """True when the node is a top-level assignment of a literal value.

    Handles ``x = 5`` / ``X: int = 5`` / ``X, Y = 1, 2``. Annotated
    assignments without a value (``x: int``) don't bind a name, so we
    ignore them. Augmented assignments (``x += 1``) read an existing
    name and are explicitly not a module-cell concern.
    """
    if isinstance(node, ast.Assign):
        if not all(isinstance(t, (ast.Name, ast.Tuple, ast.List)) for t in node.targets):
            return False
        return _is_literal_value(node.value)
    if isinstance(node, ast.AnnAssign):
        if node.value is None or not isinstance(node.target, ast.Name):
            return False
        return _is_literal_value(node.value)
    return False


def _is_literal_value(node: ast.expr) -> bool:
    """Return True for compile-time-constant expressions.

    Allows: constants (numbers, strings, bools, None, bytes), unary
    ``+/-/~/not`` of a literal, and nested tuples/lists/sets/dicts of
    literals. Deliberately excludes names, attribute access, function
    calls, binary ops, and comprehensions — anything that could reach
    into module state or have side effects on import.
    """
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.UnaryOp) and isinstance(
        node.op, (ast.USub, ast.UAdd, ast.Invert, ast.Not)
    ):
        return _is_literal_value(node.operand)
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return all(_is_literal_value(elt) for elt in node.elts)
    if isinstance(node, ast.Dict):
        return all(
            key is not None and _is_literal_value(key) and _is_literal_value(value)
            for key, value in zip(node.keys, node.values, strict=True)
        )
    return False


def _target_names_for_assignment(node: ast.AST) -> list[str]:
    """Collect names bound by a literal-constant assignment."""
    if isinstance(node, ast.Assign):
        names: list[str] = []
        for target in node.targets:
            for name in sorted(_target_names(target)):
                names.append(name)
        return names
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return [node.target.id]
    return []


def _is_module_docstring(node: ast.stmt, index: int) -> bool:
    """Return whether *node* is the module docstring expression."""
    if index != 0 or not isinstance(node, ast.Expr):
        return False
    value = node.value
    return isinstance(value, ast.Constant) and isinstance(value.value, str)


def _unsupported_reason_for_node(node: ast.stmt) -> str | None:
    """Return a specific user-facing reason for unsupported top-level statements."""
    if isinstance(node, ast.Assign):
        if isinstance(node.value, ast.Lambda):
            return "top-level lambdas are not shareable across cells"
        return "top-level runtime state (assignments like `x = ...`) is not shareable across cells"

    if isinstance(node, ast.AnnAssign):
        if isinstance(node.value, ast.Lambda):
            return "top-level lambdas are not shareable across cells"
        return "top-level runtime state (annotated assignments) is not shareable across cells"

    if isinstance(node, ast.AugAssign):
        return "top-level runtime state (augmented assignments) is not shareable across cells"

    if isinstance(node, ast.Expr):
        if isinstance(node.value, ast.Lambda):
            return "top-level lambdas are not shareable across cells"
        return "top-level runtime expressions are not shareable across cells"

    if isinstance(
        node,
        (
            ast.For,
            ast.AsyncFor,
            ast.While,
            ast.If,
            ast.With,
            ast.AsyncWith,
            ast.Try,
            ast.Match,
        ),
    ):
        return "top-level control flow is not shareable across cells"

    return None


def _defined_names_for_node(node: ast.AST) -> set[str]:
    """Return names defined by an unsupported top-level statement."""
    defined: set[str] = set()

    for candidate in ast.walk(node):
        if isinstance(candidate, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(candidate.name)
        elif isinstance(candidate, ast.Assign):
            for target in candidate.targets:
                defined.update(_target_names(target))
        elif isinstance(candidate, ast.AnnAssign):
            defined.update(_target_names(candidate.target))
        elif isinstance(candidate, ast.AugAssign):
            defined.update(_target_names(candidate.target))

    return defined


def _blocking_names_for_node(node: ast.AST) -> set[str]:
    """Return downstream names that should fail source-backed code export.

    Ordinary serializable values like ``x = 1`` should continue through the
    normal artifact path even though they are not module-exportable. This
    helper narrows blocking to code-like symbols that users likely expect to
    be reusable definitions, such as lambda assignments and defs/classes
    nested inside unsupported control flow.
    """
    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Lambda):
        names: set[str] = set()
        for target in node.targets:
            names.update(_target_names(target))
        return names

    if isinstance(node, ast.AnnAssign) and isinstance(node.value, ast.Lambda):
        return _target_names(node.target)

    blocking: set[str] = set()
    for candidate in ast.walk(node):
        if isinstance(candidate, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            blocking.add(candidate.name)
    return blocking


def _target_names(target: ast.expr) -> set[str]:
    """Extract assigned names from an assignment target."""
    if isinstance(target, ast.Name):
        return {target.id}
    if isinstance(target, (ast.Tuple, ast.List)):
        names: set[str] = set()
        for item in target.elts:
            names.update(_target_names(item))
        return names
    return set()
