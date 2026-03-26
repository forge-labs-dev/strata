"""AST-based variable analysis for notebook cells."""

from __future__ import annotations

import ast
import builtins
from dataclasses import dataclass, field


@dataclass
class CellAnalysis:
    """Result of analyzing a single cell.

    Attributes:
        defines: List of top-level variable names defined by this cell
        references: List of free variable names referenced but not defined in this cell
        error: Error message if analysis failed (None if successful)
    """

    defines: list[str] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    error: str | None = None


class VariableAnalyzer(ast.NodeVisitor):
    """AST visitor that collects defined and referenced variables."""

    def __init__(self):
        """Initialize the analyzer."""
        self.defines: set[str] = set()
        self.references: set[str] = set()
        self._in_nested_scope = False
        self._local_vars: set[str] = set()  # Track local scope variables

    def visit_Module(self, node: ast.Module) -> None:
        """Visit module — process top-level statements."""
        for child in node.body:
            self.visit(child)

    def visit_Assign(self, node: ast.Assign) -> None:
        """Handle: x = ... or x, y = ..."""
        # Collect targets (defines)
        for target in node.targets:
            self._add_assign_target(target)
        # Visit the value (may have references) — but only if not assigning to pure _
        # If all targets are _, skip visiting the value
        all_underscore = all(self._is_pure_underscore(target) for target in node.targets)
        if not all_underscore:
            self.visit(node.value)

    def _is_pure_underscore(self, target: ast.expr) -> bool:
        """Check if a target is a pure _ (not part of unpacking)."""
        if isinstance(target, ast.Name):
            return target.id == "_"
        return False

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        """Handle: x += ... or df["col"] += ..."""
        # Augmented assignment defines the target
        self._add_assign_target(node.target)
        # Visit the value
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        """Handle: x: int = ... or x: int (without value)."""
        self._add_assign_target(node.target)
        if node.value:
            self.visit(node.value)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Handle: def f(): ... — function name is defined, body is nested scope."""
        self.defines.add(node.name)
        # Don't recurse into function body (it's a nested scope)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Handle: async def f(): ..."""
        self.defines.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Handle: class C: ... — class name is defined, body is nested scope."""
        self.defines.add(node.name)
        # Don't recurse into class body

    def visit_Import(self, node: ast.Import) -> None:
        """Handle: import foo or import foo as bar."""
        for alias in node.names:
            # import foo → defines 'foo'
            # import foo as bar → defines 'bar'
            name = alias.asname if alias.asname else alias.name
            self.defines.add(name)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Handle: from foo import bar or from foo import *."""
        for alias in node.names:
            if alias.name == "*":
                # Star imports — we can't determine what's defined
                # Log would go here, but we just skip for v1
                pass
            else:
                # from foo import bar → defines 'bar'
                # from foo import bar as baz → defines 'baz'
                name = alias.asname if alias.asname else alias.name
                self.defines.add(name)

    def visit_For(self, node: ast.For) -> None:
        """Handle: for x in ... — x is defined at top level."""
        self._add_assign_target(node.target)
        # Visit iterable (may have references)
        self.visit(node.iter)
        # Visit body (nested scope, but loop variable is top-level)
        for stmt in node.body:
            self.visit(stmt)
        for stmt in node.orelse:
            self.visit(stmt)

    def visit_With(self, node: ast.With) -> None:
        """Handle: with ... as x: — x is defined at top level."""
        for item in node.items:
            if item.optional_vars:
                self._add_assign_target(item.optional_vars)
            self.visit(item.context_expr)
        # Visit body (nested scope)
        for stmt in node.body:
            self.visit(stmt)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        """Handle: async with ... as x:"""
        for item in node.items:
            if item.optional_vars:
                self._add_assign_target(item.optional_vars)
            self.visit(item.context_expr)
        for stmt in node.body:
            self.visit(stmt)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        """Handle: except E as e: — e is defined at top level."""
        if node.name:
            self.defines.add(node.name)
        if node.type:
            self.visit(node.type)
        for stmt in node.body:
            self.visit(stmt)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        """Handle: walrus operator := at top level."""
        # if (x := value) — x is defined
        if isinstance(node.target, ast.Name):
            self.defines.add(node.target.id)
        self.visit(node.value)

    def visit_Delete(self, node: ast.Delete) -> None:
        """Handle: del x — x is referenced but not defined."""
        for target in node.targets:
            self._add_delete_target(target)

    def _add_delete_target(self, target: ast.expr) -> None:
        """Extract variable names from a del statement target.

        del x → x is referenced
        del x.attr → x is referenced
        del x[key] → x is referenced
        """
        if isinstance(target, ast.Name):
            self.references.add(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for elt in target.elts:
                self._add_delete_target(elt)
        elif isinstance(target, ast.Subscript):
            self._add_delete_target(target.value)
            self.visit(target.slice)
        elif isinstance(target, ast.Attribute):
            self._add_delete_target(target.value)

    def visit_Name(self, node: ast.Name) -> None:
        """Collect referenced names (Load context)."""
        if isinstance(node.ctx, ast.Load):
            # Don't add names that are local to current scope (e.g., lambda parameters)
            if node.id not in self._local_vars:
                self.references.add(node.id)

    def visit_ListComp(self, node: ast.ListComp) -> None:
        """List comprehension — comprehension variables are scoped.

        In [x * factor for x in items]:
        - 'items' is referenced from outer scope (first generator iterable)
        - 'x' is a loop variable scoped to the comprehension
        - 'factor' is referenced from outer scope (in element)
        - But we can't easily distinguish x from factor without building the scope

        For simplicity, we treat comprehensions as opaque and only visit the
        first generator's iterable (which is guaranteed to be evaluated in outer scope).
        References in the element and conditions are skipped to avoid false positives.

        This means [x * y for x in items] will miss y as a reference, which is
        acceptable for the v1 analyzer — notebooks should avoid such patterns.
        """
        if node.generators:
            # First generator's iterable is evaluated in outer scope
            self.visit(node.generators[0].iter)

    def visit_SetComp(self, node: ast.SetComp) -> None:
        """Set comprehension."""
        if node.generators:
            self.visit(node.generators[0].iter)

    def visit_DictComp(self, node: ast.DictComp) -> None:
        """Dict comprehension."""
        if node.generators:
            self.visit(node.generators[0].iter)

    def visit_GeneratorExp(self, node: ast.GeneratorExp) -> None:
        """Generator expression."""
        if node.generators:
            self.visit(node.generators[0].iter)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        """Lambda expression — arguments are local scope."""
        # Save current local vars
        old_local_vars = self._local_vars
        # Add lambda parameters to local scope
        self._local_vars = self._local_vars | self._get_lambda_params(node.args)
        # Visit body with parameters in local scope
        self.visit(node.body)
        # Restore local vars
        self._local_vars = old_local_vars

    def _get_lambda_params(self, args: ast.arguments) -> set[str]:
        """Extract parameter names from lambda arguments."""
        params = set()
        for arg in args.posonlyargs:
            params.add(arg.arg)
        for arg in args.args:
            params.add(arg.arg)
        if args.vararg:
            params.add(args.vararg.arg)
        for arg in args.kwonlyargs:
            params.add(arg.arg)
        if args.kwarg:
            params.add(args.kwarg.arg)
        return params

    def _add_assign_target(self, target: ast.expr) -> None:
        """Extract variable names from an assignment target.

        Handles:
        - Name: x
        - Tuple/List: (x, y) or [x, y]
        - Subscript: df["col"] → defines the root name df
        - Attribute: obj.attr → defines the root name obj
        """
        if isinstance(target, ast.Name):
            # Simple assignment: x = ...
            self.defines.add(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            # Tuple/list unpacking: (a, b) = ... or [a, b] = ...
            for elt in target.elts:
                self._add_assign_target(elt)
        elif isinstance(target, ast.Subscript):
            # Subscript assignment: df["col"] = ... → defines df
            self._add_assign_target(target.value)
        elif isinstance(target, ast.Attribute):
            # Attribute assignment: obj.attr = ... → defines obj
            self._add_assign_target(target.value)
        elif isinstance(target, ast.Starred):
            # Starred assignment: *rest = ...
            self._add_assign_target(target.value)


def analyze_cell(source: str) -> CellAnalysis:
    """Analyze a cell's source code and extract defines/references.

    Args:
        source: Cell source code as a string

    Returns:
        CellAnalysis with defines, references, and optional error message
    """
    if not source or not source.strip():
        # Empty cell
        return CellAnalysis(defines=[], references=[])

    try:
        tree = ast.parse(source)
    except SyntaxError as e:
        # Syntax error — return empty analysis with error message
        return CellAnalysis(
            defines=[],
            references=[],
            error=f"Syntax error: {e.msg}",
        )

    # Analyze the tree
    analyzer = VariableAnalyzer()
    analyzer.visit(tree)

    # Filter: exclude private variables and builtins
    builtin_names = set(dir(builtins)) | {"__name__", "__file__", "__doc__", "__package__"}
    defines = [v for v in analyzer.defines if not v.startswith("_")]
    references = [
        v
        for v in analyzer.references
        if not v.startswith("_") and v not in builtin_names and v not in defines
    ]

    # Remove duplicates and sort for consistency
    defines = sorted(set(defines))
    references = sorted(set(references))

    return CellAnalysis(
        defines=defines,
        references=references,
        error=None,
    )
