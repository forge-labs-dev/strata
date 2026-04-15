"""Tests for AST-based variable analysis."""

from strata.notebook.analyzer import analyze_cell


class TestAnalyzerBasics:
    """Test basic variable analysis."""

    def test_empty_cell(self):
        """Empty cell has no defines or references."""
        result = analyze_cell("")
        assert result.defines == []
        assert result.references == []
        assert result.error is None

    def test_comment_only_cell(self):
        """Cell with only comments has no variables."""
        result = analyze_cell("# This is a comment\n# Another comment")
        assert result.defines == []
        assert result.references == []

    def test_simple_assignment(self):
        """Simple assignment: x = ..."""
        result = analyze_cell("x = 1")
        assert result.defines == ["x"]
        assert result.references == []

    def test_multiple_assignments(self):
        """Multiple assignments in same cell."""
        result = analyze_cell("x = 1\ny = 2\nz = 3")
        assert set(result.defines) == {"x", "y", "z"}
        assert result.references == []

    def test_tuple_unpacking(self):
        """Tuple unpacking: a, b = ..."""
        result = analyze_cell("a, b = (1, 2)")
        assert set(result.defines) == {"a", "b"}
        assert result.references == []

    def test_list_unpacking(self):
        """List unpacking: [a, b] = ..."""
        result = analyze_cell("[a, b] = [1, 2]")
        assert set(result.defines) == {"a", "b"}
        assert result.references == []

    def test_nested_unpacking(self):
        """Nested unpacking: (a, (b, c)) = ..."""
        result = analyze_cell("(a, (b, c)) = (1, (2, 3))")
        assert set(result.defines) == {"a", "b", "c"}

    def test_starred_unpacking(self):
        """Starred unpacking: a, *rest, b = ..."""
        result = analyze_cell("a, *rest, b = [1, 2, 3, 4]")
        assert set(result.defines) == {"a", "rest", "b"}


class TestAnalyzerAssignmentTypes:
    """Test different assignment types."""

    def test_augmented_assignment(self):
        """Augmented assignment: x += ..."""
        result = analyze_cell("x += 1")
        assert result.defines == ["x"]

    def test_subscript_assignment(self):
        """Subscript mutation defines AND references the root (see analyzer docs)."""
        result = analyze_cell('df["col"] = 1')
        assert result.defines == ["df"]
        assert "df" in result.references
        assert result.mutation_defines == ["df"]

    def test_attribute_assignment(self):
        """Attribute mutation defines AND references the root."""
        result = analyze_cell("obj.attr = 1")
        assert result.defines == ["obj"]
        assert "obj" in result.references
        assert result.mutation_defines == ["obj"]

    def test_nested_attribute_assignment(self):
        """Nested attribute mutation defines AND references the root name."""
        result = analyze_cell("obj.inner.attr = 1")
        assert result.defines == ["obj"]
        assert "obj" in result.references
        assert result.mutation_defines == ["obj"]

    def test_annotated_assignment(self):
        """Annotated assignment: x: int = ..."""
        result = analyze_cell("x: int = 1")
        assert result.defines == ["x"]

    def test_annotated_assignment_no_value(self):
        """Annotated assignment without value: x: int."""
        result = analyze_cell("x: int")
        assert result.defines == ["x"]


class TestAnalyzerDefinitions:
    """Test definition extraction."""

    def test_function_definition(self):
        """Function definition: def f(): ..."""
        result = analyze_cell("def f():\n    x = 1\n    return x")
        assert result.defines == ["f"]
        assert result.references == []

    def test_nested_function(self):
        """Nested function definition — inner function not a top-level define."""
        result = analyze_cell("def outer():\n    def inner():\n        pass")
        assert result.defines == ["outer"]

    def test_class_definition(self):
        """Class definition: class C: ..."""
        result = analyze_cell("class C:\n    x = 1")
        assert result.defines == ["C"]

    def test_async_function(self):
        """Async function: async def f(): ..."""
        result = analyze_cell("async def f():\n    pass")
        assert result.defines == ["f"]


class TestAnalyzerImports:
    """Test import statement handling."""

    def test_simple_import(self):
        """import foo → defines foo."""
        result = analyze_cell("import pandas")
        assert result.defines == ["pandas"]

    def test_import_alias(self):
        """import foo as bar → defines bar."""
        result = analyze_cell("import pandas as pd")
        assert result.defines == ["pd"]

    def test_multiple_imports(self):
        """import foo, bar → defines foo and bar."""
        result = analyze_cell("import os, sys")
        assert set(result.defines) == {"os", "sys"}

    def test_from_import(self):
        """from foo import bar → defines bar."""
        result = analyze_cell("from pandas import DataFrame")
        assert result.defines == ["DataFrame"]

    def test_from_import_alias(self):
        """from foo import bar as baz → defines baz."""
        result = analyze_cell("from pandas import DataFrame as DF")
        assert result.defines == ["DF"]

    def test_from_import_multiple(self):
        """from foo import bar, baz → defines bar and baz."""
        result = analyze_cell("from pandas import DataFrame, Series")
        assert set(result.defines) == {"DataFrame", "Series"}


class TestAnalyzerReferences:
    """Test reference extraction."""

    def test_simple_reference(self):
        """Reference to undefined variable."""
        result = analyze_cell("y = x + 1")
        assert result.defines == ["y"]
        assert result.references == ["x"]

    def test_multiple_references(self):
        """Multiple references."""
        result = analyze_cell("z = x + y")
        assert result.defines == ["z"]
        assert set(result.references) == {"x", "y"}

    def test_function_call(self):
        """Function call: f() → f is referenced."""
        result = analyze_cell("result = len(mylist)")
        assert result.defines == ["result"]
        assert set(result.references) == {"mylist"}  # len is builtin

    def test_method_call(self):
        """Method call: obj.method()."""
        result = analyze_cell("result = df.sum()")
        assert result.defines == ["result"]
        assert result.references == ["df"]

    def test_subscript_reference(self):
        """Subscript: df["col"] → df is referenced."""
        result = analyze_cell('x = df["col"]')
        assert result.defines == ["x"]
        assert result.references == ["df"]

    def test_attribute_reference(self):
        """Attribute access: obj.attr → obj is referenced."""
        result = analyze_cell("x = obj.attr")
        assert result.defines == ["x"]
        assert result.references == ["obj"]

    def test_builtin_excluded(self):
        """Builtin functions like len, print are excluded."""
        result = analyze_cell("print(len([1, 2, 3]))")
        assert result.defines == []
        assert result.references == []


class TestAnalyzerPrivateVariables:
    """Test private variable (starting with _) handling."""

    def test_private_define_excluded(self):
        """Variables starting with _ are excluded from defines."""
        result = analyze_cell("_private = 1")
        assert result.defines == []

    def test_private_reference_excluded(self):
        """References to _private variables are excluded."""
        result = analyze_cell("x = _private + 1")
        assert result.defines == ["x"]
        assert result.references == []

    def test_dunder_excluded(self):
        """__dunder__ variables are excluded."""
        result = analyze_cell("__name__ = 'main'")
        assert result.defines == []

    def test_single_underscore_excluded(self):
        """Single _ is excluded."""
        result = analyze_cell("_ = unused")
        assert result.defines == []
        assert result.references == []  # unused is not defined


class TestAnalyzerLoopsAndContextManagers:
    """Test loop variables and context manager variables."""

    def test_for_loop_variable(self):
        """For loop variable: for x in ... → x is defined."""
        result = analyze_cell("for x in items:\n    print(x)")
        assert result.defines == ["x"]
        assert result.references == ["items"]

    def test_for_loop_nested_vars(self):
        """Nested for loop: for (a, b) in items."""
        result = analyze_cell("for (a, b) in items:\n    pass")
        assert set(result.defines) == {"a", "b"}
        assert result.references == ["items"]

    def test_with_statement_variable(self):
        """With statement: with ... as x → x is defined."""
        result = analyze_cell("with open('file') as f:\n    pass")
        assert result.defines == ["f"]
        assert result.references == []

    def test_except_handler_variable(self):
        """Exception handler: except E as e → e is defined."""
        result = analyze_cell("try:\n    pass\nexcept Exception as e:\n    pass")
        assert result.defines == ["e"]

    def test_async_with_statement(self):
        """Async with statement: async with ... as x."""
        result = analyze_cell("async with async_ctx() as x:\n    pass")
        assert result.defines == ["x"]
        assert result.references == ["async_ctx"]


class TestAnalyzerComprehensions:
    """Test comprehension handling (loop vars are NOT top-level)."""

    def test_list_comprehension(self):
        """List comprehension: [x for x in items] → x is NOT a top-level define."""
        result = analyze_cell("[x for x in items]")
        assert result.defines == []
        assert result.references == ["items"]

    def test_list_comprehension_with_condition(self):
        """List comp with condition: [x for x in items if x > 0]."""
        result = analyze_cell("[x for x in items if x > 0]")
        assert result.defines == []
        assert result.references == ["items"]

    def test_list_comprehension_with_outer_var(self):
        """List comp using outer variable: [x * factor for x in items].

        Note: In v1, the analyzer treats comprehensions as opaque and only
        visits the first generator's iterable. References inside the element
        expression are not extracted. This is acceptable for typical notebooks.
        """
        result = analyze_cell("[x * factor for x in items]")
        assert result.defines == []
        assert result.references == ["items"]  # factor inside comprehension not detected

    def test_dict_comprehension(self):
        """Dict comprehension: {k: v for k, v in items}."""
        result = analyze_cell("{k: v for k, v in items}")
        assert result.defines == []
        assert result.references == ["items"]

    def test_set_comprehension(self):
        """Set comprehension: {x for x in items}."""
        result = analyze_cell("{x for x in items}")
        assert result.defines == []
        assert result.references == ["items"]

    def test_generator_expression(self):
        """Generator expression: (x for x in items)."""
        result = analyze_cell("(x for x in items)")
        assert result.defines == []
        assert result.references == ["items"]


class TestAnalyzerLambda:
    """Test lambda expressions."""

    def test_lambda_simple(self):
        """Lambda: lambda x: x + 1 — x is parameter, not cell-level define."""
        result = analyze_cell("f = lambda x: x + 1")
        assert result.defines == ["f"]
        assert result.references == []

    def test_lambda_with_outer_ref(self):
        """Lambda with outer reference: lambda x: x + y."""
        result = analyze_cell("f = lambda x: x + y")
        assert result.defines == ["f"]
        assert result.references == ["y"]


class TestAnalyzerWalrusOperator:
    """Test walrus operator (:=) at top level."""

    def test_walrus_in_if(self):
        """Walrus in if: if (x := value): — x is defined."""
        result = analyze_cell("if (x := value):\n    pass")
        assert result.defines == ["x"]
        assert result.references == ["value"]


class TestAnalyzerRealWorldExamples:
    """Test real-world notebook cells."""

    def test_pandas_cell(self):
        """Real-world: pandas DataFrame operations."""
        source = """
import pandas as pd
df = pd.read_csv('data.csv')
df['new_col'] = df['old_col'] * 2
cleaned = df[df['new_col'] > 100]
"""
        result = analyze_cell(source)
        assert set(result.defines) == {"pd", "df", "cleaned"}
        assert result.references == []

    def test_data_transformation_cell(self):
        """Real-world: transform input dataframe."""
        source = """
cleaned = df[df.value > 50]
summary = {
    'rows': len(cleaned),
    'mean': cleaned.value.mean(),
}
"""
        result = analyze_cell(source)
        assert set(result.defines) == {"cleaned", "summary"}
        assert result.references == ["df"]

    def test_plot_cell(self):
        """Real-world: plotting with external inputs."""
        source = """
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.plot(data.x, data.y)
ax.set_title(title)
plt.show()
"""
        result = analyze_cell(source)
        assert set(result.defines) == {"plt", "fig", "ax"}
        assert set(result.references) == {"data", "title"}

    def test_model_training_cell(self):
        """Real-world: ML model training."""
        source = """
from sklearn.ensemble import RandomForestClassifier
model = RandomForestClassifier(n_estimators=100)
model.fit(X_train, y_train)
score = model.score(X_test, y_test)
"""
        result = analyze_cell(source)
        assert set(result.defines) == {"RandomForestClassifier", "model", "score"}
        assert set(result.references) == {"X_train", "y_train", "X_test", "y_test"}


class TestAnalyzerSyntaxErrors:
    """Test handling of syntax errors."""

    def test_syntax_error_returns_error(self):
        """Syntax error returns error message."""
        result = analyze_cell("x = ")
        assert result.defines == []
        assert result.references == []
        assert result.error is not None
        assert "Syntax error" in result.error

    def test_syntax_error_unclosed_paren(self):
        """Unclosed parenthesis."""
        result = analyze_cell("x = sum([1, 2, 3")
        assert result.error is not None


class TestAnalyzerEdgeCases:
    """Test edge cases and corner cases."""

    def test_variable_defined_then_used(self):
        """Variable defined then used in same cell."""
        result = analyze_cell("x = 1\ny = x + 1")
        assert set(result.defines) == {"x", "y"}
        # x is not a reference because it's defined in the cell
        assert result.references == []

    def test_global_statement_ignored(self):
        """Global statement: global x — x is not a top-level define."""
        result = analyze_cell("global x\nx = 1")
        assert result.defines == ["x"]

    def test_nonlocal_statement_ignored(self):
        """Nonlocal statement: nonlocal x."""
        result = analyze_cell("def outer():\n    x = 1\n    def inner():\n        nonlocal x")
        assert result.defines == ["outer"]

    def test_del_statement(self):
        """del statement does not define variables."""
        result = analyze_cell("del x")
        assert result.defines == []
        assert result.references == ["x"]

    def test_assert_statement(self):
        """assert statement references variables."""
        result = analyze_cell("assert x > 0")
        assert result.defines == []
        assert result.references == ["x"]

    def test_raise_statement(self):
        """raise with exception expression."""
        result = analyze_cell("raise ValueError(msg)")
        assert result.defines == []
        assert result.references == ["msg"]
