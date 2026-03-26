"""Base classes for transform definitions.

This module defines the core transform abstraction. A Transform encapsulates:
1. Parameter schema (what inputs it accepts)
2. Validation logic (are the parameters valid?)
3. Execution logic (how to run the transform locally)

Transforms are identified by a reference string in the format: {name}@{version}
Examples: "scan@v1", "duckdb_sql@v1", "polars_expr@v1"

Local vs Remote Execution:
- Personal mode: Transforms run locally via Transform.execute()
- Service mode: Transforms run remotely via HTTP executor (see registry.py)

Example:
    @register_transform("my_transform@v1")
    class MyTransform(Transform):
        class Params(BaseModel):
            foo: str
            bar: int = 10

        def validate(self, inputs: list[pa.Table]) -> None:
            if len(inputs) != 1:
                raise ValueError("Expected exactly one input")

        def execute(self, inputs: list[pa.Table], params: Params) -> pa.Table:
            return inputs[0].filter(...)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel

if TYPE_CHECKING:
    import pyarrow as pa


# Type variable for transform parameters
P = TypeVar("P", bound=BaseModel)


class Transform(ABC, Generic[P]):  # noqa: UP046
    """Abstract base class for all transforms.

    Each transform must define:
    - Params: A Pydantic model for parameter validation
    - execute(): The actual transformation logic

    Optionally:
    - validate(): Custom validation before execution
    - input_names: Names for inputs (default: input0, input1, ...)
    """

    # Class-level reference string (set by @register_transform)
    ref: ClassVar[str]

    # Parameter model class
    Params: ClassVar[type[BaseModel]]

    def validate(self, inputs: list[pa.Table], params: P) -> None:
        """Validate inputs and parameters before execution.

        Override this to add custom validation logic. Called before execute().

        Args:
            inputs: List of input Arrow tables
            params: Validated parameters

        Raises:
            ValueError: If validation fails
        """
        pass  # Default: no extra validation

    @abstractmethod
    def execute(self, inputs: list[pa.Table], params: P) -> pa.Table:
        """Execute the transform.

        Args:
            inputs: List of input Arrow tables
            params: Validated parameters

        Returns:
            Result Arrow table
        """
        ...

    def get_input_names(self, num_inputs: int) -> list[str]:
        """Get names for input tables.

        Override this to provide custom names (e.g., "left", "right" for joins).
        Default returns ["input0", "input1", ...].

        Args:
            num_inputs: Number of inputs

        Returns:
            List of input names
        """
        return [f"input{i}" for i in range(num_inputs)]

    @classmethod
    def parse_params(cls, params: dict[str, Any]) -> BaseModel:
        """Parse and validate parameters.

        Args:
            params: Raw parameter dictionary

        Returns:
            Validated Params instance

        Raises:
            ValidationError: If parameters are invalid
        """
        return cls.Params.model_validate(params)

    def run(
        self,
        inputs: list[pa.Table],
        params: dict[str, Any],
    ) -> pa.Table:
        """Parse, validate, and execute the transform.

        This is the main entry point for running a transform. It:
        1. Parses and validates parameters
        2. Calls validate() for custom validation
        3. Calls execute() to run the transform

        Args:
            inputs: List of input Arrow tables
            params: Raw parameter dictionary

        Returns:
            Result Arrow table
        """
        parsed_params = self.parse_params(params)
        self.validate(inputs, parsed_params)
        return self.execute(inputs, parsed_params)


# ---------------------------------------------------------------------------
# Transform Registry
# ---------------------------------------------------------------------------

# Global registry of transforms by reference
_transforms: dict[str, type[Transform]] = {}


def register_transform(ref: str):
    """Decorator to register a transform class.

    Args:
        ref: Transform reference (e.g., "duckdb_sql@v1")

    Example:
        @register_transform("my_transform@v1")
        class MyTransform(Transform):
            ...
    """

    def decorator(cls: type[Transform]) -> type[Transform]:
        cls.ref = ref
        _transforms[ref] = cls
        return cls

    return decorator


def get_transform(ref: str) -> Transform | None:
    """Get a transform instance by reference.

    Args:
        ref: Transform reference (e.g., "duckdb_sql@v1")

    Returns:
        Transform instance, or None if not registered
    """
    # Strip local:// prefix if present
    if ref.startswith("local://"):
        ref = ref[8:]

    cls = _transforms.get(ref)
    if cls is None:
        return None
    return cls()


def list_transforms() -> list[str]:
    """List all registered transform references.

    Returns:
        List of transform references
    """
    return list(_transforms.keys())


def _run_transform(
    ref: str,
    inputs: list[pa.Table],
    params: dict[str, Any],
) -> pa.Table:
    """Run a transform by reference (internal use only).

    This is an internal function used by the server's build runner
    and embedded executor. Users should call client.materialize() instead.

    Args:
        ref: Transform reference (e.g., "duckdb_sql@v1")
        inputs: List of input Arrow tables
        params: Transform parameters

    Returns:
        Result Arrow table

    Raises:
        ValueError: If transform is not registered
    """
    transform = get_transform(ref)
    if transform is None:
        raise ValueError(f"Unknown transform: {ref}")
    return transform.run(inputs, params)


# Backward compatibility alias (deprecated)
run_transform = _run_transform
