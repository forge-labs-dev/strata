"""DAG construction and analysis for notebook cells."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass
class DagEdge:
    """An edge in the DAG representing a variable dependency.

    Attributes:
        from_cell_id: Cell that defines the variable
        to_cell_id: Cell that references the variable
        variable: Variable name that flows along this edge
    """

    from_cell_id: str
    to_cell_id: str
    variable: str


@dataclass
class NotebookDag:
    """The complete DAG for a notebook.

    Attributes:
        edges: All variable-level edges in the DAG
        cell_upstream: For each cell, list of upstream cell IDs it depends on
        cell_downstream: For each cell, list of downstream cell IDs that depend on it
        leaves: Set of cell IDs with no downstream consumers
        roots: Set of cell IDs with no upstream dependencies
        topological_order: Cells in valid execution order
        variable_producer: For each variable, which cell produces it (last in cell order wins)
        consumed_variables: For each cell, set of variable names consumed by downstream cells
    """

    edges: list[DagEdge] = field(default_factory=list)
    cell_upstream: dict[str, list[str]] = field(default_factory=dict)
    cell_downstream: dict[str, list[str]] = field(default_factory=dict)
    leaves: set[str] = field(default_factory=set)
    roots: set[str] = field(default_factory=set)
    topological_order: list[str] = field(default_factory=list)
    variable_producer: dict[str, str] = field(default_factory=dict)
    consumed_variables: dict[str, set[str]] = field(default_factory=dict)


@dataclass
class CellAnalysisWithId:
    """Cell analysis result paired with cell ID.

    Attributes:
        id: Cell ID
        defines: Variables defined by this cell
        references: Variables referenced by this cell
    """

    id: str
    defines: list[str]
    references: list[str]


def build_dag(cells: list[CellAnalysisWithId]) -> NotebookDag:
    """Build the DAG from cell analyses.

    Args:
        cells: List of cells with their analysis results, in execution order

    Returns:
        NotebookDag with edges, upstream/downstream relations, and metadata
    """
    dag = NotebookDag()
    cell_ids = [c.id for c in cells]

    # Initialize structures
    for cell_id in cell_ids:
        dag.cell_upstream[cell_id] = []
        dag.cell_downstream[cell_id] = []
        dag.consumed_variables[cell_id] = set()

    # Build variable → producer map (last definition wins)
    # This handles shadowing: if two cells define the same variable,
    # the later one is the producer for downstream consumers
    for cell in cells:
        for var in cell.defines:
            dag.variable_producer[var] = cell.id

    # Build edges: for each reference, connect from the producer
    for cell in cells:
        for var in cell.references:
            producer_id = dag.variable_producer.get(var)
            if producer_id:
                if producer_id == cell.id:
                    # Self-cycle: cell references its own output
                    raise ValueError(
                        f"Cycle detected in DAG: cell {cell.id} "
                        f"references its own variable {var}"
                    )
                else:
                    # Add edge from producer to consumer
                    edge = DagEdge(
                        from_cell_id=producer_id,
                        to_cell_id=cell.id,
                        variable=var,
                    )
                    dag.edges.append(edge)

                    # Add upstream/downstream relationships (avoid duplicates)
                    if producer_id not in dag.cell_upstream[cell.id]:
                        dag.cell_upstream[cell.id].append(producer_id)
                    if cell.id not in dag.cell_downstream[producer_id]:
                        dag.cell_downstream[producer_id].append(cell.id)

                    # Mark variable as consumed by the producer
                    dag.consumed_variables[producer_id].add(var)

    # Identify leaves (cells with no downstream consumers)
    for cell_id in cell_ids:
        if not dag.cell_downstream[cell_id]:
            dag.leaves.add(cell_id)

    # Identify roots (cells with no upstream dependencies)
    for cell_id in cell_ids:
        if not dag.cell_upstream[cell_id]:
            dag.roots.add(cell_id)

    # Topological sort
    dag.topological_order = topological_sort(dag, cell_ids)

    return dag


def topological_sort(dag: NotebookDag, cell_ids: list[str]) -> list[str]:
    """Return cells in topological (execution) order.

    Uses Kahn's algorithm with cycle detection.

    Args:
        dag: The DAG
        cell_ids: List of all cell IDs in original order

    Returns:
        Cells in topological order

    Raises:
        ValueError: If a cycle is detected
    """
    # Build in-degree map
    in_degree = {cell_id: len(dag.cell_upstream[cell_id]) for cell_id in cell_ids}

    # Find all nodes with in-degree 0
    queue = [cell_id for cell_id in cell_ids if in_degree[cell_id] == 0]
    result = []

    while queue:
        # Process a node with in-degree 0
        current = queue.pop(0)
        result.append(current)

        # For each downstream cell, reduce in-degree
        for downstream_id in dag.cell_downstream[current]:
            in_degree[downstream_id] -= 1
            if in_degree[downstream_id] == 0:
                queue.append(downstream_id)

    # Check for cycles
    if len(result) != len(cell_ids):
        cycles = detect_cycles(dag, cell_ids)
        cycle_str = " → ".join(cycles[0]) if cycles else "unknown"
        raise ValueError(f"Cycle detected in DAG: {cycle_str}")

    return result


def detect_cycles(dag: NotebookDag, cell_ids: list[str]) -> list[list[str]]:
    """Find all cycles in the DAG using DFS.

    Args:
        dag: The DAG
        cell_ids: List of all cell IDs

    Returns:
        List of cycles (each cycle is a list of cell IDs)
    """
    # Colors: 0=white, 1=gray, 2=black
    color = {cell_id: 0 for cell_id in cell_ids}
    cycles: list[list[str]] = []

    def dfs(node: str, path: list[str]) -> None:
        """DFS to find cycles."""
        color[node] = 1  # Gray
        path.append(node)

        for downstream in dag.cell_downstream[node]:
            if color[downstream] == 1:
                # Back edge — found a cycle
                cycle_start = path.index(downstream)
                cycle = path[cycle_start:] + [downstream]
                cycles.append(cycle)
            elif color[downstream] == 0:
                # White — recurse
                dfs(downstream, path)

        path.pop()
        color[node] = 2  # Black

    for cell_id in cell_ids:
        if color[cell_id] == 0:
            dfs(cell_id, [])

    return cycles


def get_cascade_plan(dag: NotebookDag, target_cell_id: str, cell_ids: list[str]) -> list[str]:
    """Get all upstream cells needed before executing a target cell.

    Args:
        dag: The DAG
        target_cell_id: The cell to execute
        cell_ids: List of all cell IDs in execution order

    Returns:
        List of cell IDs in execution order that need to run before the target.
        If the target is a root cell, includes the target cell itself.
        Otherwise, includes only upstream cells.
    """
    # BFS backwards from target to find all reachable upstream cells
    visited = set()
    queue = [target_cell_id]

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)

        for upstream_id in dag.cell_upstream[current]:
            if upstream_id not in visited:
                queue.append(upstream_id)

    # If the target cell has no upstream dependencies (is a root), keep it in the plan
    # Otherwise, remove it (we only want upstream cells)
    if dag.cell_upstream[target_cell_id]:
        # Target has upstream dependencies, remove it
        visited.discard(target_cell_id)
    else:
        # Target is a root cell, keep it (it needs to be executed)
        pass

    # Return visited cells in topological order
    plan = [cid for cid in cell_ids if cid in visited]
    return plan
