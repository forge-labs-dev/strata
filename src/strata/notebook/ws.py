"""WebSocket handler for real-time notebook execution updates.

Manages WebSocket connections per notebook, dispatches client messages,
and streams server updates (cell status, console output, execution results).
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from strata.notebook.cascade import CascadePlanner
from strata.notebook.executor import CellExecutor
from strata.notebook.impact import ImpactAnalyzer
from strata.notebook.inspect_repl import InspectManager
from strata.notebook.session import SessionManager
from strata.notebook.writer import write_cell

if TYPE_CHECKING:
    from strata.notebook.cascade import CascadePlan
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/notebooks", tags=["notebooks_ws"])

# Per-notebook WebSocket connections (for broadcast)
_notebook_connections: dict[str, list[WebSocket]] = {}

# Per-notebook execution state
_notebook_execution_state: dict[str, dict[str, Any]] = {}

# Per-notebook inspect managers
_notebook_inspect_managers: dict[str, InspectManager] = {}


def _get_session_manager() -> SessionManager:
    """Get the session manager from routes module."""
    from strata.notebook.routes import get_session_manager
    return get_session_manager()


# ============================================================================
# Message Serialization
# ============================================================================


def _serialize_datetime(obj: Any) -> str:
    """Serialize datetime to ISO 8601 string."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _json_encode(obj: Any) -> str:
    """Encode object to JSON, handling datetime and Path objects."""
    return json.dumps(
        obj,
        default=_serialize_datetime,
        ensure_ascii=False,
    )


def _json_decode(text: str) -> Any:
    """Decode JSON from string."""
    return json.loads(text)


# ============================================================================
# WebSocket Handler
# ============================================================================


@router.websocket("/ws/{notebook_id}")
async def notebook_websocket(websocket: WebSocket, notebook_id: str):
    """WebSocket endpoint for real-time notebook updates.

    Accepts messages:
    - cell_execute: Run a cell (check if cascade needed)
    - cell_execute_cascade: Execute cascade plan
    - cell_execute_force: Run with stale inputs
    - cell_cancel: Cancel execution
    - cell_source_update: Source code changed
    - notebook_sync: Request full state

    Sends messages:
    - cell_status: Status changed
    - cell_output: Execution result
    - cell_console: Incremental stdout/stderr
    - cell_error: Execution failed
    - dag_update: DAG changed
    - cascade_prompt: Cascade needed
    - cascade_progress: Progress during cascade
    - notebook_state: Full state (response to sync)
    """
    # Get or create session
    session_manager = _get_session_manager()
    session = session_manager.get_session(notebook_id)
    if not session:
        await websocket.close(code=1008, reason="Notebook not found")
        return

    # Accept connection
    await websocket.accept()

    # Add to connections list
    if notebook_id not in _notebook_connections:
        _notebook_connections[notebook_id] = []
    _notebook_connections[notebook_id].append(websocket)

    # Initialize execution state
    if notebook_id not in _notebook_execution_state:
        _notebook_execution_state[notebook_id] = {
            "running_cell": None,
            "sequence": 0,  # For sequencing incoming messages
            "cascade_plan": None,
        }

    execution_state = _notebook_execution_state[notebook_id]

    try:
        while True:
            # Receive message
            data = await websocket.receive_text()
            msg = _json_decode(data)

            # Extract message type and payload
            msg_type = msg.get("type")
            payload = msg.get("payload", {})

            # Handle each message type
            if msg_type == "cell_execute":
                await _handle_cell_execute(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "cell_execute_cascade":
                await _handle_cell_execute_cascade(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "cell_execute_force":
                await _handle_cell_execute_force(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "cell_cancel":
                await _handle_cell_cancel(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "cell_source_update":
                await _handle_cell_source_update(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "notebook_sync":
                await _handle_notebook_sync(websocket, session, notebook_id)
            elif msg_type == "impact_preview_request":
                await _handle_impact_preview_request(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "profiling_request":
                await _handle_profiling_request(
                    websocket, session, execution_state, notebook_id
                )
            elif msg_type == "inspect_open":
                await _handle_inspect_open(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "inspect_eval":
                await _handle_inspect_eval(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "inspect_close":
                await _handle_inspect_close(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "dependency_add":
                await _handle_dependency_add(
                    websocket, session, payload, execution_state, notebook_id
                )
            elif msg_type == "dependency_remove":
                await _handle_dependency_remove(
                    websocket, session, payload, execution_state, notebook_id
                )
            else:
                # Unknown message type
                await websocket.send_text(
                    _json_encode(
                        {
                            "type": "error",
                            "seq": execution_state.get("sequence", 0),
                            "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                            "payload": {"error": f"Unknown message type: {msg_type}"},
                        }
                    )
                )

    except WebSocketDisconnect:
        # Clean up connection
        if notebook_id in _notebook_connections:
            try:
                _notebook_connections[notebook_id].remove(websocket)
            except ValueError:
                pass
            # Clean up state when all connections close
            if not _notebook_connections[notebook_id]:
                del _notebook_connections[notebook_id]
                _notebook_execution_state.pop(notebook_id, None)
                _notebook_inspect_managers.pop(notebook_id, None)
    except Exception as e:
        logger.exception("WebSocket error: %s", e)
        # Remove dead connection
        if notebook_id in _notebook_connections:
            try:
                _notebook_connections[notebook_id].remove(websocket)
            except ValueError:
                pass
            if not _notebook_connections[notebook_id]:
                del _notebook_connections[notebook_id]
                _notebook_execution_state.pop(notebook_id, None)
                _notebook_inspect_managers.pop(notebook_id, None)
        try:
            await websocket.close(code=1011, reason="Internal error")
        except Exception:
            pass


# ============================================================================
# Message Handlers
# ============================================================================


async def _handle_cell_execute(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle cell_execute message.

    Check if cascade needed. If yes, send cascade_prompt.
    If no, execute cell directly.
    """
    cell_id = payload.get("cell_id")
    if not cell_id:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": execution_state["sequence"],
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": "Missing cell_id"},
                }
            )
        )
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    # Find cell
    cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
    if not cell:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": f"Cell {cell_id} not found"},
                }
            )
        )
        return

    # Check if cascade is needed
    planner = CascadePlanner(session)
    plan = planner.plan(cell_id)

    if plan:
        # Cascade needed — send cascade_prompt so the frontend can
        # auto-accept or prompt the user.  No impact_preview here;
        # downstream staleness is communicated via cell_status updates.
        logger.info(
            "Cascade needed for cell %s — upstream statuses: %s",
            cell_id,
            {
                uid: next(
                    (c.status for c in session.notebook_state.cells if c.id == uid),
                    "?",
                )
                for uid in (session.dag.cell_upstream.get(cell_id, []) if session.dag else [])
            },
        )
        execution_state["cascade_plan"] = plan
        await _broadcast_message(
            notebook_id,
            {
                "type": "cascade_prompt",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {
                    "cell_id": cell_id,
                    "plan_id": plan.plan_id,
                    "cells_to_run": [s.cell_id for s in plan.steps],
                    "estimated_duration_ms": plan.estimated_duration_ms,
                },
            },
        )
    else:
        # No cascade needed — execute directly.
        await _execute_cell_directly(
            websocket, session, cell_id, execution_state, notebook_id
        )


async def _handle_cell_execute_cascade(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle cell_execute_cascade message.

    User confirmed cascade — execute all cells in the plan.
    """
    cell_id = payload.get("cell_id")
    plan_id = payload.get("plan_id")

    if not cell_id or not plan_id:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": execution_state["sequence"],
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": "Missing cell_id or plan_id"},
                }
            )
        )
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    # Get the cascade plan
    plan = execution_state.get("cascade_plan")
    if not plan or plan.plan_id != plan_id:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": "Cascade plan not found or expired"},
                }
            )
        )
        return

    # Execute cascade
    await _execute_cascade(
        websocket, session, plan, execution_state, notebook_id
    )


async def _handle_cell_execute_force(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle cell_execute_force message.

    Execute cell with stale inputs ("Run this only").
    """
    cell_id = payload.get("cell_id")
    if not cell_id:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": execution_state["sequence"],
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": "Missing cell_id"},
                }
            )
        )
        return

    execution_state["sequence"] += 1

    # Execute cell directly, ignoring staleness
    # The result will be marked as stale:forced
    await _execute_cell_directly(
        websocket, session, cell_id, execution_state, notebook_id, force=True
    )


async def _handle_cell_cancel(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle cell_cancel message.

    Cancel a running cell (best-effort).
    """
    cell_id = payload.get("cell_id")
    if not cell_id:
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    # TODO: Implement cancellation by killing subprocess
    # For now, clear running state and send idle status
    running_cell = execution_state.get("running_cell")
    if running_cell == cell_id:
        execution_state["running_cell"] = None

    # Always send idle status to acknowledge the cancel
    await _broadcast_message(
        notebook_id,
        {
            "type": "cell_status",
            "seq": seq,
            "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            "payload": {
                "cell_id": cell_id,
                "status": "idle",
            },
        },
    )


async def _handle_cell_source_update(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle cell_source_update message.

    Cell source changed — re-analyze and update DAG.
    """
    cell_id = payload.get("cell_id")
    source = payload.get("source")

    if not cell_id or source is None:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": execution_state["sequence"],
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": "Missing cell_id or source"},
                }
            )
        )
        return

    if len(source) > 1_000_000:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": execution_state["sequence"],
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": "Cell source exceeds 1MB limit"},
                }
            )
        )
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    try:
        # Write to disk
        write_cell(session.path, cell_id, source)

        # Update source in session (must happen before re-analysis)
        cell_in_session = next(
            (c for c in session.notebook_state.cells if c.id == cell_id), None
        )
        if cell_in_session:
            cell_in_session.source = source

        # Re-analyze cell and rebuild DAG
        session.re_analyze_cell(cell_id)

        # Recompute staleness
        staleness_map = session.compute_staleness()

        # Build DAG update message
        dag_edges = []
        if session.dag:
            for edge in session.dag.edges:
                dag_edges.append(
                    {
                        "from_cell_id": edge.from_cell_id,
                        "to_cell_id": edge.to_cell_id,
                        "variable": edge.variable,
                    }
                )

        # Send DAG update
        await _broadcast_message(
            notebook_id,
            {
                "type": "dag_update",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {
                    "edges": dag_edges,
                    "roots": list(session.dag.roots) if session.dag else [],
                    "leaves": list(session.dag.leaves) if session.dag else [],
                    "topological_order": (
                        session.dag.topological_order if session.dag else []
                    ),
                },
            },
        )

        # Send cell status updates for affected cells (with v1.1 causality)
        for cell_id_to_update, staleness in staleness_map.items():
            payload: dict[str, Any] = {
                "cell_id": cell_id_to_update,
                "status": staleness.status,
                "staleness_reasons": (
                    [r.value for r in staleness.reasons]
                    if staleness.reasons
                    else []
                ),
            }
            # v1.1: Attach causality chain if cell is stale
            causality = session.causality_map.get(cell_id_to_update)
            if causality:
                payload["causality"] = causality.to_dict()

            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_status",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "payload": payload,
                },
            )

    except Exception as e:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "error",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"error": str(e)},
                }
            )
        )


async def _handle_notebook_sync(
    websocket: WebSocket, session: NotebookSession, notebook_id: str
) -> None:
    """Handle notebook_sync message.

    Return full notebook state (for reconnection).
    """
    # Build full notebook state
    cells_data = []
    for cell in session.notebook_state.cells:
        cells_data.append(
            {
                "id": cell.id,
                "source": cell.source,
                "language": cell.language,
                "order": cell.order,
                "status": cell.status,
                "defines": cell.defines,
                "references": cell.references,
                "upstream_ids": cell.upstream_ids,
                "downstream_ids": cell.downstream_ids,
                "is_leaf": cell.is_leaf,
            }
        )

    # Build DAG
    dag_edges = []
    if session.dag:
        for edge in session.dag.edges:
            dag_edges.append(
                {
                    "from_cell_id": edge.from_cell_id,
                    "to_cell_id": edge.to_cell_id,
                    "variable": edge.variable,
                }
            )

    state = {
        "id": session.notebook_state.id,
        "name": session.notebook_state.name,
        "cells": cells_data,
        "dag": {
            "edges": dag_edges,
            "roots": list(session.dag.roots) if session.dag else [],
            "leaves": list(session.dag.leaves) if session.dag else [],
            "topological_order": (
                session.dag.topological_order if session.dag else []
            ),
        },
    }

    await websocket.send_text(
        _json_encode(
            {
                "type": "notebook_state",
                "seq": 0,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": state,
            }
        )
    )


# ============================================================================
# Execution Helpers
# ============================================================================


async def _execute_cell_directly(
    websocket: WebSocket,
    session: NotebookSession,
    cell_id: str,
    execution_state: dict[str, Any],
    notebook_id: str,
    force: bool = False,
) -> None:
    """Execute a cell directly (not part of cascade)."""
    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    # Find cell
    cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
    if not cell:
        return

    # Mark as running — update backend state AND broadcast
    execution_state["running_cell"] = cell_id
    run_cell = next(
        (c for c in session.notebook_state.cells if c.id == cell_id), None
    )
    if run_cell:
        run_cell.status = "running"
    await _broadcast_message(
        notebook_id,
        {
            "type": "cell_status",
            "seq": seq,
            "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            "payload": {"cell_id": cell_id, "status": "running"},
        },
    )

    # Execute
    executor = CellExecutor(session)
    try:
        result = await executor.execute_cell(cell_id, cell.source)

        # Send console output
        if result.stdout:
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_console",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {
                        "cell_id": cell_id,
                        "stream": "stdout",
                        "text": result.stdout,
                    },
                },
            )

        if result.stderr:
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_console",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {
                        "cell_id": cell_id,
                        "stream": "stderr",
                        "text": result.stderr,
                    },
                },
            )

        # v1.1: Record execution for profiling
        session.record_execution(
            cell_id, result.duration_ms, result.cache_hit
        )

        # Send output with v1.1 profiling data
        if result.success:
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_output",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "payload": {
                        "cell_id": cell_id,
                        "outputs": result.outputs,
                        "cache_hit": result.cache_hit,
                        "duration_ms": int(result.duration_ms),
                        "artifact_uri": result.artifact_uri,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                        # v1.1: Profiling data
                        "execution_method": result.execution_method,
                        "mutation_warnings": result.mutation_warnings,
                    },
                },
            )
        else:
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_error",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "payload": {
                        "cell_id": cell_id,
                        "error": result.error,
                        **({"suggest_install": result.suggest_install}
                           if result.suggest_install else {}),
                    },
                },
            )

        # Mark as ready — update backend state AND broadcast
        status = "ready" if result.success else "error"
        cell = next(
            (c for c in session.notebook_state.cells if c.id == cell_id), None
        )
        if cell:
            cell.status = status
        await _broadcast_message(
            notebook_id,
            {
                "type": "cell_status",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"cell_id": cell_id, "status": status},
            },
        )

    except Exception as e:
        await _broadcast_message(
            notebook_id,
            {
                "type": "cell_error",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"cell_id": cell_id, "error": str(e)},
            },
        )
        await _broadcast_message(
            notebook_id,
            {
                "type": "cell_status",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"cell_id": cell_id, "status": "error"},
            },
        )
    finally:
        execution_state["running_cell"] = None


async def _execute_cascade(
    websocket: WebSocket,
    session: NotebookSession,
    plan: CascadePlan,
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Execute all cells in a cascade plan."""
    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    executor = CellExecutor(session)

    logger.info(
        "Cascade %s: executing %d steps: %s",
        plan.plan_id,
        len(plan.steps),
        [(s.cell_id, s.reason, s.skip) for s in plan.steps],
    )

    cascade_failed = False

    for i, step in enumerate(plan.steps):
        if step.skip:
            # Skip cached cells
            continue

        cell_id = step.cell_id
        cell = next(
            (c for c in session.notebook_state.cells if c.id == cell_id), None
        )
        if not cell:
            continue

        # If an earlier cascade step failed, abort remaining steps
        if cascade_failed:
            logger.warning(
                "Cascade %s: skipping cell %s (earlier step failed)",
                plan.plan_id, cell_id,
            )
            cascade_cell = next(
                (c for c in session.notebook_state.cells if c.id == cell_id),
                None,
            )
            if cascade_cell:
                cascade_cell.status = "idle"
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_status",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "payload": {"cell_id": cell_id, "status": "idle"},
                },
            )
            continue

        execution_state["running_cell"] = cell_id

        # Send cascade progress
        await _broadcast_message(
            notebook_id,
            {
                "type": "cascade_progress",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {
                    "plan_id": plan.plan_id,
                    "current_cell_id": cell_id,
                    "completed": i,
                    "total": len([s for s in plan.steps if not s.skip]),
                },
            },
        )

        # Execute cell — update backend state AND broadcast
        cascade_run_cell = next(
            (c for c in session.notebook_state.cells if c.id == cell_id),
            None,
        )
        if cascade_run_cell:
            cascade_run_cell.status = "running"
        await _broadcast_message(
            notebook_id,
            {
                "type": "cell_status",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"cell_id": cell_id, "status": "running"},
            },
        )

        try:
            result = await executor.execute_cell(cell_id, cell.source)

            # v1.1: Record execution for profiling
            session.record_execution(
                cell_id, result.duration_ms, result.cache_hit
            )

            # Send console output for cascade cells
            if result.stdout:
                await _broadcast_message(
                    notebook_id,
                    {
                        "type": "cell_console",
                        "seq": seq,
                        "ts": datetime.now(tz=UTC).isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "payload": {
                            "cell_id": cell_id,
                            "stream": "stdout",
                            "text": result.stdout,
                        },
                    },
                )

            # Send output with v1.1 profiling data
            if result.success:
                await _broadcast_message(
                    notebook_id,
                    {
                        "type": "cell_output",
                        "seq": seq,
                        "ts": datetime.now(tz=UTC).isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "payload": {
                            "cell_id": cell_id,
                            "outputs": result.outputs,
                            "cache_hit": result.cache_hit,
                            "duration_ms": int(result.duration_ms),
                            "artifact_uri": result.artifact_uri,
                            "stdout": result.stdout,
                            "stderr": result.stderr,
                            "execution_method": result.execution_method,
                            "mutation_warnings": result.mutation_warnings,
                        },
                    },
                )
            else:
                await _broadcast_message(
                    notebook_id,
                    {
                        "type": "cell_error",
                        "seq": seq,
                        "ts": datetime.now(tz=UTC).isoformat().replace(
                            "+00:00", "Z"
                        ),
                        "payload": {
                            "cell_id": cell_id,
                            "error": result.error,
                            **({"suggest_install": result.suggest_install}
                               if result.suggest_install else {}),
                        },
                    },
                )

            # Mark as ready — update backend state AND broadcast
            status = "ready" if result.success else "error"
            cascade_cell = next(
                (c for c in session.notebook_state.cells if c.id == cell_id),
                None,
            )
            if cascade_cell:
                cascade_cell.status = status
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_status",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "payload": {"cell_id": cell_id, "status": status},
                },
            )

            logger.info(
                "Cascade %s: cell %s finished status=%s "
                "artifact_uri=%s cache_hit=%s",
                plan.plan_id, cell_id, status,
                getattr(cascade_cell, "artifact_uri", None)
                if cascade_cell else None,
                result.cache_hit,
            )

            # If a step fails, abort the rest of the cascade
            if not result.success:
                cascade_failed = True

        except Exception as e:
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_error",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"cell_id": cell_id, "error": str(e)},
                },
            )
            await _broadcast_message(
                notebook_id,
                {
                    "type": "cell_status",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                    "payload": {"cell_id": cell_id, "status": "error"},
                },
            )
            cascade_failed = True

    execution_state["running_cell"] = None


def _get_inspect_manager(notebook_id: str) -> InspectManager:
    """Get or create an InspectManager for a notebook."""
    if notebook_id not in _notebook_inspect_managers:
        _notebook_inspect_managers[notebook_id] = InspectManager()
    return _notebook_inspect_managers[notebook_id]


async def _handle_inspect_open(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle inspect_open — spawn REPL with cell's inputs loaded."""
    cell_id = payload.get("cell_id")
    if not cell_id:
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    mgr = _get_inspect_manager(notebook_id)
    inspect_session, status = await mgr.open_session(cell_id, session)

    await websocket.send_text(
        _json_encode(
            {
                "type": "inspect_result",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace(
                    "+00:00", "Z"
                ),
                "payload": {
                    "cell_id": cell_id,
                    "action": "open",
                    "ok": inspect_session.ready,
                    "result": status,
                    "type": "str",
                },
            }
        )
    )


async def _handle_inspect_eval(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle inspect_eval — evaluate expression in REPL."""
    cell_id = payload.get("cell_id")
    expr = payload.get("expr", "")
    if not cell_id or not expr:
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    mgr = _get_inspect_manager(notebook_id)
    inspect_session = await mgr.get_session(cell_id)

    if inspect_session is None:
        await websocket.send_text(
            _json_encode(
                {
                    "type": "inspect_result",
                    "seq": seq,
                    "ts": datetime.now(tz=UTC).isoformat().replace(
                        "+00:00", "Z"
                    ),
                    "payload": {
                        "cell_id": cell_id,
                        "action": "eval",
                        "ok": False,
                        "error": "No inspect session open for this cell",
                    },
                }
            )
        )
        return

    result = await inspect_session.evaluate(expr)

    await websocket.send_text(
        _json_encode(
            {
                "type": "inspect_result",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace(
                    "+00:00", "Z"
                ),
                "payload": {
                    "cell_id": cell_id,
                    "action": "eval",
                    "expr": expr,
                    **result,
                },
            }
        )
    )


async def _handle_inspect_close(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle inspect_close — shut down REPL."""
    cell_id = payload.get("cell_id")
    if not cell_id:
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    mgr = _get_inspect_manager(notebook_id)
    await mgr.close_session(cell_id)

    await websocket.send_text(
        _json_encode(
            {
                "type": "inspect_result",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace(
                    "+00:00", "Z"
                ),
                "payload": {
                    "cell_id": cell_id,
                    "action": "close",
                    "ok": True,
                    "result": "closed",
                },
            }
        )
    )


async def _handle_impact_preview_request(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle impact_preview_request — user wants to see impact before running."""
    cell_id = payload.get("cell_id")
    if not cell_id:
        return

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    analyzer = ImpactAnalyzer(session)
    impact = analyzer.preview(cell_id)

    await _broadcast_message(
        notebook_id,
        {
            "type": "impact_preview",
            "seq": seq,
            "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            "payload": impact.to_dict(),
        },
    )


async def _handle_profiling_request(
    websocket: WebSocket,
    session: NotebookSession,
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle profiling_request — return notebook profiling summary."""
    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    summary = session.get_profiling_summary()

    await websocket.send_text(
        _json_encode(
            {
                "type": "profiling_summary",
                "seq": seq,
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": summary,
            }
        )
    )


async def _handle_dependency_add(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle dependency_add — add a package and broadcast the change."""
    from strata.notebook.dependencies import add_dependency
    from strata.notebook.routes import validate_package_name

    package = payload.get("package", "")
    if not package:
        execution_state["sequence"] += 1
        await websocket.send_text(
            _json_encode({
                "type": "error",
                "seq": execution_state["sequence"],
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"error": "Missing 'package' in payload"},
            })
        )
        return

    try:
        package = validate_package_name(package)
    except ValueError as e:
        execution_state["sequence"] += 1
        await websocket.send_text(
            _json_encode({
                "type": "error",
                "seq": execution_state["sequence"],
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"error": str(e)},
            })
        )
        return

    result = add_dependency(session.path, package)

    if result.lockfile_changed:
        await session.on_dependencies_changed()

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    msg = {
        "type": "dependency_changed",
        "seq": seq,
        "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "payload": {
            "action": "add",
            "package": result.package,
            "success": result.success,
            "error": result.error,
            "lockfile_changed": result.lockfile_changed,
            "dependencies": [
                {"name": d.name, "version": d.version, "specifier": d.specifier}
                for d in result.dependencies
            ],
        },
    }
    await _broadcast_message(notebook_id, msg)


async def _handle_dependency_remove(
    websocket: WebSocket,
    session: NotebookSession,
    payload: dict[str, Any],
    execution_state: dict[str, Any],
    notebook_id: str,
) -> None:
    """Handle dependency_remove — remove a package and broadcast the change."""
    from strata.notebook.dependencies import remove_dependency
    from strata.notebook.routes import validate_package_name

    package = payload.get("package", "")
    if not package:
        execution_state["sequence"] += 1
        await websocket.send_text(
            _json_encode({
                "type": "error",
                "seq": execution_state["sequence"],
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"error": "Missing 'package' in payload"},
            })
        )
        return

    try:
        package = validate_package_name(package)
    except ValueError as e:
        execution_state["sequence"] += 1
        await websocket.send_text(
            _json_encode({
                "type": "error",
                "seq": execution_state["sequence"],
                "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "payload": {"error": str(e)},
            })
        )
        return

    result = remove_dependency(session.path, package)

    if result.lockfile_changed:
        await session.on_dependencies_changed()

    execution_state["sequence"] += 1
    seq = execution_state["sequence"]

    msg = {
        "type": "dependency_changed",
        "seq": seq,
        "ts": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "payload": {
            "action": "remove",
            "package": result.package,
            "success": result.success,
            "error": result.error,
            "lockfile_changed": result.lockfile_changed,
            "dependencies": [
                {"name": d.name, "version": d.version, "specifier": d.specifier}
                for d in result.dependencies
            ],
        },
    }
    await _broadcast_message(notebook_id, msg)


async def _broadcast_message(
    notebook_id: str, message: dict[str, Any]
) -> None:
    """Broadcast a message to all connected clients for a notebook."""
    connections = _notebook_connections.get(notebook_id, [])
    if not connections:
        return

    message_text = _json_encode(message)
    disconnected = []

    for ws in connections:
        try:
            await ws.send_text(message_text)
        except Exception:
            disconnected.append(ws)

    # Clean up disconnected clients
    for ws in disconnected:
        if ws in connections:
            connections.remove(ws)
