"""Test: 3-cell cascade after source edit.

Scenario:
  c1: x = 1
  c2: y = x + 1
  c3: print(y)

After running all three, edit c1 to x=2, then run c3.
Expected: cascade re-runs c1 → c2 → c3, prints "3".
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from tests.notebook.e2e_fixtures import (
    NotebookBuilder,
    WebSocketTestHelper,
    create_test_app,
    execute_cell_and_wait,
    open_notebook_session,
    ws_connect,
)


class TestCascadeAfterEdit:
    """Run c3 after editing c1 — should cascade through all three cells."""

    @pytest.fixture
    def setup(self):
        app = create_test_app()
        client = TestClient(app)
        with tempfile.TemporaryDirectory() as tmpdir:
            nb = (
                NotebookBuilder(Path(tmpdir))
                .add_cell("c1", "x = 1")
                .add_cell("c2", "y = x + 1", after="c1")
                .add_cell("c3", "print(y)", after="c2")
            )
            yield client, nb

    def test_cascade_reruns_all_after_edit(self, setup):
        """Edit c1, run c3 → cascade runs c1, c2, c3 and prints new value."""
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # 1. Run all cells in order so they become "ready"
                result1 = execute_cell_and_wait(ws, "c1")
                ws.clear()
                result2 = execute_cell_and_wait(ws, "c2")
                ws.clear()
                result3 = execute_cell_and_wait(ws, "c3")

                # Verify initial output: print(y) where y=1+1=2
                assert result3["type"] == "cell_output" or result3["type"] == "cell_status"
                # Check stdout from c3 messages
                c3_outputs = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                if c3_outputs:
                    assert "2" in c3_outputs[-1]["payload"].get("stdout", "")

                ws.clear()

                # 2. Edit c1 to x = 2
                ws.update_source("c1", "x = 2")
                # Wait for dag_update
                ws.receive_until("dag_update")
                ws.clear()

                # 3. Now run c3 — should trigger cascade (c1 → c2 → c3)
                result = execute_cell_and_wait(ws, "c3")

                # 4. Verify cascade happened — look for cascade_prompt
                cascade_msgs = ws.messages_of_type("cascade_prompt")
                assert len(cascade_msgs) > 0, (
                    "Expected cascade_prompt but got none. "
                    f"Message types: {[m['type'] for m in ws.messages]}"
                )

                # 5. Verify c3's output is now "3" (x=2, y=x+1=3)
                c3_outputs = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert len(c3_outputs) > 0, (
                    f"Expected cell_output for c3 but got none. "
                    f"Message types: {[m['type'] for m in ws.messages]}"
                )
                stdout = c3_outputs[-1]["payload"].get("stdout", "")
                assert "3" in stdout, f"Expected '3' in stdout but got: {stdout!r}"

    def test_cascade_works_when_cells_added_incrementally(self, setup):
        """Simulate UI workflow: run c1+c2, THEN add c3, then edit c1, run c3.

        This is the most common UI pattern — users add cells one at a time
        and run them as they go.  When c2 was first run, c3 didn't exist yet
        so consumed_variables[c2] was empty and y was never stored.
        The cascade must still work.
        """
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Run c1 and c2 BEFORE c3 exists in consumed_variables
                # (c3 already exists in the fixture, but simulate the scenario
                #  where c2 was first run when c3 wasn't yet referencing y)

                # Step 1: Run c1 and c2
                execute_cell_and_wait(ws, "c1")
                ws.clear()
                execute_cell_and_wait(ws, "c2")
                ws.clear()

                # Step 2: Run c3 — first run of c3, should resolve y
                result3 = execute_cell_and_wait(ws, "c3")
                c3_out = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert c3_out, "Expected cell_output for c3"
                assert "2" in c3_out[-1]["payload"].get("stdout", ""), (
                    f"Expected '2' but got {c3_out[-1]['payload'].get('stdout', '')!r}"
                )
                ws.clear()

                # Step 3: Edit c1 to x = 2
                ws.update_source("c1", "x = 2")
                ws.receive_until("dag_update")
                ws.clear()

                # Step 4: Run c3 — should cascade c1→c2→c3 and print 3
                result = execute_cell_and_wait(ws, "c3")

                cascade_msgs = ws.messages_of_type("cascade_prompt")
                assert len(cascade_msgs) > 0, (
                    f"Expected cascade. Types: {[m['type'] for m in ws.messages]}"
                )

                c3_out = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert c3_out, (
                    f"Expected cell_output for c3. Types: {[m['type'] for m in ws.messages]}"
                )
                stdout = c3_out[-1]["payload"].get("stdout", "")
                assert "3" in stdout, f"Expected '3' in stdout but got: {stdout!r}"

                # Also verify no cell_error messages
                errors = [
                    m for m in ws.messages
                    if m["type"] == "cell_error"
                ]
                assert not errors, (
                    f"Unexpected errors during cascade: {errors}"
                )

    def test_cascade_from_cold_start(self, setup):
        """After server restart (no prior execution), edit c1, run c3.

        All cells are idle with no artifact_uris. The cascade must run
        c1→c2→c3 from scratch and produce correct output.
        """
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Don't run any cells first — simulate fresh open
                # Edit c1 to x = 2
                ws.update_source("c1", "x = 2")
                ws.receive_until("dag_update")
                ws.clear()

                # Run c3 — should cascade c1→c2→c3
                result = execute_cell_and_wait(ws, "c3")

                cascade_msgs = ws.messages_of_type("cascade_prompt")
                assert len(cascade_msgs) > 0, (
                    f"Expected cascade. Types: {[m['type'] for m in ws.messages]}"
                )

                c3_out = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert c3_out, (
                    f"Expected cell_output for c3. Types: {[m['type'] for m in ws.messages]}"
                )
                stdout = c3_out[-1]["payload"].get("stdout", "")
                assert "3" in stdout, f"Expected '3' in stdout but got: {stdout!r}"

                # No errors should have occurred
                errors = [m for m in ws.messages if m["type"] == "cell_error"]
                assert not errors, f"Unexpected errors: {errors}"

    def test_staleness_propagates_to_downstream(self, setup):
        """After editing c1, c2 and c3 should not be 'ready'."""
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Run all cells
                execute_cell_and_wait(ws, "c1")
                ws.clear()
                execute_cell_and_wait(ws, "c2")
                ws.clear()
                execute_cell_and_wait(ws, "c3")
                ws.clear()

                # Verify all are ready
                for cell in session.notebook_state.cells:
                    assert cell.status == "ready", (
                        f"Cell {cell.id} should be ready, got {cell.status}"
                    )

                # Edit c1
                ws.update_source("c1", "x = 2")
                ws.receive_until("dag_update")

                # After the edit, c1 should NOT be ready, and c2/c3 should
                # also NOT be ready since their upstream changed
                c1 = next(c for c in session.notebook_state.cells if c.id == "c1")
                c2 = next(c for c in session.notebook_state.cells if c.id == "c2")
                c3 = next(c for c in session.notebook_state.cells if c.id == "c3")

                assert c1.status != "ready", f"c1 should be stale, got {c1.status}"
                assert c2.status != "ready", (
                    f"c2 should be stale (upstream c1 changed), got {c2.status}"
                )
                assert c3.status != "ready", (
                    f"c3 should be stale (upstream chain changed), got {c3.status}"
                )

    def test_artifact_store_state_after_cascade(self, setup):
        """Verify artifact store has correct artifacts after cascade.

        This is a regression test for the bug where c1's artifact was
        not stored during cascade, causing c2 to fail with
        'name x is not defined'.
        """
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # 1. Run all cells initially
                execute_cell_and_wait(ws, "c1")
                ws.clear()
                execute_cell_and_wait(ws, "c2")
                ws.clear()
                execute_cell_and_wait(ws, "c3")
                ws.clear()

                # Verify artifact store has c1's x artifact
                artifact_mgr = session.get_artifact_manager()
                notebook_id = session.notebook_state.id
                c1_x_id = f"nb_{notebook_id}_cell_c1_var_x"
                c2_y_id = f"nb_{notebook_id}_cell_c2_var_y"

                art_x_v1 = artifact_mgr.artifact_store.get_latest_version(c1_x_id)
                assert art_x_v1 is not None, (
                    f"Expected artifact for c1:x after initial run. "
                    f"artifact_id={c1_x_id}"
                )
                assert art_x_v1.state == "ready", (
                    f"Expected c1:x artifact to be ready, got {art_x_v1.state}"
                )

                art_y_v1 = artifact_mgr.artifact_store.get_latest_version(c2_y_id)
                assert art_y_v1 is not None, (
                    f"Expected artifact for c2:y after initial run. "
                    f"artifact_id={c2_y_id}"
                )

                # 2. Edit c1 and trigger cascade
                ws.update_source("c1", "x = 2")
                ws.receive_until("dag_update")
                ws.clear()

                # 3. Run c3 — triggers cascade c1→c2→c3
                result = execute_cell_and_wait(ws, "c3")

                # 4. Verify no errors
                errors = [m for m in ws.messages if m["type"] == "cell_error"]
                assert not errors, (
                    f"Unexpected errors during cascade: {errors}"
                )

                # 5. Verify artifact store has UPDATED c1:x artifact
                art_x_v2 = artifact_mgr.artifact_store.get_latest_version(c1_x_id)
                assert art_x_v2 is not None, (
                    f"Expected artifact for c1:x after cascade. "
                    f"artifact_id={c1_x_id}"
                )
                assert art_x_v2.state == "ready", (
                    f"Expected c1:x artifact to be ready after cascade, "
                    f"got {art_x_v2.state}"
                )
                # Should be a NEW version (different provenance from v1)
                assert art_x_v2.version >= art_x_v1.version, (
                    f"Expected new version for c1:x, "
                    f"got v{art_x_v2.version} (was v{art_x_v1.version})"
                )

                # 6. Verify c2:y also has updated artifact
                art_y_v2 = artifact_mgr.artifact_store.get_latest_version(c2_y_id)
                assert art_y_v2 is not None, (
                    f"Expected artifact for c2:y after cascade. "
                    f"artifact_id={c2_y_id}"
                )

                # 7. Verify cell artifact_uri fields are set
                c1 = next(c for c in session.notebook_state.cells if c.id == "c1")
                c2 = next(c for c in session.notebook_state.cells if c.id == "c2")
                assert c1.artifact_uri is not None, (
                    "c1.artifact_uri should be set after cascade"
                )
                assert c2.artifact_uri is not None, (
                    "c2.artifact_uri should be set after cascade"
                )

                # 8. Verify c3 printed "3"
                c3_outputs = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert c3_outputs, "Expected cell_output for c3"
                stdout = c3_outputs[-1]["payload"].get("stdout", "")
                assert "3" in stdout, f"Expected '3' in stdout but got: {stdout!r}"

    def test_upstream_rerun_on_missing_artifact(self, setup):
        """If an upstream artifact is missing, the upstream cell is re-run.

        Simulates the real-server failure mode: c1 runs and succeeds but
        its artifact is deleted/missing.  When c2 tries to resolve 'x' it
        should detect the gap, re-run c1 automatically, and proceed.
        """
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # Run c1 so it stores an artifact for x
                execute_cell_and_wait(ws, "c1")
                ws.clear()

                # Verify artifact exists
                artifact_mgr = session.get_artifact_manager()
                notebook_id = session.notebook_state.id
                c1_x_id = f"nb_{notebook_id}_cell_c1_var_x"
                art = artifact_mgr.artifact_store.get_latest_version(c1_x_id)
                assert art is not None, "Precondition: c1:x artifact must exist"

                # --- sabotage: delete the artifact row so it looks missing ---
                conn = artifact_mgr.artifact_store._get_connection()
                try:
                    conn.execute(
                        "DELETE FROM artifact_versions WHERE id = ?",
                        (c1_x_id,),
                    )
                    conn.commit()
                finally:
                    conn.close()

                # Confirm it's gone
                assert artifact_mgr.artifact_store.get_latest_version(c1_x_id) is None

                # Now run c2 — it should detect the missing artifact,
                # re-run c1, and succeed.
                execute_cell_and_wait(ws, "c2")

                # c2 should be ready (not error)
                c2 = next(
                    c for c in session.notebook_state.cells if c.id == "c2"
                )
                assert c2.status == "ready", (
                    f"c2 should be ready after auto-rerun of c1, "
                    f"got {c2.status}"
                )

                # c1:x artifact should now exist again (re-created by retry)
                art_after = artifact_mgr.artifact_store.get_latest_version(
                    c1_x_id
                )
                assert art_after is not None, (
                    "c1:x artifact should exist after auto-rerun"
                )

    def test_provenance_dedup_does_not_break_cascade(self, setup):
        """Provenance dedup must not poison the canonical artifact ID.

        Regression test for a bug where:
        1. An artifact with matching provenance exists under a different ID
           (e.g. from a previous cell layout or a notebook copy).
        2. ``finalize_artifact`` detects the duplicate and marks the
           canonical ``nb_..._cell_c1_var_x`` as "failed".
        3. ``find_by_provenance`` returns the OTHER artifact → cache hit.
        4. ``get_latest_version("nb_..._cell_c1_var_x")`` finds only the
           "failed" entry → None.
        5. Downstream cells fail with "name 'x' is not defined".

        The fix ensures:
        - ``store_cell_output`` forces the canonical version to "ready"
          even when provenance dedup would mark it as failed.
        - The cache-hit path validates that canonical IDs are resolvable
          before accepting the hit.
        """
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                artifact_mgr = session.get_artifact_manager()
                notebook_id = session.notebook_state.id

                # --- Poison the artifact store ---
                # Insert a "foreign" artifact with the SAME provenance that
                # c1 will compute, but under a different artifact ID.
                # This simulates leftover artifacts from a previous cell
                # layout or a copied notebook.

                # First, compute what c1's provenance will be.
                from strata.notebook.provenance import (
                    compute_provenance_hash,
                    compute_source_hash,
                )
                from strata.notebook.env import compute_lockfile_hash
                import hashlib

                source_hash = compute_source_hash("x = 1")
                env_hash = compute_lockfile_hash(session.path)
                cell_prov = compute_provenance_hash([], source_hash, env_hash)
                var_prov = hashlib.sha256(
                    f"{cell_prov}:x".encode()
                ).hexdigest()

                # Store a foreign artifact with matching provenance.
                from strata.artifact_store import TransformSpec

                foreign_id = f"nb_{notebook_id}_cell_GHOST_var_x"
                fv = artifact_mgr.artifact_store.create_artifact(
                    artifact_id=foreign_id,
                    provenance_hash=var_prov,
                    transform_spec=TransformSpec(
                        executor="notebook/cell@v1",
                        params={"content_type": "json/object"},
                        inputs=[],
                    ),
                )
                artifact_mgr.artifact_store.blob_store.write_blob(
                    foreign_id, fv, b"1",
                )
                artifact_mgr.artifact_store.finalize_artifact(
                    foreign_id, fv, "", 0, 1,
                )

                # Confirm the foreign artifact is findable by provenance.
                assert artifact_mgr.find_cached(var_prov) is not None
                # Confirm canonical ID does NOT exist yet.
                canonical_id = f"nb_{notebook_id}_cell_c1_var_x"
                assert (
                    artifact_mgr.artifact_store.get_latest_version(canonical_id)
                    is None
                )

                # --- Now run c3 — should cascade c1→c2→c3 ---
                result = execute_cell_and_wait(ws, "c3")

                cascade_msgs = ws.messages_of_type("cascade_prompt")
                assert len(cascade_msgs) > 0, (
                    f"Expected cascade. Types: {[m['type'] for m in ws.messages]}"
                )

                # c3 should print 2 (x=1, y=x+1=2)
                c3_out = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert c3_out, (
                    f"Expected cell_output for c3. "
                    f"Types: {[m['type'] for m in ws.messages]}"
                )
                stdout = c3_out[-1]["payload"].get("stdout", "")
                assert "2" in stdout, (
                    f"Expected '2' in stdout but got: {stdout!r}"
                )

                # Canonical artifact must now be ready.
                art = artifact_mgr.artifact_store.get_latest_version(
                    canonical_id,
                )
                assert art is not None, (
                    f"Canonical artifact {canonical_id} must be ready "
                    f"after cascade execution."
                )
                assert art.state == "ready"

                # No errors should have occurred.
                errors = [
                    m for m in ws.messages if m["type"] == "cell_error"
                ]
                assert not errors, f"Unexpected errors: {errors}"

    def test_rest_edit_then_ws_run_triggers_cascade(self, setup):
        """Edit via REST PUT, then run via WebSocket → cascade must trigger.

        Regression test: the frontend updates source via the REST API
        (PUT /v1/notebooks/{id}/cells/{cell_id}), NOT via the WebSocket
        ``cell_source_update`` message.  If the REST endpoint forgets
        to recompute staleness, all cells stay "ready" on the backend
        and the cascade planner says "no cascade needed".
        """
        client, nb = setup

        with open_notebook_session(client, nb.path) as (sid, session):
            with ws_connect(client, sid) as ws:
                # 1. Run all cells so they become "ready"
                execute_cell_and_wait(ws, "c1")
                ws.clear()
                execute_cell_and_wait(ws, "c2")
                ws.clear()
                execute_cell_and_wait(ws, "c3")
                ws.clear()

                # Verify all are ready
                for cell in session.notebook_state.cells:
                    assert cell.status == "ready", (
                        f"Cell {cell.id} should be ready, got {cell.status}"
                    )

                # 2. Edit c1 via REST API (not WebSocket!)
                resp = client.put(
                    f"/v1/notebooks/{sid}/cells/c1",
                    json={"source": "x = 2"},
                )
                assert resp.status_code == 200
                rest_data = resp.json()

                # Verify the REST response includes updated cell statuses
                assert "cells" in rest_data, (
                    "REST response should include 'cells' with statuses"
                )

                # Verify c1 is no longer "ready" on the backend
                c1 = next(
                    c for c in session.notebook_state.cells if c.id == "c1"
                )
                assert c1.status != "ready", (
                    f"c1 should be stale after REST edit, got {c1.status}"
                )

                # 3. Run c3 via WebSocket — should trigger cascade
                result = execute_cell_and_wait(ws, "c3")

                cascade_msgs = ws.messages_of_type("cascade_prompt")
                assert len(cascade_msgs) > 0, (
                    f"Expected cascade after REST edit. "
                    f"Types: {[m['type'] for m in ws.messages]}"
                )

                # 4. Verify c3 prints "3"
                c3_out = [
                    m for m in ws.messages
                    if m["type"] == "cell_output"
                    and m["payload"].get("cell_id") == "c3"
                ]
                assert c3_out, "Expected cell_output for c3"
                stdout = c3_out[-1]["payload"].get("stdout", "")
                assert "3" in stdout, (
                    f"Expected '3' in stdout but got: {stdout!r}"
                )
