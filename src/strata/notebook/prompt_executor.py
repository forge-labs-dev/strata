"""Executor for prompt-type notebook cells.

Resolves upstream artifacts to text, renders the prompt template,
calls the LLM, and stores the response as an artifact.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import TYPE_CHECKING, Any

from strata.notebook.llm import (
    LlmConfig,
    chat_completion,
    estimate_tokens,
    render_prompt_template,
)
from strata.notebook.prompt_analyzer import analyze_prompt_cell

if TYPE_CHECKING:
    from strata.notebook.session import NotebookSession

logger = logging.getLogger(__name__)


async def execute_prompt_cell(
    session: NotebookSession,
    cell_id: str,
    source: str,
    llm_config: LlmConfig,
    *,
    use_cache: bool = True,
) -> dict[str, Any]:
    """Execute a prompt cell and return the result.

    Returns a dict compatible with CellExecutionResult fields:
        success, outputs, stdout, stderr, error, cache_hit,
        duration_ms, execution_method, artifact_uri, mutation_warnings
    """
    start_time = time.time()
    analysis = analyze_prompt_cell(source)
    output_name = analysis.name

    # Resolve model config from annotations (override llm_config defaults)
    model = analysis.model or llm_config.model
    temperature = analysis.temperature if analysis.temperature is not None else 0.0
    max_tokens = analysis.max_tokens or llm_config.max_output_tokens
    output_type = analysis.output_type or "text"
    system_prompt = analysis.system_prompt

    # Load upstream variables from artifacts
    variables = _load_upstream_variables(session, cell_id)

    # Render template
    rendered = render_prompt_template(
        analysis.template_body,
        variables,
        max_tokens_per_var=2000,
    )

    if not rendered.strip():
        return _error_result("Prompt template is empty after rendering", start_time)

    # Compute provenance hash
    provenance_parts = [
        rendered,
        model,
        str(temperature),
        system_prompt or "",
        output_type,
    ]
    provenance_hash = hashlib.sha256("\n".join(provenance_parts).encode()).hexdigest()

    # Cache check
    artifact_mgr = session.get_artifact_manager()
    notebook_id = session.notebook_state.id
    canonical_id = f"nb_{notebook_id}_cell_{cell_id}_var_{output_name}"

    if use_cache:
        cached = artifact_mgr.find_cached(provenance_hash)
        if cached is not None:
            canonical = artifact_mgr.artifact_store.get_latest_version(canonical_id)
            if canonical is not None and canonical.provenance_hash == provenance_hash:
                # Cache hit
                duration_ms = (time.time() - start_time) * 1000
                blob = artifact_mgr.load_artifact_data(canonical.id, canonical.version)
                content_type = "json/object"
                try:
                    spec = json.loads(canonical.transform_spec or "{}")
                    content_type = spec.get("params", {}).get("content_type", content_type)
                except Exception:
                    pass

                value = _parse_output(blob, content_type)
                uri = f"strata://artifact/{canonical.id}@v={canonical.version}"

                return {
                    "success": True,
                    "outputs": {
                        output_name: {
                            "preview": _preview(value),
                            "content_type": content_type,
                            "bytes": len(blob),
                        },
                    },
                    "stdout": "",
                    "stderr": "",
                    "error": None,
                    "cache_hit": True,
                    "duration_ms": int(duration_ms),
                    "execution_method": "cached",
                    "artifact_uri": uri,
                    "mutation_warnings": [],
                }

    # Build messages
    messages: list[dict[str, str]] = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": rendered})

    # Estimate tokens
    input_tokens_est = estimate_tokens(rendered)
    if system_prompt:
        input_tokens_est += estimate_tokens(system_prompt)

    logger.info(
        "prompt_cell_execute %s: model=%s temp=%s est_tokens=%d output_type=%s",
        cell_id,
        model,
        temperature,
        input_tokens_est,
        output_type,
    )

    # Call LLM
    try:
        from strata.notebook.llm import LlmConfig as _LlmConfig

        call_config = _LlmConfig(
            base_url=llm_config.base_url,
            api_key=llm_config.api_key,
            model=model,
            max_output_tokens=max_tokens,
            timeout_seconds=llm_config.timeout_seconds,
        )
        result = await chat_completion(call_config, messages)
    except Exception as e:
        return _error_result(f"LLM call failed: {e}", start_time)

    # Parse output
    content = result.content
    if output_type == "json":
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code block
            import re

            m = re.search(r"```(?:json)?\s*\n([\s\S]*?)\n```", content)
            if m:
                try:
                    content = json.loads(m.group(1))
                except json.JSONDecodeError:
                    pass  # Keep as string

    # Serialize and store artifact
    content_type = "json/object"
    blob = json.dumps(content, indent=2, default=str).encode()

    try:
        var_provenance = hashlib.sha256(f"{provenance_hash}:{output_name}".encode()).hexdigest()

        version = artifact_mgr.artifact_store.create_artifact(
            artifact_id=canonical_id,
            provenance_hash=var_provenance,
            transform_spec=json.dumps(
                {
                    "executor": "prompt",
                    "params": {
                        "content_type": content_type,
                        "model": model,
                        "temperature": temperature,
                        "output_type": output_type,
                        "input_tokens": result.input_tokens,
                        "output_tokens": result.output_tokens,
                    },
                }
            ),
        )
        artifact_mgr.artifact_store.write_blob(canonical_id, version, blob)
        artifact_mgr.artifact_store.finalize_artifact(
            canonical_id,
            version,
            schema_json="{}",
            row_count=1,
            byte_size=len(blob),
        )
        artifact_uri = f"strata://artifact/{canonical_id}@v={version}"
    except Exception as e:
        logger.error("Failed to store prompt cell artifact: %s", e)
        artifact_uri = None

    duration_ms = (time.time() - start_time) * 1000

    return {
        "success": True,
        "outputs": {
            output_name: {
                "preview": _preview(content),
                "content_type": content_type,
                "bytes": len(blob),
            },
        },
        "stdout": "",
        "stderr": f"Model: {result.model} | Tokens: {result.input_tokens}→{result.output_tokens}",
        "error": None,
        "cache_hit": False,
        "duration_ms": int(duration_ms),
        "execution_method": "llm",
        "artifact_uri": artifact_uri,
        "mutation_warnings": [],
    }


def _load_upstream_variables(
    session: NotebookSession,
    cell_id: str,
) -> dict[str, Any]:
    """Load upstream variable values from artifacts."""
    variables: dict[str, Any] = {}
    cell = next((c for c in session.notebook_state.cells if c.id == cell_id), None)
    if cell is None:
        return variables

    artifact_mgr = session.get_artifact_manager()
    notebook_id = session.notebook_state.id

    for upstream_id in cell.upstream_ids:
        upstream_cell = next(
            (c for c in session.notebook_state.cells if c.id == upstream_id),
            None,
        )
        if upstream_cell is None:
            continue

        referenced_vars = [v for v in cell.references if v in upstream_cell.defines]

        for var_name in referenced_vars:
            canonical_id = f"nb_{notebook_id}_cell_{upstream_id}_var_{var_name}"
            try:
                artifact = artifact_mgr.artifact_store.get_latest_version(canonical_id)
                if artifact is None:
                    continue
                blob = artifact_mgr.load_artifact_data(canonical_id, artifact.version)
                content_type = "json/object"
                if artifact.transform_spec:
                    try:
                        spec = json.loads(artifact.transform_spec)
                        ct = spec.get("params", {}).get("content_type")
                        if ct:
                            content_type = ct
                    except (ValueError, KeyError):
                        pass
                variables[var_name] = _parse_output(blob, content_type)
            except Exception as e:
                logger.warning("Failed to load upstream %s: %s", var_name, e)

    return variables


def _parse_output(blob: bytes, content_type: str) -> Any:
    """Parse artifact blob back to a Python value."""
    if content_type == "arrow/ipc":
        try:
            import pyarrow as pa

            reader = pa.ipc.open_stream(blob)
            table = reader.read_all()
            try:
                return table.to_pandas()
            except Exception:
                return table
        except Exception:
            return blob.decode(errors="replace")
    elif content_type == "json/object":
        try:
            return json.loads(blob)
        except Exception:
            return blob.decode(errors="replace")
    elif content_type == "pickle/object":
        import pickle

        try:
            return pickle.loads(blob)  # noqa: S301
        except Exception:
            return blob.decode(errors="replace")
    return blob.decode(errors="replace")


def _preview(value: Any) -> Any:
    """Create a JSON-safe preview of a value."""
    if isinstance(value, str):
        return value[:500] if len(value) > 500 else value
    if isinstance(value, (int, float, bool, type(None))):
        return value
    if isinstance(value, (dict, list)):
        text = json.dumps(value, indent=2, default=str)
        return text[:500] if len(text) > 500 else text
    return str(value)[:500]


def _error_result(error: str, start_time: float) -> dict[str, Any]:
    """Build an error result dict."""
    return {
        "success": False,
        "outputs": {},
        "stdout": "",
        "stderr": "",
        "error": error,
        "cache_hit": False,
        "duration_ms": int((time.time() - start_time) * 1000),
        "execution_method": "llm",
        "artifact_uri": None,
        "mutation_warnings": [],
    }
