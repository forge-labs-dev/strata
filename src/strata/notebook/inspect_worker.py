#!/usr/bin/env python3
"""Inspect worker script that runs interactive REPL for artifact exploration.

NOTE: This module is not used in production. The WebSocket handler uses
inspect_repl.py instead. Kept for potential future use.

This script:
1. Receives input artifact information via stdin
2. Loads artifacts into the namespace
3. Sends a 'ready' signal
4. Accepts eval expressions via stdin
5. Evaluates and returns results via stdout

It runs in the notebook's venv.
"""

import json
import sys
from typing import Any


def load_artifact(artifact_uri: str) -> Any:
    """Load an artifact from a URI.

    For v1, this is a simplified version that loads from the artifact
    store. In a real deployment, this would use the artifact manager.

    Args:
        artifact_uri: URI like strata://artifact/{id}@v={version}

    Returns:
        The loaded artifact data
    """
    # For now, return a placeholder that indicates the artifact wasn't loaded
    # In a real implementation, this would integrate with the artifact store
    return None


def main() -> None:
    """Main REPL loop."""
    try:
        # Read input configuration
        config_line = sys.stdin.readline()
        if not config_line:
            sys.exit(1)

        config = json.loads(config_line)
        variables = config.get("variables", {})

        # Initialize namespace with standard imports
        namespace: dict[str, Any] = {
            "__name__": "__console__",
            "__doc__": None,
        }

        # Try to add common imports
        try:
            import pandas as pd
            namespace["pd"] = pd
        except ImportError:
            pass

        try:
            import numpy as np
            namespace["np"] = np
        except ImportError:
            pass

        try:
            import pyarrow as pa
            namespace["pa"] = pa
        except ImportError:
            pass

        # Load artifacts into namespace
        for var_name, artifact_uri in variables.items():
            try:
                artifact_data = load_artifact(artifact_uri)
                if artifact_data is not None:
                    namespace[var_name] = artifact_data
            except Exception as e:
                # Log error but continue
                print(f"Warning: Failed to load {var_name}: {e}", file=sys.stderr)

        # Send ready signal
        print("ready", flush=True)

        # Main REPL loop
        while True:
            try:
                # Read request
                req_line = sys.stdin.readline()
                if not req_line:
                    break

                req = json.loads(req_line)
                expr = req.get("expr", "")

                if not expr:
                    continue

                # Evaluate expression
                result_value = eval(expr, namespace)
                result_str = repr(result_value)
                result_type = type(result_value).__name__

                # Return result
                response = {
                    "result": result_str,
                    "type": result_type,
                    "error": None,
                }
                print(json.dumps(response), flush=True)

            except SyntaxError as e:
                response = {
                    "result": None,
                    "type": None,
                    "error": f"SyntaxError: {str(e)}",
                }
                print(json.dumps(response), flush=True)

            except Exception as e:
                response = {
                    "result": None,
                    "type": None,
                    "error": f"{type(e).__name__}: {str(e)}",
                }
                print(json.dumps(response), flush=True)

    except Exception as e:
        print(f"fatal: {e}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
