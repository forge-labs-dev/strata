# Notebook Module Export Manual Test

This checklist covers the current cross-cell code reuse feature:

- top-level `def` / `class` are exported as synthetic notebook modules
- instances of exported classes are serialized as `module/cell-instance`
- flows should work in local, direct HTTP executor, and signed HTTP executor modes

## Setup

Start the Strata server in personal mode:

```bash
STRATA_DEPLOYMENT_MODE=personal uv run strata-server
```

Start the reference notebook executor in another terminal:

```bash
uv run uvicorn 'strata.notebook.remote_executor:create_notebook_executor_app' --factory --host 127.0.0.1 --port 8766
```

In the notebook UI, add:

- direct worker
  - name: `gpu-http`
  - backend: `executor`
  - url: `http://127.0.0.1:8766/v1/execute`
  - transport: `direct`
- signed worker
  - name: `gpu-http-signed`
  - backend: `executor`
  - url: `http://127.0.0.1:8766/v1/execute`
  - transport: `signed`
  - strata url: `http://127.0.0.1:8765`

## Baseline Function Export

Cell 1:

```python
import math

def area(r):
    return round(math.pi * r * r, 5)
```

Cell 2:

```python
result = area(2)
```

Expected:

- Cell 1 succeeds and stores `area` as reusable code
- Cell 2 succeeds and returns `12.56637`

## Baseline Class Export

Cell 1:

```python
class Box:
    def __init__(self, value):
        self.value = value
```

Cell 2:

```python
result = Box(7).value
```

Expected:

- Cell 1 succeeds and stores `Box` as reusable code
- Cell 2 succeeds and returns `7`

## Class Instance Across Three Cells

Cell 1:

```python
class Person:
    name = "John"
    age = 20

    def __str__(self):
        return f"{self.name}:{self.age}"
```

Cell 2:

```python
p = Person()
p
```

Cell 3:

```python
rendered = str(p)
rendered
```

Expected:

- Cell 2 succeeds and persists `p` as a class instance
- Cell 3 succeeds and returns `John:20`

## Invalidation

Edit Cell 1 in the previous example to:

```python
class Person:
    name = "Ada"
    age = 30

    def __str__(self):
        return f"{self.name}:{self.age}"
```

Expected:

- downstream cells become stale
- rerunning Cell 2 and Cell 3 produces `Ada:30`

## Execution Modes

Repeat the three-cell `Person` flow in each mode:

1. local
   - leave worker unset or `local`
2. direct remote
   - set cells to `gpu-http`
3. signed remote
   - set cells to `gpu-http-signed`

Expected:

- all three modes succeed
- direct and signed modes show remote worker metadata in the cell output/status
- signed mode keeps `remote_build_state = ready`

## Unsupported Cases

Top-level runtime state:

```python
x = 1

def add(y):
    return x + y
```

Top-level lambda:

```python
add = lambda y: y + 1
```

Top-level control flow:

```python
if True:
    def add(y):
        return y + 1
```

Expected:

- execution fails before downstream reuse
- the error explains why the code cannot be shared across cells yet
- messages should mention:
  - top-level runtime state
  - top-level lambdas
  - top-level control flow
