# @name Dynamic Markdown via display()
#
# The Markdown(...) wrapper is auto-injected into every cell namespace,
# so a Python cell can emit formatted prose without importing anything.
# This is the path that lets a code cell produce a "report" alongside
# numeric output.

import datetime as _dt

now = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

display(
    Markdown(
        f"""
## Generated at runtime

This block was rendered from a **Python cell**, not a markdown cell.

- Current time: `{now}`
- Cell language: `python`
- Display path: `Markdown(text) → text/markdown artifact → renderer`

> The same renderer powers both surfaces, so anything the markdown
> showcase renders correctly will also work here.
"""
    )
)
