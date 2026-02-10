2026-02-08: marimo `mo.ui.run_button`, `mo.ui.dropdown`, `mo.ui.date`, and `mo.ui.table` are available and suitable for reactive proof controls + spot-check tables.
2026-02-08: In marimo `@app.cell` scripts, a name is only available in later cells if it is returned from an earlier cell and included as a parameter in the dependent cell’s function signature (otherwise you’ll hit `NameError`).
2026-02-08: Avoid reusing generic variable names across cells (`controls`, `view`, `df`, etc.); marimo may skip cells that “redefine variables from other cells”, so prefer unique names per cell or wrap scratch logic in a helper function and return outputs.
2026-02-08: `mo.ui.dropdown(options=<dict>)` expects `value` to be one of the dict’s keys (the displayed option name), while `dropdown.value` evaluates to the mapped dict value.
2026-02-08: marimo treats names prefixed with `_` as cell-local; don’t use `_helper` names if you need to pass them between cells via returns/parameters.
2026-02-08: `mo.ui.date(start=..., stop=..., value=...)` accepts ISO date strings or `datetime.date` values; `date_picker.value` returns a `datetime.date`.
2026-02-09: `uv run marimo check` flags nested branch-only display calls (`mo.md(...)`, `mo.vstack(...)`) as `branch-expression`; when rendering inside `if/else`, assign to `_ = ...` to keep notebooks lint-clean.
2026-02-10: pydantic_monty sandbox sorted() accepts only one positional iterable argument; key/reverse keyword args are unsupported and crash script execution.
2026-02-10: `slidev build --base /some/prefix/` rewrites output asset URLs to that prefix, which is required when embedding a built deck under a nested static path like `/static/about-deck/`.
