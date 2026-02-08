2026-02-08: marimo `mo.ui.run_button`, `mo.ui.dropdown`, `mo.ui.date`, and `mo.ui.table` are available and suitable for reactive proof controls + spot-check tables.
2026-02-08: In marimo `@app.cell` scripts, a name is only available in later cells if it is returned from an earlier cell and included as a parameter in the dependent cell’s function signature (otherwise you’ll hit `NameError`).
2026-02-08: Avoid reusing generic variable names across cells (`controls`, `view`, `df`, etc.); marimo may skip cells that “redefine variables from other cells”, so prefer unique names per cell or wrap scratch logic in a helper function and return outputs.
2026-02-08: `mo.ui.dropdown(options=<dict>)` expects `value` to be one of the dict’s keys (the displayed option name), while `dropdown.value` evaluates to the mapped dict value.
2026-02-08: marimo treats names prefixed with `_` as cell-local; don’t use `_helper` names if you need to pass them between cells via returns/parameters.
2026-02-08: `mo.ui.date(start=..., stop=..., value=...)` accepts ISO date strings or `datetime.date` values; `date_picker.value` returns a `datetime.date`.
