---
name: marimo-notebook
description: >-
  Writes and edits marimo reactive Python notebooks. Use when creating, refactoring, or debugging marimo
  notebooks, or when the user mentions marimo, reactive notebooks, or .py notebook files.
---

# Marimo Notebook

Marimo notebooks are reactive Python notebooks where cells form a DAG based on variable dependencies. Execution order is determined by data flow, not cell position. They work differently from jyupiter notebooks.

## Critical rules

These rules prevent compile errors and broken reactivity. Follow them in every cell.

1. **One definition per global variable** -- A global name must be defined in exactly one cell. Use distinct names for each processing stage (e.g., `raw_data`, `cleaned_data`).

2. **Underscore prefix for cell-local variables** -- `_`-prefixed names are cell-local, invisible to other cells, and safe to reuse across cells.

3. **Underscore loop iterators** -- Always prefix loop variables (`_i`, `_row`, `_item`) to prevent global scope leaks.

4. **No cross-cell mutation** -- Marimo does not track mutations (lists, dicts, attrs) across cells. Create a new variable or mutate within a single cell.

5. **Encapsulate bulk locals** -- Wrap many temporaries in a `def _(): ...` function, call it, and assign only the result to a global.

6. Where it makes sense use mo.md() etc. to break up code blocks with explanations

## Quick reference

### UI and reactivity

- Assign UI elements to globals to trigger reactivity: `slider = mo.ui.slider(0, 100)`
- Access state with `element.value`
- Batch inputs with `mo.ui.form()` to re-run only on submit
- In loops with callbacks, bind via default args: `lambda _, i=i: ...`

### Performance

- `mo.stop(condition)` -- halt cell execution conditionally
- `@mo.cache` -- in-memory memoization
- `mo.persistent_cache()` -- disk-based cache surviving restarts
- `mo.lazy()` -- defer expensive UI rendering

### Output

- Last expression in a cell is its output
- Return figure objects instead of calling `plt.show()`
- Use `mo.md()` for rich text, `mo.hstack()`/`mo.vstack()` for layout
- Use `mo.sql()` for SQL queries (returns Polars DataFrames)

## Project conventions

- **DataFrames:** `polars` (not `pandas`)
- **Logging:** `loguru`
- **Packages:** `uv` (`uv add`, `uv run`)
- **Environment:** `dotenv.load_dotenv(dotenv.find_dotenv(usecwd=True))`

## Additional resources

- For code examples and common patterns, see [examples.md](examples.md)
- For detailed UI, caching, and SQL reference, see [reference.md](reference.md)
