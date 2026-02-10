# Marimo Notebook Writing Guide

When writing or editing marimo notebooks, follow these rules strictly. Marimo notebooks are reactive — cells form a DAG based on variable dependencies, not page order.

---

## Critical Rules

### 1. One Definition Per Global Variable
Every global variable must be defined by **exactly one cell**. Two cells defining `df` is a compile error.

```python
# WRONG — two cells both define df
# Cell 1: df = load_data()
# Cell 2: df = transform(df)  # ERROR: multiple definitions

# CORRECT — use distinct names per cell
# Cell 1: raw_df = load_data()
# Cell 2: clean_df = transform(raw_df)
```

### 2. Underscore Prefix = Cell-Local
Variables starting with `_` are **local to that cell**. They cannot be read by other cells, and multiple cells can reuse the same `_`-prefixed names.

```python
# Cell 1:
_conn = sqlite3.connect("db.sqlite")
df = pl.read_database("SELECT * FROM t", _conn)
_conn.close()

# Cell 2:
_conn = sqlite3.connect("other.db")  # No conflict
other_df = pl.read_database("SELECT * FROM t2", _conn)
_conn.close()
```

### 3. Always Prefix Loop Iterators with `_`
Loop variables like `i`, `row`, `item` become globals after the loop. Two cells using `for i in ...` causes a multiple-definition error.

```python
# WRONG
for i in range(10): ...

# CORRECT
for _i in range(10): ...
for _row in df.iter_rows(named=True): ...
```

### 4. Never Mutate Global Variables Across Cells
Marimo does NOT track mutations. Appending to a list, modifying a dict, or setting an attribute in a different cell will NOT trigger reactivity.

```python
# WRONG — mutation in a different cell
# Cell 1: my_list = [1, 2, 3]
# Cell 2: my_list.append(4)  # Dependents will NOT re-run

# CORRECT — create new variables
# Cell 1: my_list = [1, 2, 3]
# Cell 2: extended = my_list + [4]

# CORRECT — chain DataFrame ops in one cell
df = (
    pl.read_database("SELECT * FROM t", conn)
    .with_columns(pl.col("date").cast(pl.Date))
    .filter(pl.col("amount") > 0)
)
```

### 5. Wrap Bulk Locals in a Function
When a cell needs many temporary variables, wrap in a function to avoid polluting the global namespace:

```python
def _():
    fig, ax = plt.subplots()
    ax.plot(data)
    ax.set_title("Revenue")
    plt.tight_layout()
    return fig

chart = _()
```

---

## UI Elements

### Assign UI Elements to Global Variables
UI interactions only trigger reactivity when the element is bound to a global variable.

```python
# Cell 1: Define and display
slider = mo.ui.slider(1, 100, value=50, label="Threshold")
slider

# Cell 2: React (auto-runs when slider changes)
filtered = df.filter(pl.col("amount") > slider.value)
filtered
```

### Use Forms to Batch Inputs
Wrap multiple inputs in `mo.ui.form()` so cells only re-run on submit, not every keystroke.

### Avoid `mo.state()` (99% of cases)
Rely on the reactive model with `.value`. Only use `mo.state()` for bidirectional UI sync or intentional cycles.

### Late-Binding Closure Fix
When creating UI elements in loops with callbacks:

```python
# WRONG — all reference final loop value
for i in range(5):
    buttons.append(mo.ui.button(on_change=lambda _: print(i)))

# CORRECT — bind with default arg
for i in range(5):
    buttons.append(mo.ui.button(on_change=lambda _, i=i: print(i)))
```

---

## Performance

- **`mo.stop()`** — halt cell execution conditionally (e.g., until a button is clicked)
- **`@mo.cache`** — in-memory memoization for expensive functions
- **`mo.persistent_cache()`** — disk cache that survives restarts (stored in `__marimo__/cache/`)
- **`mo.lazy()`** — defer rendering until element is visible (e.g., accordion panels)
- **Lazy runtime mode** — cells marked stale instead of auto-running; trigger manually
- **Disable cells** — temporarily prevent a cell and its descendants from running

---

## Output and Display

- The **last expression** of a cell is its visual output (rendered above the cell)
- `print()` goes to a separate console area, not the cell output
- Use `mo.md(f"Found **{len(df)}** rows")` for rich text
- Use `mo.as_html()` to wrap non-marimo objects
- Layout: `mo.hstack()`, `mo.vstack()`, `mo.accordion()`, `mo.ui.tabs()`

### Plotting
- **Matplotlib:** Return `fig` or `plt.gca()` as last expression. Do NOT call `plt.show()`. Call `plt.tight_layout()`.
- **Altair/Plotly:** Return chart object directly
- **Interactive selections:** `mo.ui.altair_chart(chart)` connects mouse selections back to Python

---

## SQL Integration

```python
result_df = mo.sql(f"SELECT * FROM {table_name} WHERE amount > {slider.value}")
```

- Results are Polars DataFrames (if polars installed)
- Reference Python DataFrames by variable name in SQL
- Escape literal braces with `{{...}}` (SQL cells are f-strings)

---

## Common Gotchas Checklist

- [ ] No `exec()`, `eval()`, or metaprogramming (breaks static analysis)
- [ ] No IPython magics (`%pip`, `!command`, `%%time`)
- [ ] No `plt.show()` — just return the figure
- [ ] Loop iterators prefixed with `_`
- [ ] No cross-cell mutation of lists, dicts, or object attributes
- [ ] `_unparsable_cell` in saved file = refactor needed (usually early `return` in cell body)
- [ ] Use `dotenv.load_dotenv(dotenv.find_dotenv(usecwd=True))` not bare `load_dotenv()`
- [ ] Outputs are NOT stored in the .py file — they're cached in `__marimo__/`

---

## Project Conventions (from CLAUDE.md)

- Use **polars** for DataFrames (not pandas)
- Use **loguru** for logging
- Use **uv** for package management (`uv add`, `uv run`)
- Run notebooks with `marimo edit notebook.py` or `uv run marimo edit notebook.py`
