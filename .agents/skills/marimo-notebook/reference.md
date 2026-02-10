# Marimo Reference

## UI elements

### Core widgets

| Widget | Usage |
|--------|-------|
| `mo.ui.slider(start, stop, value=)` | Numeric slider |
| `mo.ui.number(start, stop, value=)` | Number input |
| `mo.ui.text(value=, placeholder=)` | Text input |
| `mo.ui.text_area(value=)` | Multi-line text |
| `mo.ui.checkbox(value=False)` | Boolean toggle |
| `mo.ui.dropdown(options, value=)` | Single select |
| `mo.ui.multiselect(options, value=[])` | Multi select |
| `mo.ui.radio(options, value=)` | Radio buttons |
| `mo.ui.button(label=, on_click=)` | Clickable button |
| `mo.ui.date(value=)` | Date picker |
| `mo.ui.file(filetypes=)` | File upload |
| `mo.ui.table(data, selection=)` | Interactive table |

### Composite widgets

- `mo.ui.form(element)` -- batch inputs, re-run on submit only
- `mo.ui.array([elements])` -- group multiple elements into a list
- `mo.ui.dictionary({key: element})` -- group elements into a dict
- `mo.ui.batch(**kwargs)` via `mo.md("...").batch(x=widget)` -- template-based grouping

### Reactivity rules

- A UI element **must** be assigned to a global variable to participate in the DAG.
- Read state with `element.value` in a downstream cell.
- `mo.ui.form()` suppresses reactivity until the user clicks submit.
- In loops, bind the current iteration value via default args to avoid closure bugs.

## Caching and performance

### `mo.stop(condition, output=None)`

Halts cell execution when `condition` is truthy. Optional `output` is rendered in place of the cell.

```python
mo.stop(data is None, mo.md("*Waiting for data...*"))
```

### `@mo.cache`

In-memory memoization. The decorated function re-runs only when its arguments change.

```python
@mo.cache
def expensive(x):
    return compute(x)
```

### `mo.persistent_cache(name=)`

Context manager for disk-based caching. Variables assigned inside the block are serialized and restored across notebook restarts.

```python
with mo.persistent_cache(name="my_cache"):
    result = expensive_computation()
```

### `mo.lazy(element)`

Defers rendering until the element scrolls into view. Useful for dashboards with many heavy outputs.

## Layout and display

| Function | Purpose |
|----------|---------|
| `mo.md(text)` | Render markdown |
| `mo.hstack(items, gap=)` | Horizontal layout |
| `mo.vstack(items, gap=)` | Vertical layout |
| `mo.tabs({label: content})` | Tabbed layout |
| `mo.accordion({label: content})` | Collapsible sections |
| `mo.callout(content, kind=)` | Callout box (info, warn, danger) |
| `mo.tree(data)` | Tree view of nested data |
| `mo.as_html(obj)` | Convert object to HTML output |
| `mo.image(src)` | Display image |
| `mo.plain_text(text)` | Plain text output |

## SQL integration

`mo.sql(query)` runs a SQL query and returns a Polars DataFrame. Python variables in scope can be referenced via f-strings. DataFrames in scope are accessible as table names.

```python
result = mo.sql(f"SELECT * FROM my_dataframe WHERE col > {threshold.value}")
```

## Notebook structure

### Cell anatomy

Each cell is a Python function decorated with `@app.cell`. The function signature declares the cell's dependencies (inputs), and global assignments in the body are the cell's outputs.

```python
@app.cell
def _(raw_data):  # depends on raw_data
    processed = raw_data.filter(pl.col("x") > 0)
    return (processed,)  # exports processed
```

### App creation

```python
import marimo as mo
app = mo.App(width="medium")  # "medium", "full", or "compact"
```
