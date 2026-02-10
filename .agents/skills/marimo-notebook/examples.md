# Marimo Examples

## Variable naming across cells

```python
# Cell 1
raw_data = pl.read_csv("data.csv")

# Cell 2 -- distinct name, references Cell 1's raw_data
processed_data = raw_data.filter(pl.col("val") > 10)
```

## Encapsulating locals with `_()`

```python
# Cell 3
def _():
    _fig, _ax = plt.subplots()
    _ax.plot(processed_data["val"])
    return _fig

chart = _()
chart
```

## Reactive UI

```python
# Cell 4
threshold = mo.ui.slider(0, 100, value=50)
threshold

# Cell 5 -- re-runs automatically when threshold changes
filtered_df = processed_data.filter(pl.col("val") > threshold.value)
filtered_df
```

## Form with batched inputs

```python
# Cell 6
form = mo.ui.form(
    mo.md("""
    **Filters**

    {min_val}
    {max_val}
    """).batch(
        min_val=mo.ui.number(0, 100, label="Min"),
        max_val=mo.ui.number(0, 100, value=100, label="Max"),
    )
)
form

# Cell 7 -- only re-runs on form submit
mo.stop(form.value is None)
result = processed_data.filter(
    (pl.col("val") >= form.value["min_val"])
    & (pl.col("val") <= form.value["max_val"])
)
result
```

## Persistent caching

```python
# Cell 8
with mo.persistent_cache(name="expensive_computation"):
    model = train_model(processed_data)  # cached to disk
```

## SQL integration

```python
# Cell 9
min_threshold = 10
query_result = mo.sql(f"SELECT * FROM processed_data WHERE val > {min_threshold}")
query_result
```

## Loop with UI callbacks

```python
# Cell 10
buttons = mo.ui.array([
    mo.ui.button(
        label=f"Option {_i}",
        on_click=lambda _, i=_i: f"Selected {i}"
    )
    for _i in range(5)
])
mo.hstack(buttons)
```
