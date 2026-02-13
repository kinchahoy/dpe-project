import marimo

__generated_with = "0.19.9"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        """
    # Legacy Notebook Notice

    `coffee_analysis_old_rawdata.py` previously explored raw Kaggle CSV files.

    It is intentionally retired to avoid confusion with the current split-DB architecture.

    Use these updated notebooks instead:

    - `coffee_analysis.py` for core ops overview
    - `price_inventory_analysis.py` for price anomalies and cash/card behavior
    - `prediction_accuracy.py` for projection-vs-actual performance
    - `inventory_predictor.py` for ingredient pressure and capacity cover
    - `price_drivers.py` for product-level off-price root-cause analysis
    """
    )
    return


if __name__ == "__main__":
    app.run()
