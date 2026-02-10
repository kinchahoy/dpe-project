import marimo

__generated_with = "0.19.9"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    import os
    from datetime import date
    from pathlib import Path
    from typing import Any

    import marimo as mo

    from simple_agent_framework.db import query_all, query_one, resolve_vending_db_paths
    from simple_agent_framework.script_context import (
        SCRIPT_CONTEXT_INPUT_NAMES,
        build_script_context,
    )
    from simple_agent_framework.script_registry import discover_scripts
    from simple_agent_framework.script_runner import run_script

    return (
        Any,
        Path,
        SCRIPT_CONTEXT_INPUT_NAMES,
        build_script_context,
        date,
        discover_scripts,
        json,
        mo,
        os,
        query_all,
        query_one,
        resolve_vending_db_paths,
        run_script,
    )


@app.cell
def _(Any, query_one):
    def coerce_jsonable(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): coerce_jsonable(v) for k, v in value.items()}
        if isinstance(value, list):
            return [coerce_jsonable(v) for v in value]
        if isinstance(value, tuple):
            return [coerce_jsonable(v) for v in value]
        if isinstance(value, (int, float, str, bool)) or value is None:
            return value
        return str(value)

    def resolve_currency(dbs, location_id: int) -> str:
        row = query_one(
            dbs.observed_db,
            """
            SELECT COALESCE(
                (
                    SELECT currency
                    FROM "transaction"
                    WHERE location_id = ?
                    ORDER BY date DESC, id DESC
                    LIMIT 1
                ),
                'USD'
            ) AS currency
            """.strip(),
            (location_id,),
            readonly=True,
        )
        if row is None:
            return "USD"
        return str(row.get("currency") or "USD")

    def sanitize_filename(value: str) -> str:
        return "".join(
            ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in value
        )

    return coerce_jsonable, resolve_currency, sanitize_filename


@app.cell
def _(discover_scripts, os, resolve_vending_db_paths):
    db_dir = os.environ.get("VENDING_DB_DIR")
    dbs = resolve_vending_db_paths(db_dir=db_dir)
    missing = [
        p for p in (dbs.facts_db, dbs.observed_db, dbs.analysis_db) if not p.exists()
    ]
    scripts = discover_scripts()
    script_map = {name: code for name, code in scripts}
    return dbs, missing, script_map


@app.cell
def _(date, dbs, missing, query_all, query_one, script_map):
    machine_options: dict[str, tuple[int, int]] = {}
    min_date = None
    max_date = None
    machine_context_message = None

    if missing:
        machine_context_message = (
            "Missing DB file(s): "
            + ", ".join(f"`{p}`" for p in missing)
            + "\n\nSet `VENDING_DB_DIR` to override."
        )
    else:
        machine_rows = query_all(
            dbs.facts_db,
            """
            SELECT m.id AS machine_id, m.name AS machine_name, m.location_id AS location_id, l.name AS location_name
            FROM machine m
            JOIN location l ON l.id = m.location_id
            ORDER BY l.name, m.name, m.id
            """.strip(),
            readonly=True,
        )
        for row in machine_rows:
            machine_id = int(row["machine_id"])
            location_id = int(row["location_id"])
            label = (
                f"{row['location_name']} | {row['machine_name']} "
                f"(machine_id={machine_id}, location_id={location_id})"
            )
            machine_options[label] = (location_id, machine_id)

        tx_range = query_one(
            dbs.observed_db,
            'SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM "transaction"',
            readonly=True,
        )
        if (
            tx_range is None
            or tx_range["min_date"] is None
            or tx_range["max_date"] is None
        ):
            machine_context_message = f"No transactions found in `{dbs.observed_db}`"
        else:
            min_date = date.fromisoformat(str(tx_range["min_date"]))
            max_date = date.fromisoformat(str(tx_range["max_date"]))

        if not machine_options and machine_context_message is None:
            machine_context_message = f"No machines found in `{dbs.facts_db}`"
        if not script_map and machine_context_message is None:
            machine_context_message = "No sandbox scripts discovered."

    return machine_context_message, machine_options, max_date, min_date


@app.cell
def _(
    dbs, machine_context_message, machine_options, max_date, min_date, mo, script_map
):
    day_picker = None
    run_form = None
    machine_picker = None
    script_picker = None

    if machine_context_message is not None:
        controls_view = mo.md(machine_context_message)
    else:
        machine_picker = mo.ui.dropdown(
            options=machine_options,
            value=next(iter(machine_options.keys())),
            searchable=True,
            label="Machine",
            full_width=True,
        )
        day_picker = mo.ui.date(
            start=min_date,
            stop=max_date,
            value=max_date,
            label="Day",
        )
        script_picker = mo.ui.dropdown(
            options={name: name for name in sorted(script_map.keys())},
            value=next(iter(sorted(script_map.keys()))),
            searchable=True,
            label="Script",
            full_width=True,
        )
        form_body = mo.md(
            """
            {day_picker}
            {script_picker}
            {machine_picker}
            """
        ).batch(
            day_picker=day_picker,
            script_picker=script_picker,
            machine_picker=machine_picker,
        )
        run_form = mo.ui.form(
            form_body,
            bordered=False,
            submit_button_label="Run proof",
            clear_on_submit=False,
        )
        controls_view = mo.vstack(
            [
                mo.md(
                    "\n".join(
                        [
                            "## PROOF: Script Context + Script Response",
                            "",
                            "This notebook proves the end-to-end flow for one machine and day:",
                            "1. resolve inputs from UI selection",
                            "2. build script context",
                            "3. run a sandbox script",
                            "4. inspect the emitted alerts",
                            "5. persist a proof artifact",
                            "",
                            "### Data Sources",
                            f"Facts DB: `{dbs.facts_db}`",
                            f"Observed DB: `{dbs.observed_db}`",
                            f"Analysis DB: `{dbs.analysis_db}`",
                        ]
                    )
                ),
                mo.md("### Step 1: Choose inputs and submit"),
                mo.md(
                    "Select a day, script, and machine. The proof only runs when you click **Run proof**."
                ),
                run_form,
            ]
        )

    controls_view
    return day_picker, machine_picker, run_form, script_picker


@app.cell
def _(
    build_script_context,
    dbs,
    resolve_currency,
    run_form,
    run_script,
    script_map,
):
    alerts = None
    context = None
    currency = None
    execution_message = "Submit the form to run the proof."
    selected_day = None
    selected_location_id = None
    selected_machine_id = None
    selected_script = None

    if run_form is not None and run_form.value is not None:
        submitted = run_form.value
        selected_day = submitted.get("day_picker")
        selection = submitted.get("machine_picker")
        selected_script = submitted.get("script_picker")

        if (
            selected_day is not None
            and selection is not None
            and selected_script is not None
        ):
            selected_location_id, selected_machine_id = selection
            currency = resolve_currency(dbs, int(selected_location_id))
            context = build_script_context(
                dbs=dbs,
                as_of_date=selected_day,
                location_id=int(selected_location_id),
                machine_id=int(selected_machine_id),
                currency=currency,
            )
            script_code = script_map[selected_script]
            alerts = run_script(
                script_name=selected_script,
                code=script_code,
                context=context,
            )
            execution_message = "Proof executed successfully."
        else:
            execution_message = (
                "Incomplete form submission; choose day, machine, and script."
            )

    return (
        alerts,
        context,
        currency,
        execution_message,
        selected_day,
        selected_location_id,
        selected_machine_id,
        selected_script,
    )


@app.cell
def _(
    Path,
    SCRIPT_CONTEXT_INPUT_NAMES,
    alerts,
    coerce_jsonable,
    context,
    currency,
    execution_message,
    json,
    mo,
    sanitize_filename,
    selected_day,
    selected_location_id,
    selected_machine_id,
    selected_script,
):
    if (
        alerts is None
        or context is None
        or selected_day is None
        or selected_script is None
        or selected_location_id is None
        or selected_machine_id is None
    ):
        proof_view = mo.vstack(
            [
                mo.md("### Step 2: Execute proof"),
                mo.md(execution_message),
            ]
        )
    else:
        context_jsonable = coerce_jsonable(context)
        alerts_jsonable = coerce_jsonable(alerts)
        context_json = json.dumps(context_jsonable, indent=2, sort_keys=True)
        alerts_json = json.dumps(alerts_jsonable, indent=2, sort_keys=True)

        step_lines = [
            "PROOF START",
            "Step 1/6: Resolve selection",
            f"  day={selected_day.isoformat()}",
            f"  location_id={selected_location_id}",
            f"  machine_id={selected_machine_id}",
            f"  script={selected_script}",
            "Step 2/6: Resolve currency",
            f"  currency={currency}",
            "Step 3/6: Build script context",
            f"  context_keys={len(context.keys())}",
            f"  contract_match={set(context.keys()) == set(SCRIPT_CONTEXT_INPUT_NAMES)}",
            f"  ctx_keys={len((context.get('ctx') or {}).keys())}",
            f"  inventory_snapshot_date={(context.get('ctx') or {}).get('inventory', {}).get('snapshot_date')}",
            f"  inventory_rows={len((context.get('ctx') or {}).get('inventory', {}).get('by_ingredient') or [])}",
            f"  observed_days={len([d for d in ((context.get('ctx') or {}).get('days') or []) if d.get('kind') == 'observed'])}",
            f"  predicted_days={len([d for d in ((context.get('ctx') or {}).get('days') or []) if d.get('kind') == 'predicted'])}",
            "Step 4/6: Run script in sandbox",
            f"  emitted_alerts={len(alerts)}",
            "Step 5/6: Persist proof log",
        ]

        safe_script = sanitize_filename(selected_script)
        log_dir = Path(__file__).resolve().parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / (
            f"prove_script_context_and_response_"
            f"{selected_day.isoformat()}_loc{selected_location_id}_machine{selected_machine_id}_{safe_script}.log"
        )
        with log_path.open("w", encoding="utf-8") as handle:
            handle.write("\n".join(step_lines) + "\n")
            handle.write("\nContext JSON\n")
            handle.write(context_json + "\n")
            handle.write("\nScript Response JSON\n")
            handle.write(alerts_json + "\n")

        summary_rows = [
            {"key": "day", "value": selected_day.isoformat()},
            {"key": "location_id", "value": selected_location_id},
            {"key": "machine_id", "value": selected_machine_id},
            {"key": "script", "value": selected_script},
            {"key": "currency", "value": currency},
            {"key": "context_keys", "value": len(context.keys())},
            {
                "key": "contract_match",
                "value": set(context.keys()) == set(SCRIPT_CONTEXT_INPUT_NAMES),
            },
            {"key": "emitted_alerts", "value": len(alerts)},
            {"key": "log_path", "value": str(log_path)},
        ]
        step_lines.append("Step 6/6: Render proof output")

        proof_view = mo.vstack(
            [
                mo.md("### Step 2: Execution trace"),
                mo.md(
                    "These are the exact runtime steps taken for the selected day, machine, and script."
                ),
                mo.md("```\n" + "\n".join(step_lines) + "\n```"),
                mo.md("### Step 3: Validate high-level outcomes"),
                mo.md(
                    "This summary confirms the selection, context contract match, and alert count."
                ),
                mo.ui.table(data=summary_rows),
                mo.md("### Step 4: Inspect context sent to script"),
                mo.md("Review the full JSON payload passed into the sandbox script."),
                mo.md(f"```json\n{context_json}\n```"),
                mo.md("### Step 5: Inspect script response"),
                mo.md("Review the exact JSON response emitted by the script."),
                mo.md(f"```json\n{alerts_json}\n```"),
                mo.md("### Step 6: Proof artifact"),
                mo.md(
                    f"The full trace and JSON payloads were persisted to `{log_path}`."
                ),
            ]
        )

    proof_view


if __name__ == "__main__":
    app.run()
