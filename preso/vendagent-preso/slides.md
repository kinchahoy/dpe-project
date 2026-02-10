---
theme: dracula
title: Vendagent
info: |
  Vendagent – agentic operations dashboard for vending machines.
  Predictable AI enabled Continous Improvement
layout: cover
class: text-center
transition: slide-left
mdc: true
---

<style>
.accent-bar {
  width: 40px;
  height: 3px;
  background: #bd93f9;
  border-radius: 2px;
}
.slide-subtitle {
  color: #6272a4;
  font-size: 1.05rem;
  font-weight: 400;
}
.label-sm {
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #6272a4;
}
.sep {
  width: 100%;
  height: 1px;
  background: #44475a;
}
.item-title {
  font-weight: 600;
  color: #f8f8f2;
  font-size: 0.95rem;
}
.item-desc {
  color: #8b95b0;
  font-size: 0.85rem;
  line-height: 1.55;
}
.tag {
  display: inline-block;
  padding: 0.2rem 0.65rem;
  font-size: 0.7rem;
  font-weight: 600;
  letter-spacing: 0.03em;
  border-radius: 3px;
}
.tag-purple {
  background: rgba(189, 147, 249, 0.15);
  color: #bd93f9;
}
.tag-muted {
  background: rgba(98, 114, 164, 0.2);
  color: #6272a4;
}
</style>

# Vendagent

Agentic operations dashboard for vending machines

<div class="pt-6">
  <span class="tag tag-purple">Simple-Agentic-Framework Demo</span>
</div>

<div class="abs-br m-6 text-xs tracking-wide" style="color: #6272a4;">
  Pydantic AI · FastAPI · SvelteKit
</div>

---
layout: two-cols-header
transition: slide-left
---

# Vendagent

::left::

<div class="pr-10">

<div class="label-sm mb-3">Challenge</div>

<div class="text-base font-medium leading-relaxed" style="color: #f8f8f2;">
Enable operations managers to make better, faster decisions across their vending machine fleet.
</div>

<div class="mt-5 item-desc">
Today, managers react to problems after they happen. They need tools that help them act <strong style="color: #50fa7b;">before</strong> issues impact revenue.
</div>

</div>

::right::

<div class="pl-6" style="border-left: 1px solid #44475a;">

<div class="label-sm mb-4">Approach</div>

<div class="space-y-4">
  <div>
    <div class="item-title">Proactive Actions</div>
    <div class="item-desc mt-1">Surface decisions for managers — don't wait for problems to escalate</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Predictable Behaviors</div>
    <div class="item-desc mt-1">Reliable, repeatable agent-generated analysis — not ad-hoc LLM responses</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Controlled Improvement</div>
    <div class="item-desc mt-1">Understandable process improvements that managers can refine over time</div>
  </div>
</div>

</div>

---
transition: slide-left
---

# Approach

<div class="grid grid-cols-3 gap-10 pt-2">

<div>
  <div class="accent-bar mb-4"></div>
  <div class="item-title !text-base mb-2">Proactive Actions</div>
  <div class="item-desc">Ingests transactions and processes developments to generate actionable alerts — before issues surface.</div>
</div>

<div>
  <div class="accent-bar mb-4"></div>
  <div class="item-title !text-base mb-2">Predictable Behaviors</div>
  <div class="item-desc">Actions are generated from sandboxed, replayable agent code — not one-off, non-deterministic LLM responses.</div>
</div>

<div>
  <div class="accent-bar mb-4"></div>
  <div class="item-title !text-base mb-2">Understandable Improvement</div>
  <div class="item-desc">Managers can continuely refine behaviors with AI support to generate targetted refinements to the underlying logic.</div>
</div>

</div>

---
layout: two-cols-header
transition: slide-left
---

# Current Scope

<div class="slide-subtitle -mt-2">Proof of concept delivery</div>

::left::

<div class="pr-10">

<div class="label-sm mb-3">What We Built</div>

<div class="text-sm leading-relaxed" style="color: #f8f8f2;">
A working end-to-end system demonstrating agentic operations management — from data ingestion through manager decision-making.
</div>

<div class="mt-5">
  <span class="tag tag-purple">4 core capabilities</span>
</div>

</div>

::right::

<div class="pl-6" style="border-left: 1px solid #44475a;">

<div class="label-sm mb-4">4 core capabilities</div>

<div class="space-y-4">
  <div>
    <div class="item-title">Core Agentic Engine</div>
    <div class="item-desc mt-1">Clean, extensible architecture for running predictable agentic scripts against transactions </div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Extensible Ingestion Pipeline</div>
    <div class="item-desc mt-1">Expand to new machines, locations, and products with minimal configuration</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Safe Sandboxed Operations</div>
    <div class="item-desc mt-1">Leverages bleeding-edge Pydantic sandbox for secure code execution</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Core Interaction Loop</div>
    <div class="item-desc mt-1">Manager receives alerts, takes action, requests review, or suppresses — full feedback cycle</div>
  </div>
</div>

</div>


---
transition: slide-left
---

# Configured Alerts (Baseline)

<div class="slide-subtitle -mt-2 mb-6">Key scripts shipped in <code>scripts_sandbox/</code></div>

<div class="grid grid-cols-2 gap-8 pt-2">

<div>
  <div class="item-title">Service Due</div>
  <div class="item-desc mt-1">Triggers when a machine is within the service window based on <code>last_serviced_at</code>.</div>
  <div class="mt-2 text-xs" style="color: #6272a4;">
    Script: <code>service_due_predictor</code><br/>
    Alert type: <code>service_due</code> · Action: <code>SCHEDULE_SERVICE</code><br/>
    Knobs: <code>SERVICE_INTERVAL_DAYS</code>, <code>SERVICE_WINDOW_DAYS</code>
  </div>
</div>

<div>
  <div class="item-title">Machine Drop-Off</div>
  <div class="item-desc mt-1">Detects unit/revenue drops vs recent observed mean with top-product attribution.</div>
  <div class="mt-2 text-xs" style="color: #6272a4;">
    Script: <code>machine_dropoff_monitor</code><br/>
    Alert type: <code>machine_dropoff</code> · Action: <code>CHECK_MACHINE</code><br/>
    Knobs: <code>UNITS_DROP_PCT_TRIGGER</code>, <code>REVENUE_DROP_PCT_TRIGGER</code>, baseline minimums
  </div>
</div>

<div>
  <div class="item-title">Pricing Anomaly</div>
  <div class="item-desc mt-1">Flags undercharge drift vs <code>expected_price</code> across multiple events in the window.</div>
  <div class="mt-2 text-xs" style="color: #6272a4;">
    Script: <code>pricing_anomaly</code><br/>
    Alert type: <code>pricing_anomaly</code> · Action: <code>CHECK_MACHINE</code><br/>
    Knobs: <code>UNDERCHARGE_PCT_TRIGGER</code>, <code>MIN_UNDERCHARGE_COUNT</code>
  </div>
</div>

<div>
  <div class="item-title">Restock Risk</div>
  <div class="item-desc mt-1">Combines inventory fill and 3-day projected drawdown to surface restock urgency.</div>
  <div class="mt-2 text-xs" style="color: #6272a4;">
    Script: <code>restock_predictor</code><br/>
    Alert type: <code>restock_risk</code> · Actions: <code>RESTOCK_MACHINE</code>, <code>ORDER_INGREDIENTS</code><br/>
    Knobs: <code>FILL_PCT_TRIGGER</code>, <code>DAYS_COVER_TRIGGER</code>, <code>CRITICAL_FILL_PCT</code>
  </div>
</div>

<div>
  <div class="item-title">Sustained Demand Above Forecast</div>
  <div class="item-desc mt-1">Detects 7-day lift vs forecast and suggests a controlled price increase test.</div>
  <div class="mt-2 text-xs" style="color: #6272a4;">
    Script: <code>systematic_demand_change_watch</code><br/>
    Alert type: <code>systematic_demand_change</code> · Action: <code>ADJUST_PRICE</code><br/>
    Knobs: <code>LIFT_TRIGGER_PCT</code>, <code>MIN_PREDICTED_WEEK_UNITS</code>
  </div>
</div>

</div>

---
layout: two-cols-header
transition: slide-left
---

# Available Actions

::left::

<div class="pr-10">

<div class="label-sm mb-4">Current Capabilities</div>

<div class="space-y-4">
  <div>
    <div class="item-title">Initiate Machine Checkup</div>
    <div class="item-desc mt-1">Price errors, low credit card rate, low transaction volume</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Restock Machine</div>
    <div class="item-desc mt-1">Proactive restock recommendation for a target date</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Test New Price</div>
    <div class="item-desc mt-1">Propose and evaluate pricing changes</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Initiate Machine Service</div>
    <div class="item-desc mt-1">Propose optimal date for maintenance</div>
  </div>
</div>

</div>

::right::

<div class="pl-6" style="border-left: 1px solid #44475a;">

<div class="label-sm mb-4">Future Actions <span class="tag-muted tag ml-2">Roadmap</span></div>

<div class="space-y-4">
  <div>
    <div class="font-medium text-sm" style="color: #6272a4;">Introduce / Remove Product</div>
    <div class="item-desc mt-1">Identify gaps in product mix and recommend changes to optimize sales</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="font-medium text-sm" style="color: #6272a4;">Optimize Pricing Strategy</div>
    <div class="item-desc mt-1">Dynamic pricing recommendations to maximize revenue across locations</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="font-medium text-sm" style="color: #6272a4;">Fleet-wide Analytics</div>
    <div class="item-desc mt-1">Cross-machine pattern detection and comparative performance insights</div>
  </div>
</div>

</div>

---
layout: two-cols-header
transition: slide-left
---

# Fine-Tuning with AI

<div class="slide-subtitle -mt-2">Use AI to refine behavior without losing determinism</div>

::left::

<div class="pr-10">

<div class="label-sm mb-4">Fast Controls</div>

<div class="space-y-4">
  <div>
    <div class="item-title">Snooze Noisy Identities</div>
    <div class="item-desc mt-1">“Remind me later” suppresses repeats for that machine + alert type</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Enable / Disable Scripts</div>
    <div class="item-desc mt-1">Turn off a detector globally while you iterate on logic</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Tune Behaviors with AI</div>
    <div class="item-desc mt-1">Adjust percent triggers, minimum baselines, windows, and severity thresholds</div>
  </div>
</div>

</div>

::right::

<div class="pl-6" style="border-left: 1px solid #44475a;">

<div class="label-sm mb-4">Agentic refinement workflow </div>

<div class="space-y-4">
  <div>
    <div class="item-title">1) Request improvement </div>
    <div class="item-desc mt-1">Example: “Raise the dropoff trigger from -30% to -40% and require 5 baseline days.”</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">2) Autogenerate next generation </div>
    <div class="item-desc mt-1">The model edits sandbox-compatible Python and saves a draft revision</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">3) Backtest Behavior </div>
    <div class="item-desc mt-1">Compare old vs new scripts over historical simulation days</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">4) AI auto-refinement loop </div>
    <div class="item-desc mt-1">Accept the draft (override) or revert to baseline at any time</div>
  </div>
</div>

</div>

---
layout: two-cols-header
transition: slide-left
---

# Safety + Determinism

<div class="slide-subtitle -mt-2 mb-8">Make changes without losing trust</div>

<div class="grid grid-cols-3 gap-10">

<div>
  <div class="label-sm mb-2">Sandbox</div>
  <div class="accent-bar mb-4"></div>
  <div class="text-xl font-semibold" style="color: #f8f8f2;">Pydantic Monty</div>
  <div class="item-desc mt-2">No imports, limited helpers, enforced output shape (<code>result = []</code>)</div>
</div>

<div>
  <div class="label-sm mb-2">Validation</div>
  <div class="accent-bar mb-4"></div>
  <div class="text-xl font-semibold" style="color: #f8f8f2;">Pydantic Models</div>
  <div class="item-desc mt-2">Every emitted alert is validated (<code>AlertPayload</code>) before persistence</div>
</div>

<div>
  <div class="label-sm mb-2">Guardrails</div>
  <div class="accent-bar mb-4"></div>
  <div class="text-xl font-semibold" style="color: #f8f8f2;">Backtest + Revert</div>
  <div class="item-desc mt-2">Draft edits are compared over history and can be reverted instantly</div>
</div>

</div>

---
transition: slide-left
---

# Tech Stack

<div class="slide-subtitle -mt-2 mb-8">Architecture overview</div>

<div class="grid grid-cols-3 gap-10">

<div>
  <div class="label-sm mb-2">AI & Agent Layer</div>
  <div class="accent-bar mb-4"></div>
  <div class="text-xl font-semibold" style="color: #f8f8f2;">Pydantic AI</div>
  <div class="item-desc mt-2">Structured review + script edit + final check workflows</div>
  <div class="mt-4">
    <span class="tag tag-purple">+ Pydantic Monty (sandbox)</span>
  </div>
</div>

<div>
  <div class="label-sm mb-2">API &amp; Storage</div>
  <div class="accent-bar mb-4"></div>
  <div class="text-xl font-semibold" style="color: #f8f8f2;">FastAPI</div>
  <div class="item-desc mt-2">Serves API + static dashboard UI</div>
  <div class="mt-4">
    <span class="tag tag-purple">SQLModel · SQLite</span>
  </div>
</div>

<div>
  <div class="label-sm mb-2">Frontend</div>
  <div class="accent-bar mb-4"></div>
  <div class="text-xl font-semibold" style="color: #f8f8f2;">TypeScript</div>
  <div class="item-desc mt-2">Static UI (vanilla DOM) with modals and script tooling</div>
  <div class="mt-4">
    <span class="tag tag-purple">Lightweight & fast</span>
  </div>
</div>

</div>

---
layout: two-cols-header
transition: slide-left
---

# Future Potential

<div class="slide-subtitle -mt-2">Path to production</div>

::left::

<div class="pr-10">

<div class="label-sm mb-4">Infrastructure</div>

<div class="space-y-4">
  <div>
    <div class="item-title">Productionized Deployment</div>
    <div class="item-desc mt-1">CI/CD pipelines, monitoring, automated testing</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Production Database</div>
    <div class="item-desc mt-1">Upgrade from SQLite to PostgreSQL for scale</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Secure Auth &amp; Roles</div>
    <div class="item-desc mt-1">Location-level vs. overall ops permissions</div>
  </div>
    <div class="sep"></div>
  <div>
    <div class="item-title">End to end automation</div>
    <div class="item-desc mt-1"> Inject every transaction, push price/stock changes, review impact</div>
  </div>

</div>

</div>

::right::

<div class="pl-6" style="border-left: 1px solid #44475a;">

<div class="label-sm mb-4">Product</div>

<div class="space-y-4">
  <div>
    <div class="item-title">iOS / Android App</div>
    <div class="item-desc mt-1">Mobile-first experience for location managers on the go</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Dashboards &amp; Reports</div>
    <div class="item-desc mt-1">Comprehensive analytics across fleet performance, revenue, and operational efficiency</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Incorporate richer signals</div>
    <div class="item-desc mt-1">Integrate more signals to optimize revenue (time of day, weather etc.) </div>
  </div>
</div>

</div>

---
layout: two-cols-header
transition: slide-left
---

# Data + State Model

<div class="slide-subtitle -mt-2">What’s read-only vs what the app owns</div>

::left::

<div class="pr-10">

<div class="label-sm mb-3">Read-only DBs</div>

<div class="text-sm leading-relaxed" style="color: #f8f8f2;">
The engine reads three SQLite databases generated by the data pipeline.
</div>

<div class="mt-5 item-desc">
<strong style="color: #50fa7b;">Facts:</strong> <code>vending_machine_facts.db</code><br/>
<strong style="color: #50fa7b;">Observed:</strong> <code>vending_sales_observed.db</code><br/>
<strong style="color: #50fa7b;">Analysis:</strong> <code>vending_analysis.db</code>
</div>

</div>

::right::

<div class="pl-6" style="border-left: 1px solid #44475a;">

<div class="label-sm mb-4">Agent-owned DB</div>

<div class="space-y-4">
  <div>
    <div class="item-title">Engine Timeline</div>
    <div class="item-desc mt-1"><code>engine_state</code> tracks start/end/current simulation day</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Alerts + Suppression</div>
    <div class="item-desc mt-1"><code>alert</code> is upserted by identity; <code>alert_suppression</code> powers snooze</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Inventory State</div>
    <div class="item-desc mt-1"><code>inventory_state</code> advances day-by-day using forecast drawdown and restock actions</div>
  </div>
  <div class="sep"></div>
  <div>
    <div class="item-title">Script Revisions</div>
    <div class="item-desc mt-1">Baseline scripts + <code>script_revision</code> overrides that can be activated or reverted</div>
  </div>
</div>

</div>
