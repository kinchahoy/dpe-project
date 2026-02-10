"use strict";
const el = {
    statePill: document.getElementById("state-pill"),
    alertsMeta: document.getElementById("alerts-meta"),
    alertsList: document.getElementById("alerts-list"),
    dashMeta: document.getElementById("dash-meta"),
    dashboard: document.getElementById("dashboard"),
    scriptsMeta: document.getElementById("scripts-meta"),
    scriptsList: document.getElementById("scripts-list"),
    machineSalesModal: document.getElementById("machine-sales-modal"),
    machineSalesBackdrop: document.getElementById("machine-sales-backdrop"),
    machineSalesClose: document.getElementById("machine-sales-close"),
    machineSalesTitle: document.getElementById("machine-sales-title"),
    machineSalesMeta: document.getElementById("machine-sales-meta"),
    machineSalesBody: document.getElementById("machine-sales-body"),
    scriptCompareModal: document.getElementById("script-compare-modal"),
    scriptCompareBackdrop: document.getElementById("script-compare-backdrop"),
    scriptCompareClose: document.getElementById("script-compare-close"),
    scriptCompareTitle: document.getElementById("script-compare-title"),
    scriptCompareMeta: document.getElementById("script-compare-meta"),
    scriptCompareBody: document.getElementById("script-compare-body"),
    aboutModal: document.getElementById("about-modal"),
    aboutBackdrop: document.getElementById("about-backdrop"),
    aboutClose: document.getElementById("about-close"),
    aboutBtn: document.getElementById("about-btn"),
    skipDate: document.getElementById("skip-date"),
    resetBtn: document.getElementById("reset-btn"),
    runBtn: document.getElementById("run-btn"),
    nextBtn: document.getElementById("next-btn"),
    skipBtn: document.getElementById("skip-btn"),
    refreshBtn: document.getElementById("refresh-btn"),
    roleSelect: document.getElementById("role-select"),
};
const LOCATION_NAMES = {
    1: "Lviv, Ukraine",
    2: "San Francisco, CA",
};
const locationCurrencyMap = new Map();
const CURRENCY_LOCALES = {
    USD: "en-US",
    UAH: "uk-UA",
};
const currencyFormatterCache = new Map();
function currencyForLocation(locationId) {
    return locationCurrencyMap.get(locationId) ?? "USD";
}
function updateLocationCurrencyFromDashboard(dash) {
    if (!dash?.location_currency)
        return;
    Object.entries(dash.location_currency).forEach(([key, value]) => {
        if (!value)
            return;
        const numericKey = Number(key);
        if (Number.isNaN(numericKey))
            return;
        locationCurrencyMap.set(numericKey, value);
    });
}
function updateLocationCurrencyFromInventory(inv) {
    if (!inv)
        return;
    for (const loc of inv.locations) {
        if (loc.currency) {
            locationCurrencyMap.set(loc.location_id, loc.currency);
        }
    }
}
const ROLE_SCOPE = {
    overall_manager: { label: "Overall manager", locationIds: null },
    europe_manager: { label: "Europe manager", locationIds: [1] },
    us_manager: { label: "US manager", locationIds: [2] },
};
let state = null;
let rawAlerts = [];
let alerts = [];
let selectedAlertId = null;
let rawDashboard = null;
let rawInventory = null;
let activeRole = "overall_manager";
let rawScripts = [];
let selectedScriptName = null;
const aiReviewByAlertId = new Map();
const aiReviewBusyByAlertId = new Set();
const aiReviewErrorByAlertId = new Map();
const scriptDetailByName = new Map();
const scriptDraftByName = new Map();
const scriptInstructionByName = new Map();
const scriptBusyByName = new Map();
const scriptErrorByName = new Map();
let machineSalesModalState = null;
let scriptCompareModalState = null;
let aboutModalOpen = false;
/* ── Formatting helpers ─────────────────────────────────── */
const ALERT_TYPE_LABELS = {
    service_due: "Service Due",
    machine_dropoff: "Sales Drop-Off",
    pricing_anomaly: "Pricing Issue",
    systematic_demand_change: "Demand Above Forecast",
    low_stock: "Low Stock",
    refill_needed: "Refill Needed",
    ingredient_low: "Ingredient Running Low",
    revenue_drop: "Revenue Decline",
    high_revenue: "High Revenue",
};
const SEVERITY_LABELS = {
    LOW: "Low",
    MEDIUM: "Medium",
    HIGH: "High",
    CRITICAL: "Critical",
};
const EVIDENCE_KEY_LABELS = {
    machine_id: "Machine",
    location_id: "Location",
    product_id: "Product",
    ingredient_id: "Ingredient",
    current_revenue: "Current Revenue",
    previous_revenue: "Previous Revenue",
    revenue_change: "Revenue Change",
    revenue_change_pct: "Revenue Change %",
    avg_daily_revenue: "Avg. Daily Revenue",
    total_revenue: "Total Revenue",
    tx_count: "Number of Sales",
    days_since_service: "Days Since Service",
    last_service_date: "Last Serviced",
    price: "Price",
    avg_price: "Average Price",
    expected_price: "Expected Price",
    units_sold: "Units Sold",
    days_without_sale: "Days Without a Sale",
    last_sold_date: "Last Sold",
    stock_level: "Stock Level",
    threshold: "Threshold",
    drop_pct: "Drop Percentage",
    drop_percent: "Drop Percentage",
    period_days: "Period (Days)",
    current_period: "Current Period",
    previous_period: "Previous Period",
    currency: "Currency",
};
const ACTION_TYPE_LABELS = {
    schedule_service: "Schedule a service visit",
    adjust_price: "Adjust the price",
    remove_product: "Consider removing this product",
    restock: "Restock this item",
    investigate: "Investigate further",
    review_pricing: "Review the pricing",
    review_menu: "Review the product menu",
    notify_manager: "Notify the manager",
    refill: "Refill the machine",
};
function formatAlertType(raw) {
    return ALERT_TYPE_LABELS[raw] ?? raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
function formatSeverity(raw) {
    return SEVERITY_LABELS[raw] ?? raw;
}
function formatDate(iso) {
    try {
        const d = new Date(iso + (iso.includes("T") ? "" : "T00:00:00"));
        return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
    }
    catch {
        return iso;
    }
}
function formatDateRange(startIso, endIso) {
    return `${formatDate(startIso)} \u2013 ${formatDate(endIso)}`;
}
function formatEvidenceKey(raw) {
    return EVIDENCE_KEY_LABELS[raw] ?? raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
function formatEvidenceValue(key, value, currency) {
    if (value === null || value === undefined)
        return "\u2014";
    if (typeof value === "number") {
        if (key.includes("revenue") || key.includes("price") || key === "amount" || key === "current_revenue" || key === "previous_revenue" || key === "avg_daily_revenue" || key === "total_revenue") {
            return formatCurrency(value, currency);
        }
        if (key.includes("pct") || key.includes("percent")) {
            return `${value.toFixed(1)}%`;
        }
        return value.toLocaleString("en-US");
    }
    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)) {
        return formatDate(value);
    }
    return String(value);
}
function formatLocation(locationId) {
    return LOCATION_NAMES[locationId] ?? `Location ${locationId}`;
}
function roleAllowedLocations() {
    const locations = ROLE_SCOPE[activeRole].locationIds;
    return locations ? new Set(locations) : null;
}
function isVisibleLocation(locationId) {
    const allowed = roleAllowedLocations();
    return allowed ? allowed.has(locationId) : true;
}
function formatMachine(a) {
    const loc = formatLocation(a.location_id);
    if (a.machine_id != null)
        return `${loc}, Machine #${a.machine_id}`;
    return loc;
}
function formatActionType(raw) {
    return ACTION_TYPE_LABELS[raw] ?? raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
function formatCurrency(amount, currency) {
    const code = currency ?? "USD";
    let formatter = currencyFormatterCache.get(code);
    if (!formatter) {
        const locale = CURRENCY_LOCALES[code] ?? "en-US";
        formatter = new Intl.NumberFormat(locale, {
            style: "currency",
            currency: code,
            minimumFractionDigits: 2,
            maximumFractionDigits: 2,
        });
        currencyFormatterCache.set(code, formatter);
    }
    return formatter.format(amount);
}
function filterDashboardForRole(dash) {
    return {
        ...dash,
        daily_revenue: dash.daily_revenue.filter((x) => isVisibleLocation(x.location_id)),
        machine_revenue: dash.machine_revenue.filter((x) => isVisibleLocation(x.location_id)),
        top_alert_patterns: dash.top_alert_patterns.filter((x) => isVisibleLocation(x.location_id)),
    };
}
function filterInventoryForRole(inv) {
    return {
        ...inv,
        locations: inv.locations.filter((loc) => isVisibleLocation(loc.location_id)),
    };
}
/* ── Core helpers ────────────────────────────────────────── */
async function api(path, init) {
    const response = await fetch(path, {
        headers: init?.body ? { "Content-Type": "application/json" } : undefined,
        ...init,
    });
    if (!response.ok) {
        const text = await response.text();
        throw new Error(`${response.status} ${response.statusText}: ${text}`);
    }
    return (await response.json());
}
function setBusy(busy) {
    for (const button of [el.resetBtn, el.runBtn, el.nextBtn, el.skipBtn, el.refreshBtn]) {
        button.disabled = busy;
    }
}
function escapeHtml(text) {
    return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}
function jsonInline(data) {
    try {
        return JSON.stringify(data);
    }
    catch {
        return String(data);
    }
}
function createSpinnerLabel(text) {
    const wrap = document.createElement("div");
    wrap.className = "llm-status llm-status--pending";
    const spinner = document.createElement("span");
    spinner.className = "spinner";
    spinner.setAttribute("aria-hidden", "true");
    const label = document.createElement("span");
    label.textContent = text;
    wrap.appendChild(spinner);
    wrap.appendChild(label);
    return wrap;
}
function upsertScriptInstruction(scriptName, instruction) {
    if (!instruction.trim())
        return;
    scriptInstructionByName.set(scriptName, instruction.trim());
}
function scrollToScriptsPanel() {
    el.scriptsList.closest(".panel")?.scrollIntoView({ behavior: "smooth", block: "start" });
}
function quickEditPrompts(scriptName) {
    return [
        `Raise the trigger threshold in ${scriptName} by about 20% to reduce noisy alerts while keeping severe cases.`,
        `Change ${scriptName} from per-product alerting to product-group alerting using stable group labels in context.`,
        `Require at least 3 repeated events across recent days before emitting an alert in ${scriptName}.`,
    ];
}
function closeMachineSalesModal() {
    machineSalesModalState = null;
    renderMachineSalesModal();
}
function renderMachineSalesModal() {
    const state = machineSalesModalState;
    if (!state) {
        el.machineSalesModal.classList.add("modal--hidden");
        el.machineSalesBody.innerHTML = "";
        el.machineSalesMeta.textContent = "";
        return;
    }
    el.machineSalesModal.classList.remove("modal--hidden");
    el.machineSalesTitle.textContent = `${state.machineName} Day Sales`;
    el.machineSalesMeta.textContent = `${state.locationName} · ${state.loading ? "Loading..." : ""}`;
    el.machineSalesBody.innerHTML = "";
    if (state.loading) {
        el.machineSalesBody.appendChild(createSpinnerLabel("Loading today's grouped sales..."));
        return;
    }
    if (state.error) {
        const err = document.createElement("div");
        err.className = "llm-status llm-status--error";
        err.textContent = state.error;
        el.machineSalesBody.appendChild(err);
        return;
    }
    if (!state.data) {
        const empty = document.createElement("div");
        empty.className = "detail__empty";
        empty.textContent = "No day sales available.";
        el.machineSalesBody.appendChild(empty);
        return;
    }
    const data = state.data;
    const meta = document.createElement("div");
    meta.className = "modal__summary";
    meta.textContent =
        `${formatDate(data.date)} · ${data.totals.tx_count.toLocaleString("en-US")} sales · ${formatCurrency(data.totals.revenue, data.currency)}`;
    el.machineSalesBody.appendChild(meta);
    if (data.groups.length === 0) {
        const empty = document.createElement("div");
        empty.className = "detail__empty";
        empty.textContent = "No transactions for this machine on the current simulation day.";
        el.machineSalesBody.appendChild(empty);
        return;
    }
    const tableWrap = document.createElement("div");
    tableWrap.className = "modal-table-wrap";
    const table = document.createElement("table");
    table.className = "modal-table";
    table.innerHTML = `
    <thead>
      <tr>
        <th>Product Group</th>
        <th>Sales</th>
        <th>Revenue</th>
        <th>Avg Price</th>
        <th>Avg Expected</th>
      </tr>
    </thead>
  `;
    const body = document.createElement("tbody");
    for (const row of data.groups) {
        const tr = document.createElement("tr");
        const avgPrice = row.avg_price != null ? formatCurrency(row.avg_price, data.currency) : "—";
        const avgExpected = row.avg_expected_price != null
            ? formatCurrency(row.avg_expected_price, data.currency)
            : "—";
        tr.innerHTML = `
      <td>${escapeHtml(formatIngredientName(row.product_group))}</td>
      <td>${row.tx_count.toLocaleString("en-US")}</td>
      <td>${escapeHtml(formatCurrency(row.revenue, data.currency))}</td>
      <td>${escapeHtml(avgPrice)}</td>
      <td>${escapeHtml(avgExpected)}</td>
    `;
        body.appendChild(tr);
    }
    table.appendChild(body);
    tableWrap.appendChild(table);
    el.machineSalesBody.appendChild(tableWrap);
}
async function openMachineSalesModal(machineId, machineName, locationId, locationName) {
    machineSalesModalState = {
        machineId,
        machineName,
        locationId,
        locationName,
        loading: true,
        error: null,
        data: null,
    };
    renderMachineSalesModal();
    try {
        const data = await api(`/api/machine-sales?machine_id=${machineId}`);
        if (!machineSalesModalState || machineSalesModalState.machineId !== machineId)
            return;
        machineSalesModalState = {
            ...machineSalesModalState,
            loading: false,
            data,
            error: null,
        };
    }
    catch (err) {
        if (!machineSalesModalState || machineSalesModalState.machineId !== machineId)
            return;
        machineSalesModalState = {
            ...machineSalesModalState,
            loading: false,
            data: null,
            error: `Failed to load machine sales: ${String(err)}`,
        };
    }
    renderMachineSalesModal();
}
function closeScriptCompareModal() {
    scriptCompareModalState = null;
    renderScriptCompareModal();
}
function openAboutModal() {
    aboutModalOpen = true;
    renderAboutModal();
}
function closeAboutModal() {
    aboutModalOpen = false;
    renderAboutModal();
}
function renderAboutModal() {
    if (aboutModalOpen) {
        el.aboutModal.classList.remove("modal--hidden");
        return;
    }
    el.aboutModal.classList.add("modal--hidden");
}
function renderScriptCompareModal() {
    const state = scriptCompareModalState;
    if (!state) {
        el.scriptCompareModal.classList.add("modal--hidden");
        el.scriptCompareMeta.textContent = "";
        el.scriptCompareBody.innerHTML = "";
        return;
    }
    el.scriptCompareModal.classList.remove("modal--hidden");
    el.scriptCompareTitle.textContent = `${state.scriptName} Backtest`;
    if (state.phase === "generating") {
        el.scriptCompareMeta.textContent = "Step 1/4: generating draft script with AI...";
    }
    else if (state.phase === "comparing") {
        el.scriptCompareMeta.textContent =
            "Step 2/4: running historical trigger comparison (old vs new)...";
    }
    else if (state.phase === "final_checking") {
        el.scriptCompareMeta.textContent = "Step 3/4: asking AI for final recommendation...";
    }
    else {
        el.scriptCompareMeta.textContent = "Step 4/4: recommendation ready.";
    }
    el.scriptCompareBody.innerHTML = "";
    const codeGrid = document.createElement("div");
    codeGrid.className = "script-compare-code-grid";
    const oldWrap = document.createElement("div");
    const oldHeader = document.createElement("div");
    oldHeader.className = "section-header";
    oldHeader.textContent = "Old Script";
    oldWrap.appendChild(oldHeader);
    if (state.oldCode) {
        const oldCode = document.createElement("pre");
        oldCode.className = "code script-compare-code";
        oldCode.textContent = state.oldCode;
        oldWrap.appendChild(oldCode);
    }
    else if (state.phase !== "done") {
        oldWrap.appendChild(createSpinnerLabel("Loading current active script..."));
    }
    else {
        const oldMissing = document.createElement("div");
        oldMissing.className = "detail__empty";
        oldMissing.textContent = "Current active script unavailable.";
        oldWrap.appendChild(oldMissing);
    }
    codeGrid.appendChild(oldWrap);
    const newWrap = document.createElement("div");
    const newHeader = document.createElement("div");
    newHeader.className = "section-header";
    newHeader.textContent = "New Draft";
    newWrap.appendChild(newHeader);
    if (state.newCode) {
        const newCode = document.createElement("pre");
        newCode.className = "code script-compare-code";
        newCode.textContent = state.newCode;
        newWrap.appendChild(newCode);
    }
    else if (state.phase === "generating") {
        newWrap.appendChild(createSpinnerLabel("Generating new draft script..."));
    }
    else {
        const newMissing = document.createElement("div");
        newMissing.className = "detail__empty";
        newMissing.textContent = "No draft code generated.";
        newWrap.appendChild(newMissing);
    }
    codeGrid.appendChild(newWrap);
    el.scriptCompareBody.appendChild(codeGrid);
    if (state.error) {
        const err = document.createElement("div");
        err.className = "llm-status llm-status--error";
        err.textContent = state.error;
        el.scriptCompareBody.appendChild(err);
        return;
    }
    if (state.loading) {
        const statusText = state.phase === "generating"
            ? "Generating draft script..."
            : state.phase === "comparing"
                ? "Comparing old and new script triggers across historical days..."
                : "Processing...";
        el.scriptCompareBody.appendChild(createSpinnerLabel(statusText));
        return;
    }
    if (!state.comparison) {
        const empty = document.createElement("div");
        empty.className = "detail__empty";
        empty.textContent = "No comparison output available.";
        el.scriptCompareBody.appendChild(empty);
        return;
    }
    const c = state.comparison;
    const summary = document.createElement("div");
    summary.className = "modal__summary";
    summary.textContent = `${formatDate(c.start_day)} - ${formatDate(c.end_day)} (${c.total_days} days)`;
    el.scriptCompareBody.appendChild(summary);
    const metrics = document.createElement("div");
    metrics.className = "grid2";
    metrics.innerHTML = `
    <div class="kv"><div class="kv__k">Old Script Triggered Days</div><div class="kv__v">${c.old_days_triggered}</div></div>
    <div class="kv"><div class="kv__k">New Script Triggered Days</div><div class="kv__v">${c.new_days_triggered}</div></div>
    <div class="kv"><div class="kv__k">Old Total Alerts</div><div class="kv__v">${c.old_total_alerts}</div></div>
    <div class="kv"><div class="kv__k">New Total Alerts</div><div class="kv__v">${c.new_total_alerts}</div></div>
  `;
    el.scriptCompareBody.appendChild(metrics);
    const changed = c.changed_days ?? [];
    if (changed.length === 0) {
        const noChanges = document.createElement("div");
        noChanges.className = "detail__empty";
        noChanges.textContent = "No day-level trigger count differences between old and new script.";
        el.scriptCompareBody.appendChild(noChanges);
    }
    else {
        const changedHeader = document.createElement("div");
        changedHeader.className = "section-header";
        changedHeader.textContent = `Days With Different Trigger Counts (${changed.length})`;
        el.scriptCompareBody.appendChild(changedHeader);
        const tableWrap = document.createElement("div");
        tableWrap.className = "modal-table-wrap";
        const table = document.createElement("table");
        table.className = "modal-table";
        table.innerHTML = `
      <thead>
        <tr>
          <th>Date</th>
          <th>Old Alerts</th>
          <th>New Alerts</th>
        </tr>
      </thead>
    `;
        const body = document.createElement("tbody");
        for (const row of changed.slice(0, 60)) {
            const tr = document.createElement("tr");
            tr.innerHTML = `
        <td>${escapeHtml(formatDate(row.date))}</td>
        <td>${row.old_alerts}</td>
        <td>${row.new_alerts}</td>
      `;
            body.appendChild(tr);
        }
        table.appendChild(body);
        tableWrap.appendChild(table);
        el.scriptCompareBody.appendChild(tableWrap);
    }
    const finalHeader = document.createElement("div");
    finalHeader.className = "section-header";
    finalHeader.textContent = "Final AI Check";
    el.scriptCompareBody.appendChild(finalHeader);
    if (state.finalCheckLoading || state.phase === "final_checking") {
        el.scriptCompareBody.appendChild(createSpinnerLabel("Asking AI whether to accept draft or try again..."));
        return;
    }
    if (state.finalCheckError) {
        const finalErr = document.createElement("div");
        finalErr.className = "llm-status llm-status--error";
        finalErr.textContent = state.finalCheckError;
        el.scriptCompareBody.appendChild(finalErr);
    }
    if (state.finalCheck) {
        const assessment = document.createElement("div");
        assessment.className = "ai-review__assessment";
        const recommendation = state.finalCheck.recommended_action === "accept_draft"
            ? "Accept Draft"
            : "Try Again";
        assessment.innerHTML = `
      <strong>Recommendation:</strong> ${escapeHtml(recommendation)}<br/>
      ${escapeHtml(state.finalCheck.rationale)}
    `;
        el.scriptCompareBody.appendChild(assessment);
        if (state.finalCheck.retry_instruction) {
            const retryTip = document.createElement("div");
            retryTip.className = "script-editor__hint";
            retryTip.textContent = `Suggested retry instruction: ${state.finalCheck.retry_instruction}`;
            el.scriptCompareBody.appendChild(retryTip);
        }
    }
    else {
        const missing = document.createElement("div");
        missing.className = "detail__empty";
        missing.textContent = "Final AI recommendation not available.";
        el.scriptCompareBody.appendChild(missing);
    }
    const actionRow = document.createElement("div");
    actionRow.className = "script-editor__row";
    const isActivating = scriptBusyByName.get(state.scriptName) === "activate";
    const accept = document.createElement("button");
    accept.className = "btn btn--primary";
    accept.textContent = isActivating ? "Accepting..." : "Accept Draft";
    accept.disabled = isActivating || !scriptDraftByName.has(state.scriptName);
    accept.onclick = async () => {
        const ok = await activateScriptDraft(state.scriptName);
        if (ok)
            closeScriptCompareModal();
    };
    actionRow.appendChild(accept);
    const tryAgain = document.createElement("button");
    tryAgain.className = "btn btn--ghost";
    tryAgain.textContent = "Try Again";
    tryAgain.onclick = () => {
        const suggestion = state.finalCheck?.retry_instruction?.trim();
        if (suggestion) {
            scriptInstructionByName.set(state.scriptName, suggestion);
        }
        selectedScriptName = state.scriptName;
        closeScriptCompareModal();
        renderScripts();
        scrollToScriptsPanel();
    };
    actionRow.appendChild(tryAgain);
    el.scriptCompareBody.appendChild(actionRow);
}
/* ── Render functions ────────────────────────────────────── */
function renderState() {
    if (!state) {
        el.statePill.textContent = "Loading\u2026";
        return;
    }
    el.statePill.textContent = `Viewing: ${formatDate(state.current_day)}`;
    el.skipDate.value = state.current_day;
}
function renderAlerts() {
    const count = alerts.length;
    el.alertsMeta.textContent = count === 0 ? "All clear" : `${count} item${count !== 1 ? "s" : ""} need attention`;
    el.alertsList.innerHTML = "";
    if (alerts.length === 0) {
        const empty = document.createElement("div");
        empty.className = "detail__empty";
        empty.textContent = "Nothing needs your attention right now.";
        el.alertsList.appendChild(empty);
        return;
    }
    for (const a of alerts) {
        const isActive = a.alert_id === selectedAlertId;
        const card = document.createElement("div");
        card.className = `card${isActive ? " card--active" : ""}`;
        card.tabIndex = 0;
        card.onclick = () => {
            selectedAlertId = selectedAlertId === a.alert_id ? null : a.alert_id;
            renderAlerts();
        };
        const top = document.createElement("div");
        top.className = "card__top";
        const left = document.createElement("div");
        left.className = `tag tag--sev-${a.severity}`;
        left.textContent = formatSeverity(a.severity);
        const right = document.createElement("div");
        right.className = "tag";
        right.textContent = formatAlertType(a.alert_type);
        top.appendChild(left);
        top.appendChild(right);
        const title = document.createElement("div");
        title.className = "card__title";
        title.textContent = a.title;
        const meta = document.createElement("div");
        meta.className = "card__meta";
        meta.textContent = `${formatMachine(a)} \u00b7 ${formatDate(a.created_at)}`;
        card.appendChild(top);
        card.appendChild(title);
        card.appendChild(meta);
        if (isActive) {
            card.appendChild(buildAlertExpanded(a));
        }
        el.alertsList.appendChild(card);
    }
}
function buildAlertExpanded(a) {
    const container = document.createElement("div");
    container.className = "card__expanded";
    const alertCurrency = currencyForLocation(a.location_id);
    const summary = document.createElement("div");
    summary.className = "detail__summary";
    summary.textContent = a.summary;
    container.appendChild(summary);
    // Evidence section - rendered as human-readable cards
    const evidenceEntries = Object.entries(a.evidence).filter(([k]) => !["script_name", "script_version", "fingerprint", "evidence_hash"].includes(k));
    if (evidenceEntries.length > 0) {
        const evidenceHeader = document.createElement("div");
        evidenceHeader.className = "section-header";
        evidenceHeader.textContent = "Key Information";
        container.appendChild(evidenceHeader);
        const grid = document.createElement("div");
        grid.className = "grid2";
        for (const [key, value] of evidenceEntries) {
            const wrap = document.createElement("div");
            wrap.className = "kv";
            const k = document.createElement("div");
            k.className = "kv__k";
            k.textContent = formatEvidenceKey(key);
            const v = document.createElement("div");
            v.className = "kv__v";
            v.textContent = formatEvidenceValue(key, value, alertCurrency);
            wrap.appendChild(k);
            wrap.appendChild(v);
            grid.appendChild(wrap);
        }
        container.appendChild(grid);
    }
    // Recommended actions - rendered as readable suggestions
    if (a.recommended_actions.length > 0) {
        const actionsHeader = document.createElement("div");
        actionsHeader.className = "section-header";
        actionsHeader.textContent = "Suggested Next Steps";
        container.appendChild(actionsHeader);
        const actionsList = document.createElement("div");
        actionsList.className = "suggestions-list";
        for (const action of a.recommended_actions) {
            const item = document.createElement("div");
            item.className = "suggestion";
            const actionLabel = document.createElement("div");
            actionLabel.className = "suggestion__action";
            actionLabel.textContent = formatActionType(action.action_type);
            item.appendChild(actionLabel);
            const paramEntries = Object.entries(action.params).filter(([k]) => !["action_type"].includes(k));
            if (paramEntries.length > 0) {
                const details = document.createElement("div");
                details.className = "suggestion__details";
                details.textContent = paramEntries
                    .map(([k, v]) => `${formatEvidenceKey(k)}: ${formatEvidenceValue(k, v, alertCurrency)}`)
                    .join(" \u00b7 ");
                item.appendChild(details);
            }
            actionsList.appendChild(item);
        }
        container.appendChild(actionsList);
    }
    const aiReview = aiReviewByAlertId.get(a.alert_id) ?? null;
    const aiReviewBusy = aiReviewBusyByAlertId.has(a.alert_id);
    const aiReviewError = aiReviewErrorByAlertId.get(a.alert_id) ?? null;
    if (aiReviewBusy || aiReview || aiReviewError) {
        const aiHeader = document.createElement("div");
        aiHeader.className = "section-header";
        aiHeader.textContent = "AI Operations Assistant";
        container.appendChild(aiHeader);
        const aiWrap = document.createElement("div");
        aiWrap.className = "ai-review";
        if (aiReviewBusy) {
            aiWrap.appendChild(createSpinnerLabel("AI review queued and running..."));
        }
        if (aiReviewError) {
            const err = document.createElement("div");
            err.className = "llm-status llm-status--error";
            err.textContent = `AI review failed: ${aiReviewError}`;
            aiWrap.appendChild(err);
        }
        if (aiReview) {
            const assessment = document.createElement("div");
            assessment.className = "ai-review__assessment";
            assessment.textContent = aiReview.assessment;
            aiWrap.appendChild(assessment);
            const action = document.createElement("div");
            action.className = "suggestion";
            action.innerHTML = `
        <div class="suggestion__action">${escapeHtml(formatActionType(aiReview.new_alert_action.action_type))}</div>
        <div class="suggestion__details">${escapeHtml(aiReview.new_alert_action.reason)}</div>
        <div class="suggestion__details">Params: ${escapeHtml(jsonInline(aiReview.new_alert_action.params))}</div>
      `;
            aiWrap.appendChild(action);
            if (aiReview.optional_script_change) {
                const change = aiReview.optional_script_change;
                const hint = document.createElement("div");
                hint.className = "suggestion";
                hint.innerHTML = `
          <div class="suggestion__action">Suggested script update: ${escapeHtml(change.script_name)}</div>
          <div class="suggestion__details">${escapeHtml(change.change_hint)}</div>
          <div class="suggestion__details">${escapeHtml(change.edit_instruction ?? change.change_hint)}</div>
        `;
                const useHint = document.createElement("button");
                useHint.className = "btn btn--ghost btn--small";
                useHint.textContent = "Use For Script Draft";
                useHint.onclick = (event) => {
                    event.stopPropagation();
                    selectedScriptName = change.script_name;
                    upsertScriptInstruction(change.script_name, change.edit_instruction ?? change.change_hint);
                    renderScripts();
                    scrollToScriptsPanel();
                };
                hint.appendChild(useHint);
                aiWrap.appendChild(hint);
            }
        }
        container.appendChild(aiWrap);
    }
    // Action buttons
    const actions = document.createElement("div");
    actions.className = "actions";
    const row1 = document.createElement("div");
    row1.className = "actions__row";
    const accept = document.createElement("button");
    accept.className = "btn btn--primary";
    accept.textContent = "Take current action";
    accept.onclick = async (event) => {
        event.stopPropagation();
        setBusy(true);
        try {
            await api(`/api/alerts/${a.alert_id}/accept`, { method: "POST", body: JSON.stringify({}) });
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    const snoozeDays = document.createElement("input");
    snoozeDays.className = "field__input";
    snoozeDays.type = "number";
    snoozeDays.min = "1";
    snoozeDays.max = "30";
    snoozeDays.value = "3";
    snoozeDays.style.maxWidth = "80px";
    const snoozeLabel = document.createElement("span");
    snoozeLabel.className = "snooze-label";
    snoozeLabel.textContent = "days";
    const snooze = document.createElement("button");
    snooze.className = "btn";
    snooze.textContent = "Remind Me Later";
    snooze.onclick = async (event) => {
        event.stopPropagation();
        setBusy(true);
        try {
            const days = Number(snoozeDays.value || "3");
            await api(`/api/alerts/${a.alert_id}/snooze`, { method: "POST", body: JSON.stringify({ days }) });
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    row1.appendChild(accept);
    row1.appendChild(snooze);
    row1.appendChild(snoozeDays);
    row1.appendChild(snoozeLabel);
    const row2 = document.createElement("div");
    row2.className = "actions__row";
    const note = document.createElement("textarea");
    note.className = "field__text";
    note.placeholder = "Add a note (optional)\u2026";
    const review = document.createElement("button");
    review.className = "btn btn--ghost";
    review.textContent = aiReviewBusy ? "AI Review Running..." : "Get AI Recommendation";
    review.disabled = aiReviewBusy;
    review.onclick = async (event) => {
        event.stopPropagation();
        aiReviewBusyByAlertId.add(a.alert_id);
        aiReviewErrorByAlertId.delete(a.alert_id);
        renderAlerts();
        try {
            const manager_note = note.value.trim() || null;
            const res = await api(`/api/alerts/${a.alert_id}/review-ai`, {
                method: "POST",
                body: JSON.stringify({ manager_note }),
            });
            aiReviewByAlertId.set(a.alert_id, res);
            if (res.optional_script_change) {
                upsertScriptInstruction(res.optional_script_change.script_name, res.optional_script_change.edit_instruction ?? res.optional_script_change.change_hint);
            }
        }
        catch (err) {
            aiReviewErrorByAlertId.set(a.alert_id, String(err));
        }
        finally {
            aiReviewBusyByAlertId.delete(a.alert_id);
            renderAlerts();
        }
    };
    row2.appendChild(note);
    row2.appendChild(review);
    actions.appendChild(row1);
    actions.appendChild(row2);
    container.appendChild(actions);
    return container;
}
function renderDashboard(dash, inv) {
    const roleLabel = ROLE_SCOPE[activeRole].label;
    el.dashMeta.textContent = `${roleLabel} · Last 14 days: ${formatDateRange(dash.start_day, dash.end_day)}`;
    el.dashboard.innerHTML = "";
    renderInventory(inv, dash);
    // Alert patterns section
    if (dash.top_alert_patterns.length > 0) {
        const patternsHeader = document.createElement("div");
        patternsHeader.className = "section-header";
        patternsHeader.textContent = "Common Issues";
        el.dashboard.appendChild(patternsHeader);
        const patternsList = document.createElement("div");
        patternsList.className = "patterns-list";
        for (const p of dash.top_alert_patterns.slice(0, 8)) {
            const item = document.createElement("div");
            item.className = "pattern-item";
            const left = document.createElement("div");
            left.className = "pattern-item__info";
            const typeName = document.createElement("span");
            typeName.className = "pattern-item__type";
            typeName.textContent = formatAlertType(p.alert_type);
            const locName = document.createElement("span");
            locName.className = "pattern-item__loc";
            locName.textContent = formatLocation(p.location_id);
            left.appendChild(typeName);
            left.appendChild(locName);
            const right = document.createElement("div");
            right.className = "pattern-item__right";
            const badge = document.createElement("span");
            badge.className = `tag tag--sev-${p.severity} tag--small`;
            badge.textContent = formatSeverity(p.severity);
            const count = document.createElement("span");
            count.className = "pattern-item__count";
            count.textContent = `${p.n}x`;
            right.appendChild(badge);
            right.appendChild(count);
            item.appendChild(left);
            item.appendChild(right);
            patternsList.appendChild(item);
        }
        el.dashboard.appendChild(patternsList);
    }
}
async function loadScriptDetail(scriptName) {
    if (scriptDetailByName.has(scriptName))
        return;
    if (scriptBusyByName.get(scriptName) === "loading")
        return;
    scriptBusyByName.set(scriptName, "loading");
    scriptErrorByName.delete(scriptName);
    renderScripts();
    try {
        const detail = await api(`/api/scripts/${scriptName}`);
        scriptDetailByName.set(scriptName, detail);
    }
    catch (err) {
        scriptErrorByName.set(scriptName, String(err));
    }
    finally {
        scriptBusyByName.delete(scriptName);
        renderScripts();
    }
}
function mergeScriptRow(detail) {
    const idx = rawScripts.findIndex((s) => s.script_name === detail.script_name);
    const row = {
        script_name: detail.script_name,
        enabled: detail.enabled,
        active_source: detail.active_source,
        active_revision_id: detail.active_revision_id,
        baseline_sha: detail.baseline_sha,
        active_sha: detail.active_sha,
    };
    if (idx >= 0)
        rawScripts[idx] = row;
    else
        rawScripts.push(row);
    rawScripts = [...rawScripts].sort((a, b) => a.script_name.localeCompare(b.script_name));
}
async function setScriptEnabled(scriptName, enabled) {
    scriptBusyByName.set(scriptName, "toggle");
    scriptErrorByName.delete(scriptName);
    renderScripts();
    try {
        const detail = await api(`/api/scripts/${scriptName}/enabled`, {
            method: "POST",
            body: JSON.stringify({ enabled }),
        });
        scriptDetailByName.set(scriptName, detail);
        mergeScriptRow(detail);
    }
    catch (err) {
        scriptErrorByName.set(scriptName, String(err));
    }
    finally {
        scriptBusyByName.delete(scriptName);
        renderScripts();
    }
}
async function generateScriptDraft(scriptName) {
    const instruction = (scriptInstructionByName.get(scriptName) ?? "").trim();
    if (!instruction)
        return;
    let oldCode = null;
    try {
        let detail = scriptDetailByName.get(scriptName) ?? null;
        if (!detail) {
            detail = await api(`/api/scripts/${scriptName}`);
            scriptDetailByName.set(scriptName, detail);
            mergeScriptRow(detail);
        }
        oldCode = detail.active_code ?? null;
    }
    catch {
        oldCode = null;
    }
    scriptCompareModalState = {
        scriptName,
        instruction,
        revisionId: null,
        phase: "generating",
        loading: true,
        error: null,
        oldCode,
        newCode: null,
        comparison: null,
        finalCheckLoading: false,
        finalCheckError: null,
        finalCheck: null,
    };
    renderScriptCompareModal();
    scriptBusyByName.set(scriptName, "draft");
    scriptErrorByName.delete(scriptName);
    renderScripts();
    try {
        const draft = await api(`/api/scripts/${scriptName}/generate-edit`, {
            method: "POST",
            body: JSON.stringify({ instruction }),
        });
        scriptDraftByName.set(scriptName, draft);
        scriptCompareModalState = {
            scriptName,
            instruction,
            revisionId: draft.revision_id,
            phase: "comparing",
            loading: true,
            error: null,
            oldCode,
            newCode: draft.code,
            comparison: null,
            finalCheckLoading: false,
            finalCheckError: null,
            finalCheck: null,
        };
        renderScriptCompareModal();
        const compareRes = await api(`/api/scripts/${scriptName}/compare-draft`, {
            method: "POST",
            body: JSON.stringify({ revision_id: draft.revision_id }),
        });
        scriptCompareModalState = {
            scriptName,
            instruction,
            revisionId: draft.revision_id,
            phase: "final_checking",
            loading: false,
            error: null,
            oldCode,
            newCode: draft.code,
            comparison: compareRes.comparison ?? null,
            finalCheckLoading: true,
            finalCheckError: null,
            finalCheck: null,
        };
        renderScriptCompareModal();
        await runDraftFinalCheck(scriptName, draft.revision_id, compareRes.comparison ?? null, instruction);
    }
    catch (err) {
        const message = String(err);
        scriptErrorByName.set(scriptName, message);
        const existing = scriptCompareModalState;
        scriptCompareModalState = {
            scriptName,
            instruction,
            revisionId: existing?.revisionId ?? null,
            phase: "done",
            loading: false,
            error: `Failed to generate script draft: ${message}`,
            oldCode: existing?.oldCode ?? oldCode,
            newCode: existing?.newCode ?? null,
            comparison: null,
            finalCheckLoading: false,
            finalCheckError: null,
            finalCheck: null,
        };
        renderScriptCompareModal();
    }
    finally {
        scriptBusyByName.delete(scriptName);
        renderScripts();
    }
}
async function openScriptCompareModalForDraft(scriptName) {
    const draft = scriptDraftByName.get(scriptName);
    if (!draft)
        return;
    let oldCode = null;
    try {
        let detail = scriptDetailByName.get(scriptName) ?? null;
        if (!detail) {
            detail = await api(`/api/scripts/${scriptName}`);
            scriptDetailByName.set(scriptName, detail);
            mergeScriptRow(detail);
        }
        oldCode = detail.active_code ?? null;
    }
    catch {
        oldCode = null;
    }
    scriptCompareModalState = {
        scriptName,
        instruction: scriptInstructionByName.get(scriptName) ?? "",
        revisionId: draft.revision_id,
        phase: "comparing",
        loading: true,
        error: null,
        oldCode,
        newCode: draft.code,
        comparison: null,
        finalCheckLoading: false,
        finalCheckError: null,
        finalCheck: null,
    };
    renderScriptCompareModal();
    try {
        const compareRes = await api(`/api/scripts/${scriptName}/compare-draft`, {
            method: "POST",
            body: JSON.stringify({ revision_id: draft.revision_id }),
        });
        scriptCompareModalState = {
            scriptName,
            instruction: scriptInstructionByName.get(scriptName) ?? "",
            revisionId: draft.revision_id,
            phase: "final_checking",
            loading: false,
            error: null,
            oldCode,
            newCode: draft.code,
            comparison: compareRes.comparison ?? null,
            finalCheckLoading: true,
            finalCheckError: null,
            finalCheck: null,
        };
        renderScriptCompareModal();
        await runDraftFinalCheck(scriptName, draft.revision_id, compareRes.comparison ?? null, scriptInstructionByName.get(scriptName) ?? "");
    }
    catch (err) {
        scriptCompareModalState = {
            scriptName,
            instruction: scriptInstructionByName.get(scriptName) ?? "",
            revisionId: draft.revision_id,
            phase: "done",
            loading: false,
            error: `Failed to run historical comparison: ${String(err)}`,
            oldCode,
            newCode: draft.code,
            comparison: null,
            finalCheckLoading: false,
            finalCheckError: null,
            finalCheck: null,
        };
    }
    renderScriptCompareModal();
}
async function runDraftFinalCheck(scriptName, revisionId, comparison, instruction) {
    try {
        const finalCheck = await api(`/api/scripts/${scriptName}/final-check`, {
            method: "POST",
            body: JSON.stringify({ revision_id: revisionId, comparison }),
        });
        if (!scriptCompareModalState ||
            scriptCompareModalState.scriptName !== scriptName ||
            scriptCompareModalState.revisionId !== revisionId) {
            return;
        }
        scriptCompareModalState = {
            ...scriptCompareModalState,
            instruction,
            phase: "done",
            finalCheckLoading: false,
            finalCheckError: null,
            finalCheck,
        };
    }
    catch (err) {
        if (!scriptCompareModalState ||
            scriptCompareModalState.scriptName !== scriptName ||
            scriptCompareModalState.revisionId !== revisionId) {
            return;
        }
        scriptCompareModalState = {
            ...scriptCompareModalState,
            instruction,
            phase: "done",
            finalCheckLoading: false,
            finalCheckError: `Final check failed: ${String(err)}`,
            finalCheck: null,
        };
    }
    renderScriptCompareModal();
}
async function activateScriptDraft(scriptName) {
    const draft = scriptDraftByName.get(scriptName);
    if (!draft)
        return false;
    let success = false;
    scriptBusyByName.set(scriptName, "activate");
    scriptErrorByName.delete(scriptName);
    renderScripts();
    renderScriptCompareModal();
    try {
        const detail = await api(`/api/scripts/${scriptName}/activate`, {
            method: "POST",
            body: JSON.stringify({ revision_id: draft.revision_id }),
        });
        scriptDetailByName.set(scriptName, detail);
        scriptDraftByName.delete(scriptName);
        mergeScriptRow(detail);
        success = true;
    }
    catch (err) {
        scriptErrorByName.set(scriptName, String(err));
    }
    finally {
        scriptBusyByName.delete(scriptName);
        renderScripts();
        renderScriptCompareModal();
    }
    return success;
}
async function revertScript(scriptName) {
    scriptBusyByName.set(scriptName, "revert");
    scriptErrorByName.delete(scriptName);
    renderScripts();
    try {
        const detail = await api(`/api/scripts/${scriptName}/revert`, {
            method: "POST",
        });
        scriptDetailByName.set(scriptName, detail);
        scriptDraftByName.delete(scriptName);
        mergeScriptRow(detail);
    }
    catch (err) {
        scriptErrorByName.set(scriptName, String(err));
    }
    finally {
        scriptBusyByName.delete(scriptName);
        renderScripts();
    }
}
function renderScripts() {
    el.scriptsList.innerHTML = "";
    if (rawScripts.length === 0) {
        el.scriptsMeta.textContent = "No scripts found.";
        const empty = document.createElement("div");
        empty.className = "detail__empty";
        empty.textContent = "No scripts available.";
        el.scriptsList.appendChild(empty);
        return;
    }
    if (!selectedScriptName)
        selectedScriptName = rawScripts[0].script_name;
    if (!rawScripts.some((s) => s.script_name === selectedScriptName)) {
        selectedScriptName = rawScripts[0].script_name;
    }
    el.scriptsMeta.textContent = `${rawScripts.length} scripts · click a script to inspect/edit`;
    for (const row of rawScripts) {
        const scriptName = row.script_name;
        const isActive = scriptName === selectedScriptName;
        const card = document.createElement("div");
        card.className = `card${isActive ? " card--active" : ""}`;
        card.onclick = () => {
            selectedScriptName = scriptName;
            renderScripts();
            void loadScriptDetail(scriptName);
        };
        const top = document.createElement("div");
        top.className = "script-top";
        const left = document.createElement("div");
        left.className = "script-top__left";
        const name = document.createElement("div");
        name.className = "script-top__name";
        name.textContent = scriptName;
        left.appendChild(name);
        const right = document.createElement("div");
        right.className = "script-top__meta";
        const source = document.createElement("span");
        source.className = "tag";
        source.textContent = row.active_source === "override" ? "Override" : "Baseline";
        right.appendChild(source);
        const sha = document.createElement("span");
        sha.className = "tag";
        sha.textContent = row.active_sha;
        right.appendChild(sha);
        const toggle = document.createElement("label");
        toggle.className = "script-toggle";
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = row.enabled;
        checkbox.disabled = scriptBusyByName.has(scriptName);
        checkbox.onchange = (event) => {
            event.stopPropagation();
            void setScriptEnabled(scriptName, checkbox.checked);
        };
        toggle.onclick = (event) => event.stopPropagation();
        toggle.appendChild(checkbox);
        toggle.appendChild(document.createTextNode("Enabled"));
        right.appendChild(toggle);
        top.appendChild(left);
        top.appendChild(right);
        card.appendChild(top);
        if (isActive) {
            const busyState = scriptBusyByName.get(scriptName);
            if (busyState === "loading") {
                card.appendChild(createSpinnerLabel("Loading script details..."));
            }
            const err = scriptErrorByName.get(scriptName);
            if (err) {
                const errEl = document.createElement("div");
                errEl.className = "llm-status llm-status--error";
                errEl.textContent = `Script operation failed: ${err}`;
                card.appendChild(errEl);
            }
            const detail = scriptDetailByName.get(scriptName);
            if (detail) {
                const code = document.createElement("pre");
                code.className = "code";
                code.textContent = detail.active_code;
                card.appendChild(code);
                const editor = document.createElement("div");
                editor.className = "script-editor";
                const hint = document.createElement("div");
                hint.className = "script-editor__hint";
                hint.textContent =
                    "Ask for concrete changes: thresholds, grouping logic, cooldown windows, or stricter evidence requirements.";
                editor.appendChild(hint);
                const promptRow = document.createElement("div");
                promptRow.className = "script-editor__row";
                for (const prompt of quickEditPrompts(scriptName)) {
                    const chip = document.createElement("button");
                    chip.className = "btn btn--ghost btn--small";
                    chip.textContent = prompt.length > 42 ? `${prompt.slice(0, 42)}...` : prompt;
                    chip.onclick = (event) => {
                        event.stopPropagation();
                        upsertScriptInstruction(scriptName, prompt);
                        renderScripts();
                    };
                    promptRow.appendChild(chip);
                }
                editor.appendChild(promptRow);
                const instruction = document.createElement("textarea");
                instruction.className = "field__text";
                instruction.placeholder =
                    "Example: Raise undercharge trigger from -5% to -8% and require at least 3 undercharge events in a 7-day window.";
                instruction.value = scriptInstructionByName.get(scriptName) ?? "";
                instruction.oninput = () => {
                    scriptInstructionByName.set(scriptName, instruction.value);
                };
                instruction.onclick = (event) => event.stopPropagation();
                editor.appendChild(instruction);
                const rowActions = document.createElement("div");
                rowActions.className = "script-editor__row";
                const generating = busyState === "draft";
                const generate = document.createElement("button");
                generate.className = "btn btn--primary";
                generate.textContent = generating ? "Generating Draft..." : "Generate AI Draft";
                generate.disabled =
                    scriptBusyByName.has(scriptName) ||
                        (scriptInstructionByName.get(scriptName) ?? "").trim().length === 0;
                generate.onclick = (event) => {
                    event.stopPropagation();
                    void generateScriptDraft(scriptName);
                };
                rowActions.appendChild(generate);
                if (detail.active_source === "override") {
                    const revert = document.createElement("button");
                    revert.className = "btn";
                    revert.textContent = busyState === "revert" ? "Reverting..." : "Revert to Baseline";
                    revert.disabled = scriptBusyByName.has(scriptName);
                    revert.onclick = (event) => {
                        event.stopPropagation();
                        void revertScript(scriptName);
                    };
                    rowActions.appendChild(revert);
                }
                editor.appendChild(rowActions);
                card.appendChild(editor);
            }
            else if (!scriptBusyByName.has(scriptName)) {
                void loadScriptDetail(scriptName);
            }
            const draft = scriptDraftByName.get(scriptName);
            if (draft) {
                const draftHeader = document.createElement("div");
                draftHeader.className = "section-header";
                draftHeader.textContent = `Draft Revision ${draft.revision_id.slice(0, 12)}`;
                card.appendChild(draftHeader);
                const draftHint = document.createElement("div");
                draftHint.className = "script-editor__hint";
                draftHint.textContent =
                    "Review old vs new script, trigger comparison, and final AI recommendation in one modal.";
                card.appendChild(draftHint);
                const activateRow = document.createElement("div");
                activateRow.className = "script-editor__row";
                const review = document.createElement("button");
                review.className = "btn btn--primary";
                review.textContent = "Review Draft Backtest";
                review.disabled = scriptBusyByName.has(scriptName);
                review.onclick = (event) => {
                    event.stopPropagation();
                    void openScriptCompareModalForDraft(scriptName);
                };
                activateRow.appendChild(review);
                card.appendChild(activateRow);
            }
        }
        el.scriptsList.appendChild(card);
    }
}
const INGREDIENT_ICONS = {
    espresso_shot: "\u2615",
    milk: "\ud83e\udd5b",
    caramel_syrup: "\ud83c\udf6f",
    whiskey: "\ud83e\udd43",
    chocolate_powder: "\ud83c\udf6b",
    water: "\ud83d\udca7",
    vanilla_syrup: "\ud83c\udf3c",
    tea_bag: "\ud83c\udf75",
};
function coffeeMachineSvg() {
    return `<svg viewBox="0 0 64 64" width="36" height="36" fill="none" xmlns="http://www.w3.org/2000/svg">
    <rect x="12" y="8" width="40" height="48" rx="4" fill="#1a2a38" stroke="#6ae4ff" stroke-width="1.5"/>
    <rect x="18" y="14" width="28" height="10" rx="2" fill="#0c1116" stroke="#3a4a58" stroke-width="1"/>
    <circle cx="32" cy="19" r="3" fill="#ffd25a" opacity="0.8"/>
    <rect x="22" y="32" width="20" height="4" rx="1" fill="#3a4a58"/>
    <rect x="26" y="38" width="12" height="12" rx="2" fill="#0c1116" stroke="#3a4a58" stroke-width="1"/>
    <rect x="28" y="36" width="2" height="3" fill="#6ae4ff" opacity="0.6"/>
    <rect x="34" y="36" width="2" height="3" fill="#6ae4ff" opacity="0.6"/>
    <rect x="18" y="54" width="10" height="3" rx="1" fill="#3a4a58"/>
    <rect x="36" y="54" width="10" height="3" rx="1" fill="#3a4a58"/>
  </svg>`;
}
function formatIngredientName(raw) {
    return raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
function ingredientFillPercent(quantity, capacity) {
    if (!capacity || capacity <= 0)
        return 0;
    return Math.min(100, Math.max(0, (quantity / capacity) * 100));
}
function renderInventory(inv, dash) {
    const revenueByMachine = new Map();
    for (const row of dash.machine_revenue) {
        revenueByMachine.set(`${row.location_id}:${row.machine_id}`, {
            revenue: Number(row.revenue),
            tx: Number(row.tx_count),
        });
    }
    const section = document.createElement("div");
    section.className = "inventory-section";
    const header = document.createElement("div");
    header.className = "section-header";
    header.textContent = "View by Location";
    section.appendChild(header);
    if (inv.locations.length === 0) {
        const empty = document.createElement("div");
        empty.className = "detail__empty";
        empty.textContent = "No inventory data available for this role.";
        section.appendChild(empty);
        el.dashboard.prepend(section);
        return;
    }
    const dateNote = document.createElement("div");
    dateNote.className = "inventory-date";
    dateNote.textContent = `As of ${formatDate(inv.snapshot_date)}`;
    section.appendChild(dateNote);
    const locationOverview = document.createElement("div");
    locationOverview.className = "location-overview";
    for (const loc of inv.locations) {
        const card = document.createElement("div");
        card.className = "location-card";
        const title = document.createElement("div");
        title.className = "location-card__title";
        title.textContent = loc.location_name;
        const meta = document.createElement("div");
        meta.className = "location-card__meta";
        meta.textContent = `${loc.machines.length} machine${loc.machines.length === 1 ? "" : "s"}`;
        const machines = document.createElement("div");
        machines.className = "location-card__machines";
        const locationCurrency = loc.currency ?? currencyForLocation(loc.location_id);
        for (const machine of loc.machines) {
            const chip = document.createElement("span");
            chip.className = "location-card__chip";
            chip.textContent = machine.machine_name;
            chip.style.cursor = "pointer";
            chip.onclick = (event) => {
                event.stopPropagation();
                void openMachineSalesModal(machine.machine_id, machine.machine_name, loc.location_id, loc.location_name);
            };
            machines.appendChild(chip);
        }
        card.appendChild(title);
        card.appendChild(meta);
        card.appendChild(machines);
        locationOverview.appendChild(card);
    }
    section.appendChild(locationOverview);
    const detailHeader = document.createElement("div");
    detailHeader.className = "section-header";
    detailHeader.textContent = "Machine Ingredient Levels";
    section.appendChild(detailHeader);
    for (const loc of inv.locations) {
        const locSection = document.createElement("div");
        locSection.className = "inv-location";
        const locHeader = document.createElement("div");
        locHeader.className = "inv-location__header";
        locHeader.textContent = loc.location_name;
        locSection.appendChild(locHeader);
        const locationCurrency = loc.currency ?? currencyForLocation(loc.location_id);
        for (const machine of loc.machines) {
            const machineCard = document.createElement("div");
            machineCard.className = "inv-machine inv-machine--clickable";
            machineCard.onclick = (event) => {
                if (event.target.closest("button"))
                    return;
                void openMachineSalesModal(machine.machine_id, machine.machine_name, loc.location_id, loc.location_name);
            };
            const machineLayout = document.createElement("div");
            machineLayout.className = "inv-machine__layout";
            const machineSide = document.createElement("div");
            machineSide.className = "inv-machine__side";
            const iconWrap = document.createElement("div");
            iconWrap.className = "inv-machine__icon";
            iconWrap.innerHTML = coffeeMachineSvg();
            const machineInfo = document.createElement("div");
            machineInfo.className = "inv-machine__info";
            const machineName = document.createElement("div");
            machineName.className = "inv-machine__name";
            machineName.textContent = machine.machine_name;
            const machineId = document.createElement("div");
            machineId.className = "inv-machine__id";
            machineId.textContent = `Machine #${machine.machine_id}`;
            const machineRevenue = revenueByMachine.get(`${loc.location_id}:${machine.machine_id}`);
            const machinePerf = document.createElement("div");
            machinePerf.className = "inv-machine__perf";
            machinePerf.innerHTML = `
        <div class="inv-machine__rev">${formatCurrency(machineRevenue?.revenue ?? 0, locationCurrency)}</div>
        <div class="inv-machine__tx">${(machineRevenue?.tx ?? 0).toLocaleString("en-US")} sales</div>
      `;
            const restockBtn = document.createElement("button");
            restockBtn.className = "btn btn--small";
            restockBtn.textContent = "Restock machine (tomorrow)";
            restockBtn.onclick = async () => {
                setBusy(true);
                try {
                    await api("/api/restock-machine", {
                        method: "POST",
                        body: JSON.stringify({ machine_id: machine.machine_id }),
                    });
                    await refresh();
                }
                finally {
                    setBusy(false);
                }
            };
            machineInfo.appendChild(machineName);
            machineInfo.appendChild(machineId);
            machineInfo.appendChild(machinePerf);
            machineInfo.appendChild(restockBtn);
            machineSide.appendChild(iconWrap);
            machineSide.appendChild(machineInfo);
            const ingredientsList = document.createElement("div");
            ingredientsList.className = "inv-ingredients inv-ingredients--dense";
            for (const ing of machine.ingredients) {
                const startQty = ing.start_quantity ?? ing.quantity;
                const endQty = ing.end_quantity ?? ing.quantity;
                const capacity = ing.capacity ?? null;
                const startPct = ingredientFillPercent(startQty, capacity);
                const endPct = ingredientFillPercent(endQty, capacity);
                const icon = INGREDIENT_ICONS[ing.name] ?? "\u2022";
                const isLow = endPct > 0 && endPct < 25;
                const row = document.createElement("div");
                row.className = `inv-ingredient${isLow ? " inv-ingredient--low" : ""}`;
                const label = document.createElement("div");
                label.className = "inv-ingredient__label";
                label.textContent = `${icon} ${formatIngredientName(ing.name)}`;
                const chart = document.createElement("div");
                chart.className = "inv-ingredient__chart";
                const meta = document.createElement("div");
                meta.className = "inv-ingredient__meta";
                const remainingText = capacity && capacity > 0 ? `${Math.round(endPct)}% remaining` : "Capacity unknown";
                meta.innerHTML = `Capacity ${capacity != null ? `${capacity.toLocaleString("en-US")} ${ing.capacity_unit ?? ing.unit}` : "N/A"} · Start ${startQty.toLocaleString("en-US")} ${ing.unit} · End ${endQty.toLocaleString("en-US")} ${ing.unit} · <span class="inv-ingredient__pct">${escapeHtml(remainingText)}</span>`;
                const bar = document.createElement("div");
                bar.className = "inv-ingredient__bar inv-ingredient__bar--single";
                const startFill = document.createElement("div");
                startFill.className = "inv-ingredient__fill inv-ingredient__fill--start";
                startFill.style.width = `${startPct}%`;
                const endFill = document.createElement("div");
                endFill.className = `inv-ingredient__fill inv-ingredient__fill--end${isLow ? " inv-ingredient__fill--low" : ""}`;
                endFill.style.width = `${endPct}%`;
                bar.appendChild(startFill);
                bar.appendChild(endFill);
                chart.appendChild(meta);
                chart.appendChild(bar);
                row.appendChild(label);
                row.appendChild(chart);
                ingredientsList.appendChild(row);
            }
            machineLayout.appendChild(machineSide);
            machineLayout.appendChild(ingredientsList);
            machineCard.appendChild(machineLayout);
            locSection.appendChild(machineCard);
        }
        section.appendChild(locSection);
    }
    el.dashboard.prepend(section);
}
function applyRoleFilterAndRender() {
    const role = ROLE_SCOPE[activeRole];
    const allowed = role.locationIds;
    alerts = rawAlerts.filter((a) => (allowed ? allowed.includes(a.location_id) : true));
    const dash = rawDashboard ? filterDashboardForRole(rawDashboard) : null;
    const inv = rawInventory ? filterInventoryForRole(rawInventory) : null;
    if (selectedAlertId && !alerts.some((a) => a.alert_id === selectedAlertId)) {
        selectedAlertId = null;
    }
    renderState();
    renderAlerts();
    if (dash && inv)
        renderDashboard(dash, inv);
    renderScripts();
    renderMachineSalesModal();
    renderScriptCompareModal();
}
async function refresh() {
    state = await api("/api/state");
    rawAlerts = await api("/api/alerts?limit=200");
    rawDashboard = await api("/api/dashboard?days=14");
    updateLocationCurrencyFromDashboard(rawDashboard);
    rawInventory = await api("/api/inventory");
    updateLocationCurrencyFromInventory(rawInventory);
    rawScripts = await api("/api/scripts");
    const scriptNames = new Set(rawScripts.map((s) => s.script_name));
    for (const key of [...scriptDetailByName.keys()]) {
        if (!scriptNames.has(key))
            scriptDetailByName.delete(key);
    }
    for (const key of [...scriptDraftByName.keys()]) {
        if (!scriptNames.has(key))
            scriptDraftByName.delete(key);
    }
    for (const key of [...scriptInstructionByName.keys()]) {
        if (!scriptNames.has(key))
            scriptInstructionByName.delete(key);
    }
    applyRoleFilterAndRender();
    if (selectedScriptName) {
        void loadScriptDetail(selectedScriptName);
    }
}
async function main() {
    el.roleSelect.value = activeRole;
    el.roleSelect.onchange = () => {
        activeRole = el.roleSelect.value;
        applyRoleFilterAndRender();
    };
    el.machineSalesClose.onclick = () => closeMachineSalesModal();
    el.machineSalesBackdrop.onclick = () => closeMachineSalesModal();
    el.scriptCompareClose.onclick = () => closeScriptCompareModal();
    el.scriptCompareBackdrop.onclick = () => closeScriptCompareModal();
    el.aboutClose.onclick = () => closeAboutModal();
    el.aboutBackdrop.onclick = () => closeAboutModal();
    el.aboutBtn.onclick = () => openAboutModal();
    window.addEventListener("keydown", (event) => {
        if (event.key !== "Escape")
            return;
        if (aboutModalOpen) {
            closeAboutModal();
            return;
        }
        if (scriptCompareModalState) {
            closeScriptCompareModal();
            return;
        }
        if (machineSalesModalState) {
            closeMachineSalesModal();
        }
    });
    el.resetBtn.onclick = async () => {
        setBusy(true);
        try {
            await api("/api/state/reset", { method: "POST" });
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    el.runBtn.onclick = async () => {
        setBusy(true);
        try {
            await api("/api/run-current", { method: "POST" });
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    el.nextBtn.onclick = async () => {
        setBusy(true);
        try {
            await api("/api/state/next", { method: "POST" });
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    el.skipBtn.onclick = async () => {
        setBusy(true);
        try {
            const date = el.skipDate.value;
            if (!date)
                return;
            await api("/api/state/skip", { method: "POST", body: JSON.stringify({ date }) });
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    el.refreshBtn.onclick = async () => {
        setBusy(true);
        try {
            await refresh();
        }
        finally {
            setBusy(false);
        }
    };
    // Allow opting out for screenshots/embeds: `?about=0`.
    if (new URLSearchParams(window.location.search).get("about") !== "0") {
        openAboutModal();
    }
    await refresh();
}
main().catch((err) => {
    console.error(err);
    el.statePill.textContent = `Error: ${String(err)}`;
});
