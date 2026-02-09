type EngineState = {
  start_day: string;
  end_day: string;
  current_day: string;
  updated_at?: string;
};

type AlertRow = {
  alert_id: string;
  created_at: string;
  run_date: string;
  script_name: string;
  script_version: string;
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL" | string;
  alert_type: string;
  location_id: number;
  machine_id: number | null;
  product_id: number | null;
  ingredient_id: number | null;
  title: string;
  summary: string;
  status: string;
  evidence: Record<string, unknown>;
  recommended_actions: Array<{ action_type: string; params: Record<string, unknown> }>;
};

type Dashboard = {
  start_day: string;
  end_day: string;
  daily_revenue: Array<{ location_id: number; date: string; revenue: number; tx_count: number }>;
  top_alert_patterns: Array<{ location_id: number; severity: string; alert_type: string; n: number }>;
};

type InventoryIngredient = {
  ingredient_id: number;
  name: string;
  quantity: number;
  unit: string;
};

type InventoryMachine = {
  machine_id: number;
  machine_name: string;
  ingredients: InventoryIngredient[];
};

type InventoryLocation = {
  location_id: number;
  location_name: string;
  machines: InventoryMachine[];
};

type InventoryResponse = {
  snapshot_date: string;
  locations: InventoryLocation[];
};

const el = {
  statePill: document.getElementById("state-pill") as HTMLDivElement,
  alertsMeta: document.getElementById("alerts-meta") as HTMLDivElement,
  alertsList: document.getElementById("alerts-list") as HTMLDivElement,
  detailMeta: document.getElementById("detail-meta") as HTMLDivElement,
  detail: document.getElementById("detail") as HTMLDivElement,
  dashMeta: document.getElementById("dash-meta") as HTMLDivElement,
  dashboard: document.getElementById("dashboard") as HTMLDivElement,
  skipDate: document.getElementById("skip-date") as HTMLInputElement,
  resetBtn: document.getElementById("reset-btn") as HTMLButtonElement,
  runBtn: document.getElementById("run-btn") as HTMLButtonElement,
  nextBtn: document.getElementById("next-btn") as HTMLButtonElement,
  skipBtn: document.getElementById("skip-btn") as HTMLButtonElement,
  refreshBtn: document.getElementById("refresh-btn") as HTMLButtonElement,
};

let state: EngineState | null = null;
let alerts: AlertRow[] = [];
let selectedAlertId: string | null = null;

/* ── Formatting helpers ─────────────────────────────────── */

const ALERT_TYPE_LABELS: Record<string, string> = {
  service_due: "Service Due",
  machine_dropoff: "Sales Drop-Off",
  pricing_anomaly: "Pricing Issue",
  slow_mover_cleanup: "Slow-Moving Product",
  low_stock: "Low Stock",
  refill_needed: "Refill Needed",
  ingredient_low: "Ingredient Running Low",
  revenue_drop: "Revenue Decline",
  high_revenue: "High Revenue",
};

const SEVERITY_LABELS: Record<string, string> = {
  LOW: "Low",
  MEDIUM: "Medium",
  HIGH: "High",
  CRITICAL: "Critical",
};

const EVIDENCE_KEY_LABELS: Record<string, string> = {
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

const ACTION_TYPE_LABELS: Record<string, string> = {
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

function formatAlertType(raw: string): string {
  return ALERT_TYPE_LABELS[raw] ?? raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatSeverity(raw: string): string {
  return SEVERITY_LABELS[raw] ?? raw;
}

function formatDate(iso: string): string {
  try {
    const d = new Date(iso + (iso.includes("T") ? "" : "T00:00:00"));
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
  } catch {
    return iso;
  }
}

function formatDateRange(startIso: string, endIso: string): string {
  return `${formatDate(startIso)} \u2013 ${formatDate(endIso)}`;
}

function formatEvidenceKey(raw: string): string {
  return EVIDENCE_KEY_LABELS[raw] ?? raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatEvidenceValue(key: string, value: unknown): string {
  if (value === null || value === undefined) return "\u2014";
  if (typeof value === "number") {
    if (key.includes("revenue") || key.includes("price") || key === "amount" || key === "current_revenue" || key === "previous_revenue" || key === "avg_daily_revenue" || key === "total_revenue") {
      return `$${value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
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

function formatLocation(locationId: number): string {
  return `Location ${locationId}`;
}

function formatMachine(a: AlertRow): string {
  const loc = formatLocation(a.location_id);
  if (a.machine_id != null) return `${loc}, Machine #${a.machine_id}`;
  return loc;
}

function formatActionType(raw: string): string {
  return ACTION_TYPE_LABELS[raw] ?? raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatCurrency(amount: number): string {
  return `$${amount.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

/* ── Core helpers ────────────────────────────────────────── */

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    headers: init?.body ? { "Content-Type": "application/json" } : undefined,
    ...init,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status} ${response.statusText}: ${text}`);
  }
  return (await response.json()) as T;
}

function setBusy(busy: boolean): void {
  for (const button of [el.resetBtn, el.runBtn, el.nextBtn, el.skipBtn, el.refreshBtn]) {
    button.disabled = busy;
  }
}

function escapeHtml(text: string): string {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

/* ── Render functions ────────────────────────────────────── */

function renderState(): void {
  if (!state) {
    el.statePill.textContent = "Loading\u2026";
    return;
  }
  el.statePill.textContent = `Viewing: ${formatDate(state.current_day)}`;
  el.skipDate.value = state.current_day;
}

function renderAlerts(): void {
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
    const card = document.createElement("div");
    card.className = `card${a.alert_id === selectedAlertId ? " card--active" : ""}`;
    card.tabIndex = 0;
    card.onclick = () => {
      selectedAlertId = a.alert_id;
      renderAlerts();
      renderDetail();
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
    el.alertsList.appendChild(card);
  }
}

function renderDetail(): void {
  const a = alerts.find((x) => x.alert_id === selectedAlertId) ?? null;
  if (!a) {
    el.detailMeta.textContent = "Select an item to view details";
    el.detail.innerHTML = `<div class="detail__empty">Click an item on the left to see details and take action.</div>`;
    return;
  }

  el.detailMeta.textContent = `${formatMachine(a)} \u00b7 ${formatSeverity(a.severity)} priority`;

  const container = document.createElement("div");

  const title = document.createElement("div");
  title.className = "detail__title";
  title.textContent = a.title;

  const summary = document.createElement("div");
  summary.className = "detail__summary";
  summary.textContent = a.summary;

  container.appendChild(title);
  container.appendChild(summary);

  // Evidence section - rendered as human-readable cards
  const evidenceEntries = Object.entries(a.evidence).filter(
    ([k]) => !["script_name", "script_version", "fingerprint", "evidence_hash"].includes(k)
  );
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
      v.textContent = formatEvidenceValue(key, value);
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

      const paramEntries = Object.entries(action.params).filter(
        ([k]) => !["action_type"].includes(k)
      );
      if (paramEntries.length > 0) {
        const details = document.createElement("div");
        details.className = "suggestion__details";
        details.textContent = paramEntries
          .map(([k, v]) => `${formatEvidenceKey(k)}: ${formatEvidenceValue(k, v)}`)
          .join(" \u00b7 ");
        item.appendChild(details);
      }
      actionsList.appendChild(item);
    }
    container.appendChild(actionsList);
  }

  // Action buttons
  const actions = document.createElement("div");
  actions.className = "actions";

  const row1 = document.createElement("div");
  row1.className = "actions__row";

  const accept = document.createElement("button");
  accept.className = "btn btn--primary";
  accept.textContent = "Mark as Done";
  accept.onclick = async () => {
    setBusy(true);
    try {
      await api(`/api/alerts/${a.alert_id}/accept`, { method: "POST", body: JSON.stringify({}) });
      await refresh();
    } finally {
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
  snooze.onclick = async () => {
    setBusy(true);
    try {
      const days = Number(snoozeDays.value || "3");
      await api(`/api/alerts/${a.alert_id}/snooze`, { method: "POST", body: JSON.stringify({ days }) });
      await refresh();
    } finally {
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
  review.textContent = "Get AI Recommendation";
  review.onclick = async () => {
    setBusy(true);
    try {
      const manager_note = note.value.trim() || null;
      const res = await api<Record<string, unknown>>(`/api/alerts/${a.alert_id}/review-ai`, {
        method: "POST",
        body: JSON.stringify({ manager_note }),
      });
      await refresh();
      alert(`AI recommendation:\n\n${JSON.stringify(res, null, 2)}`);
    } finally {
      setBusy(false);
    }
  };

  row2.appendChild(note);
  row2.appendChild(review);

  actions.appendChild(row1);
  actions.appendChild(row2);
  container.appendChild(actions);

  el.detail.innerHTML = "";
  el.detail.appendChild(container);
}

function renderDashboard(dash: Dashboard): void {
  el.dashMeta.textContent = `Last 14 days: ${formatDateRange(dash.start_day, dash.end_day)}`;
  el.dashboard.innerHTML = "";

  // Revenue section
  const revenueHeader = document.createElement("div");
  revenueHeader.className = "section-header";
  revenueHeader.textContent = "Revenue";
  el.dashboard.appendChild(revenueHeader);

  const totalsByLoc = new Map<number, { revenue: number; tx: number }>();
  for (const row of dash.daily_revenue) {
    const prev = totalsByLoc.get(row.location_id) ?? { revenue: 0, tx: 0 };
    prev.revenue += Number(row.revenue);
    prev.tx += Number(row.tx_count);
    totalsByLoc.set(row.location_id, prev);
  }

  const revenueGrid = document.createElement("div");
  revenueGrid.className = "stat-grid";

  if (totalsByLoc.size === 0) {
    const empty = document.createElement("div");
    empty.className = "detail__empty";
    empty.textContent = "No revenue data available yet.";
    revenueGrid.appendChild(empty);
  } else {
    for (const [loc, total] of [...totalsByLoc.entries()].sort((a, b) => a[0] - b[0])) {
      const card = document.createElement("div");
      card.className = "stat-card";

      const label = document.createElement("div");
      label.className = "stat-card__label";
      label.textContent = formatLocation(loc);

      const amount = document.createElement("div");
      amount.className = "stat-card__value";
      amount.textContent = formatCurrency(total.revenue);

      const sub = document.createElement("div");
      sub.className = "stat-card__sub";
      sub.textContent = `${total.tx.toLocaleString("en-US")} sales`;

      card.appendChild(label);
      card.appendChild(amount);
      card.appendChild(sub);
      revenueGrid.appendChild(card);
    }
  }
  el.dashboard.appendChild(revenueGrid);

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

const INGREDIENT_ICONS: Record<string, string> = {
  espresso_shot: "\u2615",
  milk: "\ud83e\udd5b",
  caramel_syrup: "\ud83c\udf6f",
  whiskey: "\ud83e\udd43",
  chocolate_powder: "\ud83c\udf6b",
  water: "\ud83d\udca7",
  vanilla_syrup: "\ud83c\udf3c",
  tea_bag: "\ud83c\udf75",
};

function coffeeMachineSvg(): string {
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

function formatIngredientName(raw: string): string {
  return raw.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function ingredientFillPercent(name: string, quantity: number): number {
  // Realistic container capacities per machine_capacities.md
  const capacities: Record<string, number> = {
    espresso_shot: 50,
    milk: 5000,
    chocolate_powder: 250,
    caramel_syrup: 750,
    vanilla_syrup: 750,
    whiskey: 750,
    water: 5000,
    tea_bag: 50,
  };
  const cap = capacities[name];
  if (!cap) return Math.min(100, Math.max(0, quantity));
  return Math.min(100, Math.max(0, (quantity / cap) * 100));
}

function renderInventory(inv: InventoryResponse): void {
  el.dashboard.insertAdjacentHTML("beforeend", "");

  const header = document.createElement("div");
  header.className = "section-header";
  header.textContent = "Machine Inventory";
  el.dashboard.appendChild(header);

  if (inv.locations.length === 0) {
    const empty = document.createElement("div");
    empty.className = "detail__empty";
    empty.textContent = "No inventory data yet. Run a day to sync inventory.";
    el.dashboard.appendChild(empty);
    return;
  }

  const dateNote = document.createElement("div");
  dateNote.className = "inventory-date";
  dateNote.textContent = `As of ${formatDate(inv.snapshot_date)}`;
  el.dashboard.appendChild(dateNote);

  for (const loc of inv.locations) {
    const locSection = document.createElement("div");
    locSection.className = "inv-location";

    const locHeader = document.createElement("div");
    locHeader.className = "inv-location__header";
    locHeader.textContent = loc.location_name;
    locSection.appendChild(locHeader);

    for (const machine of loc.machines) {
      const machineCard = document.createElement("div");
      machineCard.className = "inv-machine";

      const machineHeader = document.createElement("div");
      machineHeader.className = "inv-machine__header";

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

      machineInfo.appendChild(machineName);
      machineInfo.appendChild(machineId);
      machineHeader.appendChild(iconWrap);
      machineHeader.appendChild(machineInfo);
      machineCard.appendChild(machineHeader);

      const ingredientsList = document.createElement("div");
      ingredientsList.className = "inv-ingredients";

      for (const ing of machine.ingredients) {
        const pct = ingredientFillPercent(ing.name, ing.quantity);
        const icon = INGREDIENT_ICONS[ing.name] ?? "\u2022";
        const isLow = pct < 25;

        const row = document.createElement("div");
        row.className = `inv-ingredient${isLow ? " inv-ingredient--low" : ""}`;

        const label = document.createElement("div");
        label.className = "inv-ingredient__label";
        label.textContent = `${icon} ${formatIngredientName(ing.name)}`;

        const bar = document.createElement("div");
        bar.className = "inv-ingredient__bar";
        const fill = document.createElement("div");
        fill.className = `inv-ingredient__fill${isLow ? " inv-ingredient__fill--low" : ""}`;
        fill.style.width = `${pct}%`;
        bar.appendChild(fill);

        const qty = document.createElement("div");
        qty.className = "inv-ingredient__qty";
        qty.textContent = `${ing.quantity} ${ing.unit}`;

        row.appendChild(label);
        row.appendChild(bar);
        row.appendChild(qty);
        ingredientsList.appendChild(row);
      }

      machineCard.appendChild(ingredientsList);
      locSection.appendChild(machineCard);
    }

    el.dashboard.appendChild(locSection);
  }
}

async function refresh(): Promise<void> {
  state = await api<EngineState>("/api/state");
  alerts = await api<AlertRow[]>("/api/alerts?limit=200");
  const dash = await api<Dashboard>("/api/dashboard?days=14");
  const inv = await api<InventoryResponse>("/api/inventory");
  if (selectedAlertId && !alerts.some((a) => a.alert_id === selectedAlertId)) {
    selectedAlertId = null;
  }
  renderState();
  renderAlerts();
  renderDetail();
  renderDashboard(dash);
  renderInventory(inv);
}

async function main(): Promise<void> {
  el.resetBtn.onclick = async () => {
    setBusy(true);
    try {
      await api("/api/state/reset", { method: "POST" });
      await refresh();
    } finally {
      setBusy(false);
    }
  };
  el.runBtn.onclick = async () => {
    setBusy(true);
    try {
      await api("/api/run-current", { method: "POST" });
      await refresh();
    } finally {
      setBusy(false);
    }
  };
  el.nextBtn.onclick = async () => {
    setBusy(true);
    try {
      await api("/api/state/next", { method: "POST" });
      await refresh();
    } finally {
      setBusy(false);
    }
  };
  el.skipBtn.onclick = async () => {
    setBusy(true);
    try {
      const date = el.skipDate.value;
      if (!date) return;
      await api("/api/state/skip", { method: "POST", body: JSON.stringify({ date }) });
      await refresh();
    } finally {
      setBusy(false);
    }
  };
  el.refreshBtn.onclick = async () => {
    setBusy(true);
    try {
      await refresh();
    } finally {
      setBusy(false);
    }
  };

  await refresh();
}

main().catch((err) => {
  console.error(err);
  el.statePill.textContent = `Error: ${String(err)}`;
});
