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

function fmtMachine(a: AlertRow): string {
  const m = a.machine_id ?? "—";
  return `loc ${a.location_id} · machine ${m}`;
}

function setBusy(busy: boolean): void {
  for (const button of [el.resetBtn, el.runBtn, el.nextBtn, el.skipBtn, el.refreshBtn]) {
    button.disabled = busy;
  }
}

function escapeHtml(text: string): string {
  return text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
}

function renderState(): void {
  if (!state) {
    el.statePill.textContent = "Loading state…";
    return;
  }
  const updated = state.updated_at ? ` · updated ${state.updated_at}` : "";
  el.statePill.textContent = `Day ended: ${state.current_day} · window ${state.start_day} → ${state.end_day}${updated}`;
  el.skipDate.value = state.current_day;
}

function renderAlerts(): void {
  el.alertsMeta.textContent = `${alerts.length} open`;
  el.alertsList.innerHTML = "";

  if (alerts.length === 0) {
    const empty = document.createElement("div");
    empty.className = "detail__empty";
    empty.textContent = "No open alerts right now.";
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
    left.textContent = a.severity;

    const right = document.createElement("div");
    right.className = "tag";
    right.textContent = a.alert_type;

    top.appendChild(left);
    top.appendChild(right);

    const title = document.createElement("div");
    title.className = "card__title";
    title.textContent = a.title;

    const meta = document.createElement("div");
    meta.className = "card__meta";
    meta.textContent = `${fmtMachine(a)} · ${a.script_name} · ${a.created_at}`;

    card.appendChild(top);
    card.appendChild(title);
    card.appendChild(meta);
    el.alertsList.appendChild(card);
  }
}

function kvItem(key: string, value: unknown): HTMLDivElement {
  const wrap = document.createElement("div");
  wrap.className = "kv";
  const k = document.createElement("div");
  k.className = "kv__k";
  k.textContent = key;
  const v = document.createElement("div");
  v.className = "kv__v";
  v.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  wrap.appendChild(k);
  wrap.appendChild(v);
  return wrap;
}

function renderDetail(): void {
  const a = alerts.find((x) => x.alert_id === selectedAlertId) ?? null;
  if (!a) {
    el.detailMeta.textContent = "Select an alert";
    el.detail.innerHTML = `<div class=\"detail__empty\">No alert selected.</div>`;
    return;
  }

  el.detailMeta.textContent = `${a.alert_id} · ${fmtMachine(a)} · ${a.status}`;

  const container = document.createElement("div");
  const title = document.createElement("div");
  title.className = "detail__title";
  title.textContent = a.title;

  const summary = document.createElement("div");
  summary.className = "detail__summary";
  summary.textContent = a.summary;

  const grid = document.createElement("div");
  grid.className = "grid2";
  grid.appendChild(kvItem("Evidence", a.evidence));
  grid.appendChild(kvItem("Recommended actions", a.recommended_actions));

  const actions = document.createElement("div");
  actions.className = "actions";

  const row1 = document.createElement("div");
  row1.className = "actions__row";
  const accept = document.createElement("button");
  accept.className = "btn btn--primary";
  accept.textContent = "Accept + Resolve";
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

  const snooze = document.createElement("button");
  snooze.className = "btn";
  snooze.textContent = "Snooze";
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
  row1.appendChild(snoozeDays);
  row1.appendChild(snooze);

  const row2 = document.createElement("div");
  row2.className = "actions__row";
  const note = document.createElement("textarea");
  note.className = "field__text";
  note.placeholder = "Optional note for AI review…";

  const review = document.createElement("button");
  review.className = "btn btn--ghost";
  review.textContent = "Review by AI";
  review.onclick = async () => {
    setBusy(true);
    try {
      const manager_note = note.value.trim() || null;
      const res = await api<Record<string, unknown>>(`/api/alerts/${a.alert_id}/review-ai`, {
        method: "POST",
        body: JSON.stringify({ manager_note }),
      });
      await refresh();
      alert(`AI review created:\n\n${JSON.stringify(res, null, 2)}`);
    } finally {
      setBusy(false);
    }
  };

  row2.appendChild(note);
  row2.appendChild(review);

  actions.appendChild(row1);
  actions.appendChild(row2);

  container.appendChild(title);
  container.appendChild(summary);
  container.appendChild(grid);
  container.appendChild(actions);

  el.detail.innerHTML = "";
  el.detail.appendChild(container);
}

function renderDashboard(dash: Dashboard): void {
  el.dashMeta.textContent = `${dash.start_day} → ${dash.end_day}`;
  el.dashboard.innerHTML = "";

  const totalsByLoc = new Map<number, { revenue: number; tx: number }>();
  for (const row of dash.daily_revenue) {
    const prev = totalsByLoc.get(row.location_id) ?? { revenue: 0, tx: 0 };
    prev.revenue += Number(row.revenue);
    prev.tx += Number(row.tx_count);
    totalsByLoc.set(row.location_id, prev);
  }

  const summary = document.createElement("div");
  summary.className = "kv";
  const lines: string[] = [];
  for (const [loc, total] of [...totalsByLoc.entries()].sort((a, b) => a[0] - b[0])) {
    lines.push(`loc ${loc}: $${total.revenue.toFixed(2)} · ${total.tx} tx`);
  }
  summary.innerHTML = `<div class=\"kv__k\">Revenue totals</div><div class=\"kv__v\">${escapeHtml(
    lines.join("\n") || "No revenue rows"
  )}</div>`;

  const patterns = document.createElement("div");
  patterns.className = "kv";
  patterns.innerHTML = `<div class=\"kv__k\">Top alert patterns</div><div class=\"kv__v\">${escapeHtml(
    dash.top_alert_patterns
      .slice(0, 10)
      .map((p) => `loc ${p.location_id} · ${p.severity} · ${p.alert_type}: ${p.n}`)
      .join("\n") || "No alerts yet"
  )}</div>`;

  el.dashboard.appendChild(summary);
  el.dashboard.appendChild(patterns);
}

async function refresh(): Promise<void> {
  state = await api<EngineState>("/api/state");
  alerts = await api<AlertRow[]>("/api/alerts?limit=200");
  const dash = await api<Dashboard>("/api/dashboard?days=14");
  if (selectedAlertId && !alerts.some((a) => a.alert_id === selectedAlertId)) {
    selectedAlertId = null;
  }
  renderState();
  renderAlerts();
  renderDetail();
  renderDashboard(dash);
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

