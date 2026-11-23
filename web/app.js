/* 事件仓位图筛选前端：原生 JS，无依赖 */
"use strict";

const $ = (sel) => document.querySelector(sel);
const FIELD_LABELS = { region: "地区（单值）", channel: "渠道（单值）", tags: "标签（多值）" };

const state = {
  overview: null,
  selected: new Map(),   // field -> Set(values)
  query: null,           // 当前选中的查询会话
  page: 0,
  pageSize: 15,
  pollTimer: null,
};

async function api(path, opts = {}) {
  const resp = await fetch(path, {
    method: opts.method || "GET",
    headers: { "Content-Type": "application/json" },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await resp.json();
  if (!resp.ok) throw new Error(`${data.type || "Error"}: ${data.error}`);
  return data;
}

function buildFilter() {
  const conditions = [];
  for (const [field, values] of state.selected) {
    if (values.size) conditions.push({ field, values: [...values] });
  }
  return {
    combinator: document.querySelector('input[name="combinator"]:checked').value,
    conditions,
  };
}

// ---------------- 概览与筛选面板 ----------------
async function refreshOverview() {
  state.overview = await api("/api/overview");
  const o = state.overview;
  $("#status-bar").textContent =
    `当前代 gen-${String(o.current_gen).padStart(6, "0")} · ` +
    `存活 ${o.live_records} 条 · 总行号 ${o.total_rows} · ` +
    `已删除行 ${o.deleted_rows} · 已更正行 ${o.updated_rows} · LSN ${o.current_lsn} · ` +
    `位图后端 ${o.bitmap_backend}`;
  renderFieldGroups();
  renderGenerations();
  if (o.rejected.length) {
    flash(`检测到被拒用隔离的代次: ${o.rejected.join(", ")}`, "err");
  }
}

function renderFieldGroups() {
  const host = $("#field-groups");
  host.innerHTML = "";
  for (const field of ["region", "channel", "tags"]) {
    const wrap = document.createElement("div");
    wrap.className = "field-group";
    const title = document.createElement("h3");
    title.textContent = FIELD_LABELS[field];
    wrap.appendChild(title);
    const chips = document.createElement("div");
    chips.className = "chips";
    if (!state.selected.has(field)) state.selected.set(field, new Set());
    const picked = state.selected.get(field);
    for (const item of state.overview.distinct[field] || []) {
      const chip = document.createElement("span");
      chip.className = "chip" + (picked.has(item.value) ? " active" : "") +
        (item.is_empty ? " empty-val" : "");
      const label = item.is_empty
        ? (field === "tags" ? "∅ 空集合" : "∅ 空值")
        : item.value;
      chip.innerHTML = `${label}<span class="cnt">${item.count}</span>`;
      chip.onclick = () => {
        picked.has(item.value) ? picked.delete(item.value) : picked.add(item.value);
        renderFieldGroups();
      };
      chips.appendChild(chip);
    }
    wrap.appendChild(chips);
    host.appendChild(wrap);
  }
}

function renderGenerations() {
  const host = $("#gen-list");
  host.innerHTML = "";
  for (const g of state.overview.generations) {
    const card = document.createElement("div");
    card.className = "gen-card" + (g.is_current ? " current" : "");
    card.innerHTML =
      `<div><b>gen-${String(g.gen_id).padStart(6, "0")}</b>${g.is_current ? "（CURRENT）" : ""}</div>` +
      `<div class="muted">行号数: ${g.rows ?? "?"} · 磁盘: ${formatBytes(g.size_bytes)}</div>` +
      (g.pins ? `<div class="pinned">📌 被 ${g.pins} 个旧查询绑定，不可回收</div>` :
                `<div class="muted">无绑定，可在下次整理后回收</div>`);
    host.appendChild(card);
  }
}

// ---------------- 查询 ----------------
async function runQuery(indexEnabled) {
  try {
    const q = await api("/api/query", {
      method: "POST",
      body: { filter: buildFilter(), index_enabled: indexEnabled },
    });
    state.query = q;
    state.page = 0;
    await renderQuery();
    await refreshQueryList();
    await refreshOverview();
    flash(`${indexEnabled ? "索引" : "扫描"}查询完成，绑定 gen-${String(q.gen_id).padStart(6, "0")}`, "ok");
  } catch (e) { flash(e.message, "err"); }
}

async function renderQuery() {
  const q = state.query;
  if (!q) return;
  const data = await api(
    `/api/query/${q.query_id}/preview?offset=${state.page * state.pageSize}&limit=${state.pageSize}`
  );
  $("#result-meta").innerHTML =
    `查询 <b>${q.query_id.slice(0, 8)}</b> · 代次 <b>gen-${String(data.gen_id).padStart(6, "0")}</b> · ` +
    `命中 <b>${data.total_hits}</b> 行 · 绑定 LSN ${data.created_lsn}` +
    (data.stale ? ' <span class="banner warn" style="display:inline-block;padding:2px 8px">绑定后数据已变化（LSN ' +
      data.current_lsn + '），结果仍按绑定时刻的代次与删除掩码读取</span>' : "");

  // 读取对比：如果存在同一筛选的另一条路径查询，一并展示
  await renderReadCompare(q);

  const tbl = $("#preview-table");
  tbl.classList.remove("hidden");
  const tbody = tbl.querySelector("tbody");
  tbody.innerHTML = "";
  for (const row of data.rows) {
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td>${row.ordinal}</td><td>${row.event_id}</td>` +
      `<td>${fmtEmpty(row.record.region)}</td><td>${fmtEmpty(row.record.channel)}</td>` +
      `<td>${row.record.tags.length ? row.record.tags.map(t => `<span class="tag-pill">${t}</span>`).join("") : '<span class="muted">∅ 空集合</span>'}</td>` +
      `<td class="muted">${JSON.stringify(row.record.payload || {})}</td>`;
    tbody.appendChild(tr);
  }
  const pages = Math.max(1, Math.ceil(data.total_hits / state.pageSize));
  $("#page-info").textContent = `第 ${state.page + 1}/${pages} 页`;
  $("#prev-page").disabled = state.page === 0;
  $("#next-page").disabled = state.page >= pages - 1;
}

async function renderReadCompare(current) {
  // 用相同 filter 再跑一次相反路径做对比（创建独立查询会话）
  const box = $("#read-compare");
  const body = $("#read-compare-body");
  box.classList.remove("hidden");
  body.innerHTML = "";
  const addRow = (q) => {
    const s = q.read_stat;
    body.insertAdjacentHTML("beforeend",
      `<tr><td>${s.path === "index" ? "位图索引" : "全表扫描"}</td>` +
      `<td>${formatBytes(s.bytes_read)}</td><td>${s.bitmap_files}</td>` +
      `<td>${s.record_lines_read}</td><td>${q.hits}</td></tr>`);
  };
  addRow(current);
  try {
    const other = await api("/api/query", {
      method: "POST",
      body: { filter: buildFilter(), index_enabled: current.read_stat.path !== "index" },
    });
    addRow(other);
  } catch (e) { /* 忽略对比查询失败 */ }
}

async function refreshQueryList() {
  const data = await api("/api/queries");
  const host = $("#query-list");
  if (!data.queries.length) {
    host.innerHTML = '<p class="muted">暂无查询</p>';
    return;
  }
  host.innerHTML = "";
  for (const q of data.queries) {
    const div = document.createElement("div");
    div.className = "query-item" + (state.query && q.query_id === state.query.query_id ? " selected" : "");
    const conds = q.conditions.map(c =>
      `${FIELD_LABELS[c.field].split("（")[0]}=[${c.values.map(v => v === "__EMPTY__" ? "∅" : v).join(",")}]`
    ).join(` ${q.combinator} `);
    div.innerHTML =
      `<div>${q.query_id.slice(0, 8)} · ${q.index_enabled ? "索引" : "扫描"} · 命中 ${q.hits}</div>` +
      `<div class="qmeta">gen-${String(q.gen_id).padStart(6, "0")} · ${conds || "无条件（全量）"} · LSN ${q.created_lsn}</div>`;
    div.onclick = async () => {
      const full = { query_id: q.query_id, read_stat: q.index_enabled ? { path: "index" } : { path: "scan" } };
      state.query = full; state.page = 0;
      await renderQuery();
      await refreshQueryList();
    };
    host.appendChild(div);
  }
}

// ---------------- 整理 ----------------
async function refreshCompaction() {
  const c = await api("/api/compaction");
  const pct = c.total_rows ? Math.round((c.done_rows / c.total_rows) * 100) : 0;
  const phaseMap = {
    idle: "空闲", running: "运行中", paused: "已暂停", committing: "切换中",
    done: "完成", conflict: "冲突", aborted: "已中止",
  };
  $("#compact-status").innerHTML =
    `阶段: <b>${phaseMap[c.phase] || c.phase}</b> · 目标 gen-${String(c.target_gen).padStart(6, "0")}<br>` +
    `基线 LSN ${c.base_lsn}${c.fence_lsn ? ` · 水位 LSN ${c.fence_lsn}` : ""} · ` +
    `当前 LSN ${c.current_lsn}<br>` +
    `进度: ${c.done_rows}/${c.total_rows}（${c.stage || "-"}）` +
    (c.phase === "running" || c.phase === "paused"
      ? `<div class="progress"><div style="width:${pct}%"></div></div>` : "") +
    (c.error ? `<div class="banner err" style="margin-top:6px">${c.error}</div>` : "");
}

// ---------------- 写入操作 ----------------
async function submitForm(formId, path, buildBody, method) {
  const form = $(formId);
  const data = Object.fromEntries(new FormData(form).entries());
  try {
    const body = buildBody(data);
    const url = path.includes("{id}") ? path.replace("{id}", encodeURIComponent(data.event_id)) : path;
    const result = await api(url, { method, body });
    flash(`操作成功: ${JSON.stringify(result)}`, "ok");
    form.reset();
    await refreshOverview();
  } catch (e) { flash(e.message, "err"); }
}

// ---------------- 小工具 ----------------
function fmtEmpty(v) { return v === "__EMPTY__" ? '<span class="muted">∅ 空值</span>' : v; }
function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}
let flashTimer = null;
function flash(msg, kind) {
  const el = $("#ops-msg");
  el.className = `ops-msg banner ${kind}`;
  el.textContent = msg;
  clearTimeout(flashTimer);
  flashTimer = setTimeout(() => { el.className = "ops-msg"; el.textContent = ""; }, 6000);
}

// ---------------- 事件绑定 ----------------
$("#btn-query-index").onclick = () => runQuery(true);
$("#btn-query-scan").onclick = () => runQuery(false);
$("#prev-page").onclick = async () => { state.page--; await renderQuery(); };
$("#next-page").onclick = async () => { state.page++; await renderQuery(); };
$("#close-query").onclick = async () => {
  if (!state.query) return;
  try {
    const r = await api("/api/query/close", { method: "POST", body: { query_id: state.query.query_id } });
    flash(`查询已关闭${r.reaped.length ? "，回收旧代: " + r.reaped.join(", ") : "（旧代仍被其他查询绑定）"}`, "ok");
    state.query = null;
    $("#preview-table").classList.add("hidden");
    $("#read-compare").classList.add("hidden");
    $("#result-meta").textContent = "尚未执行查询";
    await refreshQueryList();
    await refreshOverview();
  } catch (e) { flash(e.message, "err"); }
};

$("#btn-compact-start").onclick = async () => {
  try { await api("/api/compaction/start", { method: "POST", body: {} }); flash("整理已启动", "ok"); }
  catch (e) { flash(e.message, "err"); }
};
$("#btn-compact-pause").onclick = async () => {
  try { await api("/api/compaction/control", { method: "POST", body: { action: "pause" } }); }
  catch (e) { flash(e.message, "err"); }
};
$("#btn-compact-resume").onclick = async () => {
  try { await api("/api/compaction/control", { method: "POST", body: { action: "resume" } }); }
  catch (e) { flash(e.message, "err"); }
};
$("#btn-compact-abort").onclick = async () => {
  try { await api("/api/compaction/control", { method: "POST", body: { action: "abort" } }); }
  catch (e) { flash(e.message, "err"); }
};

$("#put-form").onsubmit = (e) => {
  e.preventDefault();
  submitForm("#put-form", "/api/events", (d) => ({
    event_id: d.event_id,
    region: d.region || null,
    channel: d.channel || null,
    tags: d.tags ? d.tags.split(",").map(s => s.trim()).filter(Boolean) : [],
  }), "POST");
};
$("#update-form").onsubmit = (e) => {
  e.preventDefault();
  submitForm("#update-form", "/api/events/{id}/update", (d) => {
    const changes = {};
    if (d.region) changes.region = d.region;
    if (d.channel) changes.channel = d.channel;
    if (d.tags) changes.tags = d.tags.split(",").map(s => s.trim()).filter(Boolean);
    return { changes };
  }, "POST");
};
$("#delete-form").onsubmit = (e) => {
  e.preventDefault();
  submitForm("#delete-form", "/api/events/{id}/delete", () => ({}), "POST");
};

// 轮询：整理进度与代次状态
setInterval(() => { refreshCompaction(); }, 1000);
setInterval(() => { refreshOverview(); refreshQueryList(); }, 4000);
refreshOverview();
refreshCompaction();
refreshQueryList();
