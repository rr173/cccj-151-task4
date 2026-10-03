/* 离线车队回执中继 —— 控制台前端（原生 JS，无构建） */
const $ = (id) => document.getElementById(id);
const API = "/relay/api";
let timer = null;
let selectedBatch = null;

// ------------------------------------------------------------ 通用
function toast(msg, kind = "") {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast " + kind;
  setTimeout(() => t.classList.add("hidden"), 3200);
  t.classList.remove("hidden");
}

async function api(path, opts = {}) {
  const resp = await fetch(API + path, {
    method: opts.method || "GET",
    headers: opts.body ? { "Content-Type": "application/json" } : {},
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  let data = null;
  try { data = await resp.json(); } catch { /* ignore */ }
  if (!resp.ok) {
    const err = new Error((data && data.error) || `HTTP ${resp.status}`);
    err.status = resp.status;
    err.data = data;
    throw err;
  }
  return data;
}

function fmtTs(ts) {
  if (ts === null || ts === undefined) return "";
  const d = new Date(ts * 1000);
  return d.toLocaleString("zh-CN", { hour12: false });
}
function fmtBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  return (n / 1024 / 1024).toFixed(2) + " MB";
}
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
const STATE_BADGE = {
  buffered: "缓冲", queued: "待转发", dispatching: "转发中",
  retry_wait: "等待重试", forwarded: "已转发", rejected_terminal: "终态拒绝",
};
function stateBadge(s) {
  return `<span class="badge ${s}">${STATE_BADGE[s] || s}</span>`;
}

// ------------------------------------------------------------ Tab 切换
document.querySelectorAll(".topbar nav button").forEach((btn) => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".topbar nav button").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab").forEach((s) => s.classList.remove("active"));
    btn.classList.add("active");
    $("tab-" + btn.dataset.tab).classList.add("active");
    if (btn.dataset.tab === "terminal") loadTerminal();
    if (btn.dataset.tab === "retries") loadQueue();
    if (btn.dataset.tab === "archive") loadArchives();
    if (btn.dataset.tab === "audit") loadAudit();
    if (btn.dataset.tab === "stub") loadStub();
  });
});

// ------------------------------------------------------------ 批次总览
async function loadHealth() {
  try {
    const h = await api("/health");
    $("health").textContent =
      `适配器: ${h.adapter} | 派发 ${h.forwarder.dispatched} 成功 ${h.forwarder.acked} ` +
      `重试 ${h.forwarder.retries} 终态 ${h.forwarder.terminal} | 启动回收 ${h.startup.recovered_leases}`;
    $("health").classList.remove("bad");
    if (h.adapter === "stub") {
      document.querySelector('[data-tab="stub"]').classList.remove("hidden");
    }
  } catch {
    $("health").textContent = "中继不可达";
    $("health").classList.add("bad");
  }
}

async function loadBatches() {
  const d = await api("/batches");
  const box = $("batchCards");
  if (!d.batches.length) {
    box.innerHTML = `<p class="muted">还没有导入执行包，请到「执行包导入」页导入。</p>`;
  } else {
    box.innerHTML = d.batches.map((b) => {
      const c = b.counts;
      const gapTxt = b.gaps.length
        ? `<div class="gapline">序号缺口：${b.gaps.join(", ")}（游标待 ${b.cursor}）</div>`
        : (b.status === "collecting" || b.status === "forwarding")
          ? `<div class="muted" style="font-size:12px;margin-top:6px">无缺口，游标 @${b.cursor}</div>`
          : "";
      const blocker = b.blocker
        ? `<div class="gapline">⛔ 游标冻结于序号 ${b.blocker.seq}（${esc(b.blocker.result_code)}），后续不得越过</div>`
        : "";
      return `<div class="card" data-ref="${esc(b.batch_ref)}">
        <h4>${esc(b.batch_ref)} <span class="badge ${b.status}">${statusText(b.status)}</span></h4>
        <div class="sub">${esc(b.batch_summary)}</div>
        <div class="sub mono">hash ${esc(b.content_hash.slice(0, 16))}… · 导入于 ${fmtTs(b.imported_at)}</div>
        <div class="metrics">
          <span class="metric">已转发 <b>${c.forwarded}</b></span>
          <span class="metric">待转发 <b>${c.queued}</b></span>
          <span class="metric">缓冲 <b>${c.buffered}</b></span>
          <span class="metric">重试中 <b>${c.retry_wait + c.dispatching}</b></span>
          <span class="metric">终态拒绝 <b class="${c.rejected_terminal ? "err-text" : ""}">${c.rejected_terminal}</b></span>
        </div>
        ${gapTxt}${blocker}
      </div>`;
    }).join("");
    box.querySelectorAll(".card").forEach((card) =>
      card.addEventListener("click", () => loadBatchDetail(card.dataset.ref)));
  }
  if (selectedBatch) await loadBatchDetail(selectedBatch);
}

function statusText(s) {
  return { idle: "等待回执", collecting: "收集中", forwarding: "转发中",
           blocked_terminal: "终态阻塞", all_forwarded: "全部转发完成",
           archived: "已封存(只读)" }[s] || s;
}

async function loadBatchDetail(ref) {
  selectedBatch = ref;
  const b = await api("/batches/" + encodeURIComponent(ref));
  const c = b.counts;
  const archivedBox = b.archived
    ? `<div class="archive-box">📦 <b>批次已封存（只读归档）</b>：归档编号
        <code>${esc(b.archive_id)}</code> · 封存于 ${fmtTs(b.sealed_at)} ·
        已回收约 ${fmtBytes(b.bytes_reclaimed)}。${esc(b.message || "")}
        ${b.restored ? "" : `可到「归档封存」页校验并恢复只读详情。`}</div>` : "";
  const blocked = b.blocker
    ? `<div class="blocker-box">⛔ <b>终态拒绝（游标冻结）</b>：序号 ${b.blocker.seq}
        回执 ${esc(b.blocker.receipt_no)} 被主系统拒绝（<code>${esc(b.blocker.result_code)}</code>）。
        原因：${esc(b.blocker.result_detail)}。后续回执无法越过该项；
        只有确认主系统终态解除后，到「失败重试」页人工确认重投。</div>` : "";
  const buffered = b.buffered_items.length
    ? `<div><b>缓冲项（持久等待缺口）：</b> ${b.buffered_items.map((x) =>
        `<span class="badge buffered">#${x.seq} ${esc(x.receipt_no)}</span>`).join(" ")}</div>` : "";
  const rows = b.receipts.map((r) => `<tr>
      <td>${r.seq}</td>
      <td>${esc(r.receipt_no)}</td>
      <td>${r.type_label}</td>
      <td>${stateBadge(r.state)}</td>
      <td>${r.attempts}</td>
      <td class="${r.state === "forwarded" ? "ok-text" : r.last_error || r.result_detail ? "err-text" : ""}">
        ${esc(r.last_error || r.result_detail || "")}
        ${r.result_code && r.result_code !== "ack" ? `<div class="muted mono">${esc(r.result_code)}</div>` : ""}</td>
      <td>${r.forwarded_at ? "✅ " + fmtTs(r.forwarded_at) : ""}
          ${r.rejected_at ? "⛔ " + fmtTs(r.rejected_at) : ""}
          ${r.next_retry_at ? "↻ " + fmtTs(r.next_retry_at) : ""}</td>
    </tr>`).join("");
  $("batchDetail").classList.remove("hidden");
  $("batchDetail").innerHTML = `
    <div class="row between">
      <h2 style="margin:0">批次 ${esc(b.batch_ref)}</h2>
      <button onclick="document.getElementById('batchDetail').classList.add('hidden')">关闭</button>
    </div>
    <p class="hint">${esc(b.batch_summary)} · 游标 next_seq=<b>${b.cursor}</b>
      · 最大已见序号 ${b.max_seen ?? "—"} · 缺口 [${b.gaps.join(", ") || "无"}]</p>
    ${archivedBox}${blocked}${buffered}
    <table class="tbl"><thead><tr>
      <th>序号</th><th>回执号</th><th>类型</th><th>状态</th><th>尝试</th>
      <th>转发结果 / 失败原因</th><th>时间</th></tr></thead>
      <tbody>${rows || `<tr><td colspan="7" class="muted">尚无回执</td></tr>`}</tbody></table>`;
}

// ------------------------------------------------------------ 执行包导入
const SAMPLE_PKG = {
  batch_ref: "B-DEMO-1",
  package_id: "PKG-2026-1002",
  batch_summary: "波次1 · 车辆V-07 · 网格G3 → 避难点S1（共 4 户 9 人）",
  seq_base: 1,
  issued_at: "2026-10-02T07:30:00Z",
  items: [
    { household_ref: "H-101", members: 3, seats_taken: 3 },
    { household_ref: "H-102", members: 2, seats_taken: 2 },
    { household_ref: "H-103", members: 2, seats_taken: 2 },
    { household_ref: "H-104", members: 2, seats_taken: 3, pets: 1 },
  ],
  vehicle: { code: "V-07", seats: 11 },
  destination: { code: "S1", name: "城北体育馆避难点" },
};
$("loadSampleBtn").onclick = () => {
  $("pkgJson").value = JSON.stringify(SAMPLE_PKG, null, 2);
};
$("importBtn").onclick = async () => {
  let pkg;
  try { pkg = JSON.parse($("pkgJson").value); }
  catch { $("importResult").textContent = "JSON 解析失败"; return; }
  try {
    const r = await api("/packages/import", { method: "POST", body: { package: pkg } });
    $("importResult").textContent = JSON.stringify(r, null, 2);
    toast(r.replay ? "相同包再次导入：返回原结果（幂等）" : "执行包已不可变存档", "ok");
    loadBatches();
  } catch (e) {
    $("importResult").textContent = JSON.stringify(e.data || { error: e.message }, null, 2);
    toast(e.message, "err");
  }
};

// ------------------------------------------------------------ 现场终端
async function loadTerminal() {
  const d = await api("/terminal/manifest");
  $("terminalRows").innerHTML = d.batches.map((b) => `<tr>
    <td><b>${esc(b.batch_ref)}</b>${b.archived ? ' <span class="badge archived">已封存</span>' : ""}</td>
    <td>${esc(b.batch_summary)}</td>
    <td class="mono">${esc(b.content_hash.slice(0, 20))}…</td>
    <td>${b.forwarded_receipts}</td>
    <td>${fmtTs(b.imported_at)}</td>
    <td><button data-ref="${esc(b.batch_ref)}">下载</button></td></tr>`).join("")
    || `<tr><td colspan="6" class="muted">暂无批次</td></tr>`;
  $("terminalRows").querySelectorAll("button").forEach((btn) =>
    btn.addEventListener("click", async () => {
      const pkg = await api("/terminal/packages/" + encodeURIComponent(btn.dataset.ref));
      $("terminalPkg").classList.remove("hidden");
      $("terminalPkg").textContent = JSON.stringify(pkg, null, 2);
    }));
}

// ------------------------------------------------------------ 回执上传
function makeReceipt(seq, no, type, summary) {
  return { seq, receipt_no: no, type, batch_summary: summary,
           at: new Date().toISOString(), note: `现场回执 ${no}` };
}
$("outOfOrderSampleBtn").onclick = () => {
  $("rcptJson").value = JSON.stringify({
    batch_ref: "B-DEMO-1",
    batch_summary: SAMPLE_PKG.batch_summary,
    receipts: [
      makeReceipt(1, "R-1001", "departed", SAMPLE_PKG.batch_summary),
      makeReceipt(3, "R-1003", "arrived", SAMPLE_PKG.batch_summary),
    ],
  }, null, 2);
};
$("uploadBtn").onclick = async () => {
  let body;
  try { body = JSON.parse($("rcptJson").value); }
  catch { $("uploadResult").textContent = "JSON 解析失败"; return; }
  try {
    const r = await api("/receipts/batch", { method: "POST", body });
    $("uploadResult").textContent = JSON.stringify(r, null, 2);
    toast(`已处理 ${r.count} 条回执`, "ok");
    loadBatches();
  } catch (e) {
    $("uploadResult").textContent = JSON.stringify(e.data || { error: e.message }, null, 2);
    toast(e.message, "err");
  }
};

// ------------------------------------------------------------ 重试
async function loadQueue() {
  const d = await api("/queue");
  $("queueRows").innerHTML = d.pending.map((r) => {
    let action = "";
    if (r.state === "retry_wait" || r.state === "dispatching") {
      action = `<button data-act="retry" data-ref="${esc(r.batch_ref)}" data-seq="${r.seq}">立即重试</button>`;
    } else if (r.state === "rejected_terminal") {
      action = `<button class="warn" data-act="ack" data-ref="${esc(r.batch_ref)}" data-seq="${r.seq}">
        确认终态已解除并重投</button>`;
    } else {
      action = `<span class="muted">—</span>`;
    }
    return `<tr>
      <td>${esc(r.batch_ref)}</td><td>${r.seq}</td><td>${esc(r.receipt_no)}</td>
      <td>${r.receipt_type}</td><td>${stateBadge(r.state)}</td><td>${r.attempts}</td>
      <td class="err-text">${esc(r.last_error || r.result_code || "")}</td>
      <td>${r.next_retry_at ? fmtTs(r.next_retry_at) : ""}</td>
      <td>${action}</td></tr>`;
  }).join("") || `<tr><td colspan="9" class="muted">队列已清空</td></tr>`;

  $("queueRows").querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const note = prompt("操作备注（将进入审计）：", "操作员现场确认") || "";
      try {
        if (btn.dataset.act === "retry") {
          await api("/retries", { method: "POST",
            body: { batch_ref: btn.dataset.ref, seq: Number(btn.dataset.seq), note } });
        } else {
          await api(`/batches/${encodeURIComponent(btn.dataset.ref)}/receipts/${btn.dataset.seq}/ack-terminal`,
            { method: "POST", body: { note } });
        }
        toast("已提交", "ok");
        loadQueue(); loadBatches();
      } catch (e) { toast(e.message, "err"); }
    });
  });
}
$("queueRefresh").onclick = loadQueue;

// ------------------------------------------------------------ 归档封存
async function loadArchives() {
  const [est, arch] = await Promise.all([api("/archives/estimate"), api("/archives")]);
  $("archSummary").textContent =
    `热库回执明细约 ${fmtBytes(est.hot_receipt_bytes)} · ` +
    `可封存回收约 ${fmtBytes(est.total_reclaimable_bytes)} · ` +
    `已封存 ${arch.archives.length} 个批次`;

  const pending = est.batches.filter((b) => !b.archived);
  $("archEstRows").innerHTML = pending.map((b) => {
    const blockers = (b.blockers || []).map((x) => esc(x.message)).join("；");
    return `<tr>
      <td><b>${esc(b.batch_ref)}</b></td>
      <td><span class="badge ${b.status}">${statusText(b.status)}</span></td>
      <td>${b.receipt_count}（已转发 ${b.forwarded}）</td>
      <td>${fmtBytes(b.bytes_estimate)}</td>
      <td>${b.eligible ? '<span class="ok-text">✔ 可封存</span>'
                       : '<span class="err-text">✘ 未完结</span>'}</td>
      <td class="err-text">${blockers}</td>
      <td>${b.eligible
        ? `<button class="primary" data-seal="${esc(b.batch_ref)}">执行封存</button>`
        : '<span class="muted">—</span>'}</td></tr>`;
  }).join("") || `<tr><td colspan="7" class="muted">没有待评估批次</td></tr>`;

  $("archRows").innerHTML = arch.archives.map((a) => `<tr>
    <td><b>${esc(a.batch_ref)}</b></td>
    <td class="mono">${esc(a.archive_id)}</td>
    <td class="mono" title="${esc(a.manifest_hash)}">${esc(a.manifest_hash.slice(0, 16))}…</td>
    <td>${a.receipt_count} / ${a.audit_count}</td>
    <td>${fmtBytes(a.bytes_estimate)}</td>
    <td>${fmtTs(a.sealed_at)}</td>
    <td>${a.restored_at ? "✅ 已恢复 " + fmtTs(a.restored_at) : '<span class="muted">未恢复</span>'}
        ${a.last_verify_ok === false ? '<div class="err-text">上次校验失败</div>' : ""}</td>
    <td><div class="row" style="gap:4px;flex-wrap:wrap">
      <button data-dl="${esc(a.batch_ref)}">下载归档</button>
      <button data-verify="${esc(a.batch_ref)}">校验</button>
      <button data-restore="${esc(a.batch_ref)}">校验并恢复只读详情</button>
      ${a.restored_at ? `<button data-detail="${esc(a.batch_ref)}">查看</button>` : ""}
    </div></td></tr>`).join("")
    || `<tr><td colspan="8" class="muted">暂无归档</td></tr>`;

  $("archEstRows").querySelectorAll("button[data-seal]").forEach((btn) =>
    btn.addEventListener("click", () => sealBatch(btn.dataset.seal)));
  $("archRows").querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const ref = btn.dataset.dl || btn.dataset.verify || btn.dataset.restore || btn.dataset.detail;
      try {
        if (btn.dataset.dl) {
          window.open(`/relay/api/archives/${encodeURIComponent(ref)}/download`, "_blank");
          toast("归档包已开始下载（字节一致，可离线校验）", "ok");
        } else if (btn.dataset.verify) {
          const r = await api(`/archives/${encodeURIComponent(ref)}/verify`, { method: "POST", body: {} });
          toast(`校验通过：${r.manifest_hash.slice(0, 16)}…`, "ok");
          loadArchives();
        } else if (btn.dataset.restore) {
          const r = await api(`/archives/${encodeURIComponent(ref)}/restore`, { method: "POST", body: {} });
          toast(r.message, "ok");
          renderArchivedDetail(r.detail);
          loadArchives();
        } else if (btn.dataset.detail) {
          const d = await api(`/archives/${encodeURIComponent(ref)}/detail`);
          renderArchivedDetail(d);
        }
      } catch (e) {
        toast(e.message, "err");
        loadArchives();
      }
    });
  });
}

async function sealBatch(ref) {
  if (!confirm(`确认封存批次 ${ref}？\n热库明细将迁入只读归档（操作不可逆，归档仍可下载/恢复只读查看）。`)) return;
  try {
    const r = await api("/archives/seal", { method: "POST", body: { batch_ref: ref } });
    toast(r.replay ? "该批次已封存过，返回首次结果" :
      `已封存 ${r.receipt_count} 条回执，回收约 ${fmtBytes(r.bytes_reclaimed)}`, "ok");
    loadArchives(); loadBatches();
  } catch (e) {
    toast(e.message, "err");
    loadArchives();
  }
}

function renderArchivedDetail(d) {
  const rows = (d.receipts || []).map((r) => `<tr>
    <td>${r.seq}</td><td>${esc(r.receipt_no)}</td><td>${r.type_label}</td>
    <td>${stateBadge(r.state)}</td><td>${r.attempts}</td>
    <td class="ok-text">${esc(r.result_detail || r.result_code || "")}</td>
    <td>${r.forwarded_at ? "✅ " + fmtTs(r.forwarded_at) : ""}</td></tr>`).join("");
  $("archDetail").classList.remove("hidden");
  $("archDetail").innerHTML = `
    <div class="row between">
      <h2 style="margin:0">归档 ${esc(d.batch_ref)} <span class="badge archived">只读</span></h2>
      <button onclick="document.getElementById('archDetail').classList.add('hidden')">关闭</button>
    </div>
    <div class="archive-box">📦 归档编号 <code>${esc(d.archive_id)}</code> ·
      校验摘要 <code>${esc(d.manifest_hash)}</code> · 封存于 ${fmtTs(d.sealed_at)} ·
      已回收约 ${fmtBytes(d.bytes_reclaimed)}。明细来自只读归档包，不会回到转发队列；
      原审计时间线在「审计与冲突」页完整保留。</div>
    <p class="hint">${esc(d.batch_summary)} · 游标终值 ${d.cursor} ·
      回执 ${d.counts.forwarded} 条（全部已转发）</p>
    <table class="tbl"><thead><tr><th>序号</th><th>回执号</th><th>类型</th><th>最终状态</th>
      <th>尝试</th><th>转发结果</th><th>时间</th></tr></thead>
      <tbody>${rows || `<tr><td colspan="7" class="muted">该批次无回执</td></tr>`}</tbody></table>`;
}
$("archRefresh").onclick = loadArchives;

// ------------------------------------------------------------ 审计
async function loadAudit() {
  const [a, cf] = await Promise.all([api("/audit"), api("/conflicts")]);
  $("auditRows").innerHTML = a.events.map((e) => `<tr>
    <td class="mono">${fmtTs(e.ts)}</td><td>${esc(e.kind)}</td>
    <td>${esc(e.batch_ref)}</td><td>${e.seq ?? ""}</td>
    <td>${esc(e.receipt_no)}</td><td>${esc(e.actor)}</td>
    <td class="mono" style="max-width:420px;word-break:break-all">${esc(e.detail)}</td></tr>`).join("");
  $("conflictRows").innerHTML = cf.conflicts.map((e) => `<tr>
    <td class="mono">${fmtTs(e.ts)}</td><td><span class="badge rejected_terminal">${esc(e.reason)}</span></td>
    <td>${esc(e.batch_ref)}</td><td>${e.seq ?? ""}</td>
    <td>${esc(e.receipt_no)}</td>
    <td class="mono" style="max-width:420px;word-break:break-all">${esc(e.detail)}</td></tr>`).join("")
    || `<tr><td colspan="6" class="muted">暂无冲突记录</td></tr>`;
}
$("auditRefresh").onclick = loadAudit;

// ------------------------------------------------------------ 桩适配器
async function loadStub() {
  let d;
  try { d = await api("/admin/state"); }
  catch (e) { $("stubPanel").innerHTML = `<p class="err-text">${esc(e.message)}</p>`; return; }
  if (d.adapter !== "stub") {
    $("stubPanel").innerHTML = `<p class="muted">当前适配器为 ${esc(d.adapter)}（设置
      RELAY_CALLBACK_ADAPTER=stub 可启用离线演示桩）。</p>`;
    return;
  }
  const calls = (d.stub.calls || []).slice().reverse().slice(0, 30);
  $("stubPanel").innerHTML = `
    <p>当前规则：<code>${esc(JSON.stringify(d.stub.rules || {}))}</code></p>
    <textarea id="stubRules" rows="6">${esc(JSON.stringify(d.stub.rules || {}, null, 2))}</textarea>
    <div class="row gap">
      <button class="primary" id="stubApply">应用规则并唤醒转发</button>
      <button id="stubReset">清空调用记录</button>
    </div>
    <h3>最近 30 次适配器调用（含重放，可据此验证恰好一次）</h3>
    <table class="tbl"><thead><tr><th>#</th><th>幂等键</th><th>批次</th><th>序号</th>
      <th>回执号</th><th>命中规则</th></tr></thead>
      <tbody>${calls.map((c, i) => `<tr><td>${calls.length - i}</td>
        <td class="mono">${esc(c.idempotency_key)}</td><td>${esc(c.batch_ref)}</td>
        <td>${c.seq}</td><td>${esc(c.receipt_no)}</td><td>${esc(c.rule)}</td></tr>`).join("")}
      </tbody></table>`;
  $("stubApply").onclick = async () => {
    try {
      const rules = JSON.parse($("stubRules").value);
      await api("/admin/stub", { method: "POST", body: { rules } });
      toast("规则已应用", "ok"); loadStub();
    } catch (e) { toast(e.message, "err"); }
  };
  $("stubReset").onclick = async () => {
    await api("/admin/stub", { method: "POST", body: { reset_calls: true } });
    loadStub();
  };
}

// ------------------------------------------------------------ 自动刷新
$("refreshBtn").onclick = () => { loadBatches(); loadHealth(); };
function armTimer() {
  clearInterval(timer);
  if ($("autoRefresh").checked) {
    timer = setInterval(() => {
      loadHealth();
      loadBatches();
      const active = document.querySelector(".tab.active");
      if (active && active.id === "tab-retries") loadQueue();
      if (active && active.id === "tab-archive") loadArchives();
      if (active && active.id === "tab-audit") loadAudit();
    }, 2000);
  }
}
$("autoRefresh").onchange = armTimer;

loadHealth(); loadBatches(); armTimer();
