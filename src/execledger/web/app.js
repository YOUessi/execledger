const $ = (id) => document.getElementById(id);

const state = {
  selectedId: null,
  execution: null,
  effects: [],
  snapshots: [],
  source: null,
  activeTab: "combined",
};

const terminalStates = new Set([
  "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELLED", "INTERRUPTED",
]);

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function shortId(value, n = 12) {
  if (!value) return "—";
  return value.length <= n ? value : value.slice(0, n) + "…";
}

function formatTime(value) {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

function badge(status) {
  return `<span class="badge ${escapeHtml(status)}">${escapeHtml(status)}</span>`;
}

function toast(message, timeout = 3200) {
  const node = $("toast");
  node.textContent = message;
  node.classList.remove("hidden");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => node.classList.add("hidden"), timeout);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: {
      "Accept": "application/json",
      ...(options.body ? {"Content-Type": "application/json"} : {}),
      ...(options.headers || {}),
    },
    ...options,
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      if (body.detail) detail = body.detail;
    } catch (_) {
      // Keep HTTP status when the response is not JSON.
    }
    throw new Error(detail);
  }
  if (response.status === 204) return null;
  return response.json();
}

async function refreshHealth() {
  try {
    const health = await api("/healthz");
    $("health-dot").className = "dot ok";
    $("health-text").textContent =
      `${health.worker_id || "worker"} · ${health.workers} slot${health.workers === 1 ? "" : "s"}`;
  } catch (error) {
    $("health-dot").className = "dot bad";
    $("health-text").textContent = "API unavailable";
  }
}

async function refreshExecutions() {
  try {
    const executions = await api("/v1/executions?limit=100");
    const list = $("execution-list");
    if (!executions.length) {
      list.innerHTML = '<div class="muted">No executions yet.</div>';
      return;
    }
    list.innerHTML = executions.map((item) => `
      <button class="execution-item ${item.id === state.selectedId ? "active" : ""}"
              data-execution-id="${escapeHtml(item.id)}">
        <div class="top">
          <code>${escapeHtml(shortId(item.id, 14))}</code>
          ${badge(item.status)}
        </div>
        <div class="time">${escapeHtml(formatTime(item.created_at))}</div>
      </button>
    `).join("");
    list.querySelectorAll("[data-execution-id]").forEach((node) => {
      node.addEventListener("click", () => selectExecution(node.dataset.executionId));
    });
  } catch (error) {
    toast(`Failed to list executions: ${error.message}`);
  }
}

function renderExecution() {
  const record = state.execution;
  if (!record) return;
  $("empty-state").classList.add("hidden");
  $("detail").classList.remove("hidden");
  $("execution-id").textContent = record.id;
  $("execution-status").className = `badge ${record.status}`;
  $("execution-status").textContent = record.status;
  $("metric-attempt").textContent = record.attempt;
  $("metric-worker").textContent = record.worker_id || "—";
  $("metric-exit").textContent = record.exit_code ?? "—";
  $("metric-created").textContent = formatTime(record.created_at);
  $("request-spec").textContent = JSON.stringify(record.spec, null, 2);
  $("cancel-job").disabled = terminalStates.has(record.status);
}

function outputForTab(tab) {
  const chunks = state.effects
    .filter((effect) => effect.kind === "output_chunk")
    .map((effect) => ({
      seq: effect.seq,
      stream: effect.payload.stream,
      data: typeof effect.payload.data === "string" ? effect.payload.data : "",
    }))
    .filter((chunk) => tab === "combined" || chunk.stream === tab)
    .sort((a, b) => a.seq - b.seq);
  return chunks.map((chunk) => chunk.data).join("");
}

function renderOutput() {
  const node = $("live-output");
  const atBottom = node.scrollHeight - node.scrollTop - node.clientHeight < 28;
  const output = outputForTab(state.activeTab);
  if (output) {
    node.textContent = output;
  } else if (state.execution) {
    const fallback = state.activeTab === "stderr"
      ? state.execution.stderr
      : state.activeTab === "stdout"
        ? state.execution.stdout
        : [state.execution.stdout, state.execution.stderr].filter(Boolean).join("");
    node.textContent = fallback || "No output captured.";
  }
  if (atBottom) node.scrollTop = node.scrollHeight;
}

function renderEffects() {
  const container = $("effects");
  if (!state.effects.length) {
    container.innerHTML = '<div class="muted">No effects recorded.</div>';
    return;
  }
  container.innerHTML = [...state.effects].reverse().map((effect) => `
    <div class="effect">
      <div class="seq">#${effect.seq}</div>
      <div class="kind">${escapeHtml(effect.kind)}</div>
      <div class="payload">${escapeHtml(JSON.stringify(effect.payload))}</div>
    </div>
  `).join("");
}

function renderAttempts(attempts) {
  const container = $("attempts");
  if (!attempts.length) {
    container.innerHTML = '<div class="muted">No attempts yet.</div>';
    return;
  }
  container.innerHTML = `
    <table class="table">
      <thead><tr><th>#</th><th>Worker</th><th>Status</th><th>Exit</th></tr></thead>
      <tbody>
        ${attempts.map((attempt) => `
          <tr>
            <td>${attempt.number}</td>
            <td title="${escapeHtml(attempt.worker_id)}">${escapeHtml(shortId(attempt.worker_id, 18))}</td>
            <td>${badge(attempt.status)}</td>
            <td>${attempt.exit_code ?? "—"}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

function snapshotButton(label, action, snapshot, extra = "") {
  return `<button class="ghost small" data-action="${action}"
    data-snapshot-id="${escapeHtml(snapshot.id)}" ${extra}>${label}</button>`;
}

function renderSnapshots() {
  const container = $("snapshots");
  if (!state.snapshots.length) {
    container.innerHTML = '<div class="muted">No snapshots yet.</div>';
    return;
  }
  container.innerHTML = state.snapshots.map((snapshot, index) => {
    const previous = index > 0 ? state.snapshots[index - 1] : null;
    const compare = previous
      ? snapshotButton(
          "Diff prev",
          "diff",
          snapshot,
          `data-before-id="${escapeHtml(previous.id)}"`
        )
      : "";
    return `
      <div class="snapshot">
        <div>${escapeHtml(snapshot.phase)}</div>
        <code title="${escapeHtml(snapshot.digest)}">${escapeHtml(snapshot.digest)}</code>
        <div class="snapshot-actions">
          ${compare}
          ${snapshotButton("Restore", "restore", snapshot)}
        </div>
      </div>
    `;
  }).join("");

  container.querySelectorAll('[data-action="diff"]').forEach((node) => {
    node.addEventListener("click", () => showDiff(node.dataset.beforeId, node.dataset.snapshotId));
  });
  container.querySelectorAll('[data-action="restore"]').forEach((node) => {
    node.addEventListener("click", () => restoreSnapshot(node.dataset.snapshotId));
  });
}

function renderDiffGroup(title, items, kind) {
  const empty = '<div class="muted">None</div>';
  return `
    <div class="diff-group">
      <h4>${escapeHtml(title)} · ${items.length}</h4>
      ${items.length ? items.map((item) =>
        `<div class="diff-file ${kind}">${escapeHtml(item.path)}</div>`
      ).join("") : empty}
    </div>
  `;
}

async function showDiff(beforeId, afterId) {
  try {
    const params = new URLSearchParams({before: beforeId, after: afterId});
    const diff = await api(
      `/v1/executions/${encodeURIComponent(state.selectedId)}/diff?${params}`
    );
    $("diff-content").innerHTML = `
      <div class="diff-grid">
        ${renderDiffGroup("Added", diff.added, "added")}
        ${renderDiffGroup("Modified", diff.modified, "modified")}
        ${renderDiffGroup("Deleted", diff.deleted, "deleted")}
      </div>
    `;
    $("diff-panel").classList.remove("hidden");
  } catch (error) {
    toast(`Diff failed: ${error.message}`);
  }
}

async function restoreSnapshot(snapshotId) {
  if (!confirm("Restore this snapshot into a new server-managed directory?")) return;
  try {
    const restored = await api(
      `/v1/executions/${encodeURIComponent(state.selectedId)}/snapshots/${encodeURIComponent(snapshotId)}/restore`,
      {method: "POST"}
    );
    toast(`Restored ${restored.file_count} files to ${restored.directory}`, 6000);
    await refreshEffects();
  } catch (error) {
    toast(`Restore failed: ${error.message}`);
  }
}

async function refreshEffects() {
  if (!state.selectedId) return;
  try {
    state.effects = await api(
      `/v1/executions/${encodeURIComponent(state.selectedId)}/effects?limit=5000`
    );
    renderEffects();
    renderOutput();
  } catch (error) {
    toast(`Failed to load effects: ${error.message}`);
  }
}

async function refreshSnapshots() {
  if (!state.selectedId) return;
  try {
    state.snapshots = await api(
      `/v1/executions/${encodeURIComponent(state.selectedId)}/snapshots`
    );
    renderSnapshots();
  } catch (error) {
    toast(`Failed to load snapshots: ${error.message}`);
  }
}

async function refreshSelected() {
  if (!state.selectedId) return;
  try {
    const id = encodeURIComponent(state.selectedId);
    const [execution, attempts] = await Promise.all([
      api(`/v1/executions/${id}`),
      api(`/v1/executions/${id}/attempts`),
    ]);
    state.execution = execution;
    renderExecution();
    renderAttempts(attempts);
  } catch (error) {
    toast(`Failed to refresh execution: ${error.message}`);
  }
}

function closeStream() {
  if (state.source) {
    state.source.close();
    state.source = null;
  }
  $("stream-state").textContent = "idle";
}

function mergeEffect(effect) {
  const existing = state.effects.findIndex((item) => item.seq === effect.seq);
  if (existing >= 0) state.effects[existing] = effect;
  else state.effects.push(effect);
  state.effects.sort((a, b) => a.seq - b.seq);
}

function connectStream() {
  closeStream();
  if (!state.selectedId) return;
  const after = state.effects.length ? state.effects[state.effects.length - 1].seq : 0;
  const source = new EventSource(
    `/v1/executions/${encodeURIComponent(state.selectedId)}/events?after=${after}`
  );
  state.source = source;
  $("stream-state").textContent = "connecting";

  source.onopen = () => {
    $("stream-state").textContent = "live";
  };

  source.onmessage = () => {};

  const knownKinds = [
    "execution_submitted", "workspace_prepared", "execution_claimed", "worker_assigned",
    "process_starting", "output_chunk", "output_captured", "snapshot_created",
    "snapshot_restored", "cancel_requested", "execution_finished", "lease_expired",
    "retry_scheduled",
    "shutdown_interruption_requested", "shutdown_before_launch", "output_drain_incomplete",
  ];

  const handle = (event) => {
    try {
      const effect = JSON.parse(event.data);
      mergeEffect(effect);
      renderEffects();
      renderOutput();
      if (effect.kind === "execution_finished" || effect.kind === "snapshot_created") {
        refreshSelected();
        refreshSnapshots();
        refreshExecutions();
      }
    } catch (error) {
      console.error("Invalid ExecLedger event", error);
    }
  };
  knownKinds.forEach((kind) => source.addEventListener(kind, handle));

  source.onerror = () => {
    $("stream-state").textContent = "reconnecting";
    if (state.execution && terminalStates.has(state.execution.status)) {
      closeStream();
      $("stream-state").textContent = "complete";
    }
  };
}

async function selectExecution(id) {
  state.selectedId = id;
  state.execution = null;
  state.effects = [];
  state.snapshots = [];
  $("diff-panel").classList.add("hidden");
  await Promise.all([
    refreshSelected(),
    refreshEffects(),
    refreshSnapshots(),
  ]);
  renderOutput();
  connectStream();
  refreshExecutions();
}

async function submitJob() {
  const key = $("idempotency-key").value.trim();
  if (!key) {
    toast("Idempotency key is required.");
    return;
  }
  let spec;
  try {
    spec = JSON.parse($("spec-json").value);
  } catch (error) {
    toast(`Invalid JSON: ${error.message}`);
    return;
  }
  $("submit-status").textContent = "submitting…";
  try {
    const result = await api("/v1/executions", {
      method: "POST",
      headers: {"Idempotency-Key": key},
      body: JSON.stringify(spec),
    });
    $("submit-status").textContent = result.created ? "created" : "reused";
    await refreshExecutions();
    await selectExecution(result.execution.id);
  } catch (error) {
    $("submit-status").textContent = "failed";
    toast(`Submit failed: ${error.message}`);
  }
}

function gcRestoreAge() {
  const raw = $("gc-restore-age").value.trim();
  if (!raw) return null;
  const value = Number(raw);
  if (!Number.isFinite(value) || value < 0) {
    throw new Error("Restore retention age must be a non-negative number.");
  }
  return value;
}

function renderGcReport(report) {
  const lines = [
    report.dry_run ? "DRY RUN" : "APPLIED",
    `snapshots scanned: ${report.snapshots_scanned}`,
    `blobs scanned: ${report.blobs_scanned}`,
    `referenced blobs: ${report.referenced_blobs}`,
    `orphan blobs: ${report.orphan_blobs.length}`,
    `blob bytes reclaimable: ${report.bytes_reclaimable}`,
    `blob bytes reclaimed: ${report.bytes_reclaimed}`,
    `restores scanned: ${report.restores_scanned}`,
    `restore dirs eligible: ${report.restore_dirs_eligible.length}`,
    `restore dirs deleted: ${report.restore_dirs_deleted.length}`,
    `restore bytes reclaimable: ${report.restore_bytes_reclaimable}`,
    `restore bytes reclaimed: ${report.restore_bytes_reclaimed}`,
  ];
  $("gc-report").textContent = lines.join("\n");
}

async function runStorageGc(apply) {
  let restoreAge;
  try {
    restoreAge = gcRestoreAge();
  } catch (error) {
    toast(error.message);
    return;
  }

  if (apply) {
    const confirmed = confirm(
      "Apply storage GC? Unreferenced blobs and eligible restored copies will be deleted."
    );
    if (!confirmed) return;
  }

  const params = new URLSearchParams({apply: String(apply)});
  if (restoreAge !== null) {
    params.set("restore_older_than_seconds", String(restoreAge));
  }

  $("gc-status").textContent = apply ? "applying…" : "scanning…";
  try {
    const report = await api(
      `/v1/maintenance/gc?${params}`,
      {method: "POST"}
    );
    renderGcReport(report);
    $("gc-status").textContent = report.dry_run ? "dry-run complete" : "GC complete";
    if (!report.dry_run) {
      toast(
        `GC reclaimed ${report.bytes_reclaimed + report.restore_bytes_reclaimed} bytes.`
      );
    }
  } catch (error) {
    $("gc-status").textContent = "failed";
    toast(`Storage GC failed: ${error.message}`);
  }
}

async function cancelSelected() {
  if (!state.selectedId) return;
  if (!confirm("Request cancellation for this execution?")) return;
  try {
    state.execution = await api(
      `/v1/executions/${encodeURIComponent(state.selectedId)}/cancel`,
      {method: "POST"}
    );
    renderExecution();
    await refreshEffects();
    await refreshExecutions();
  } catch (error) {
    toast(`Cancel failed: ${error.message}`);
  }
}

function loadExample() {
  $("idempotency-key").value = `console-${Date.now()}`;
  $("spec-json").value = JSON.stringify({
    argv: [
      "python",
      "-c",
      "from pathlib import Path; print(Path('input.txt').read_text()); Path('result.txt').write_text('ok')"
    ],
    files: {"input.txt": "hello from ExecLedger console"},
    timeout_seconds: 10,
    max_output_bytes: 65536,
  }, null, 2);
}

function bindEvents() {
  $("load-example").addEventListener("click", loadExample);
  $("submit-job").addEventListener("click", submitJob);
  $("gc-scan").addEventListener("click", () => runStorageGc(false));
  $("gc-apply").addEventListener("click", () => runStorageGc(true));
  $("refresh-list").addEventListener("click", refreshExecutions);
  $("cancel-job").addEventListener("click", cancelSelected);
  $("refresh-effects").addEventListener("click", refreshEffects);
  $("refresh-snapshots").addEventListener("click", refreshSnapshots);
  $("reconnect-stream").addEventListener("click", connectStream);
  $("close-diff").addEventListener("click", () => $("diff-panel").classList.add("hidden"));
  document.querySelectorAll(".tab").forEach((node) => {
    node.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((tab) => tab.classList.remove("active"));
      node.classList.add("active");
      state.activeTab = node.dataset.tab;
      renderOutput();
    });
  });
  window.addEventListener("beforeunload", closeStream);
}

async function boot() {
  bindEvents();
  loadExample();
  await Promise.all([refreshHealth(), refreshExecutions()]);
  setInterval(refreshHealth, 10000);
  setInterval(async () => {
    await refreshExecutions();
    if (state.selectedId) await refreshSelected();
  }, 2000);
}

boot();
