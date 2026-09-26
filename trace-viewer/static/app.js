'use strict';

const PAGE_SIZE = 50;
const MAX_TREE_DEPTH = 50; // safety guard against unexpected deep nesting

// ---------- List-view state ----------
let pageIndex = 0;
let hasErrorFilter = null; // null | true | false
let currentTotal = 0;

// ---------- Detail-view state ----------
let currentTraceId = null;

// ---------- DOM refs — list view ----------
const tracesBody    = document.getElementById('traces-body');
const statusRegion  = document.getElementById('status-region');
const errorBanner   = document.getElementById('error-banner');
const errorMessage  = document.getElementById('error-message');
const retryBtn      = document.getElementById('retry-btn');
const prevBtn       = document.getElementById('prev-btn');
const nextBtn       = document.getElementById('next-btn');
const pageIndicator = document.getElementById('page-indicator');
const countDisplay  = document.getElementById('count-display');

// ---------- DOM refs — detail view ----------
const listViewEl        = document.getElementById('list-view');
const detailViewEl      = document.getElementById('detail-view');
const backBtn           = document.getElementById('back-btn');
const detailTraceIdEl   = document.getElementById('detail-trace-id-display');
const detailCopyBtn     = document.getElementById('detail-copy-btn');
const detailSpanCount   = document.getElementById('detail-span-count');
const detailHealthEl    = document.getElementById('detail-health');
const cycleWarningEl    = document.getElementById('cycle-warning');
const detailStatusEl    = document.getElementById('detail-status');
const detailErrBanner   = document.getElementById('detail-error-banner');
const detailErrMsg      = document.getElementById('detail-error-message');
const detailRetryBtn    = document.getElementById('detail-retry-btn');
const rootsContainer    = document.getElementById('roots-container');
const orphansSection    = document.getElementById('orphans-section');
const orphansContainer  = document.getElementById('orphans-container');
const cycleMembersSection   = document.getElementById('cycle-members-section');
const cycleMembersContainer = document.getElementById('cycle-members-container');
const spanPanelEmpty    = document.getElementById('span-panel-empty');
const spanPanelContent  = document.getElementById('span-panel-content');
const spanFieldsDl      = document.getElementById('span-fields');

// ==========================================================================
// Shared formatting
// ==========================================================================

function formatTimestamp(iso) {
  const d = new Date(iso);
  const pad = n => String(n).padStart(2, '0');
  return (
    d.getUTCFullYear()          + '-' +
    pad(d.getUTCMonth() + 1)    + '-' +
    pad(d.getUTCDate())         + ' ' +
    pad(d.getUTCHours())        + ':' +
    pad(d.getUTCMinutes())      + ':' +
    pad(d.getUTCSeconds())      + ' UTC'
  );
}

function formatDuration(ms) {
  if (ms < 1)      return '< 1 ms';
  if (ms < 1000)   return Math.round(ms) + ' ms';
  if (ms < 60000)  return (ms / 1000).toFixed(2) + ' s';
  const m = Math.floor(ms / 60000);
  const s = Math.floor((ms % 60000) / 1000);
  return m + ' m ' + s + ' s';
}

// ==========================================================================
// Span classification helpers
// ==========================================================================

function isErrorSpan(span) {
  return span.status_code === 'STATUS_CODE_ERROR' || span.error_type !== null;
}

function getSpanType(span) {
  if (span.agent_name !== null || span.agent_operation !== null) return 'agent';
  if (span.tool_name !== null || span.tool_status !== null)      return 'tool';
  if (span.retrieval_result_count !== null || span.retrieval_top_relevance_score !== null) return 'retrieval';
  if (span.gen_ai_operation !== null) return 'llm';
  return 'generic';
}

function spanTypeLetter(type) {
  return { agent: 'A', tool: 'T', retrieval: 'R', llm: 'L' }[type] || '';
}

// Walk a list of TraceNodeResponse objects (and their children) looking for any error span.
// Children arrays from Phase 9.2 reconstruction are already acyclic, so this is safe.
function anySpanHasError(nodes) {
  for (const node of nodes) {
    if (isErrorSpan(node.span)) return true;
    if (anySpanHasError(node.children)) return true;
  }
  return false;
}

// ==========================================================================
// List view — API & rendering
// ==========================================================================

function buildApiUrl() {
  const params = new URLSearchParams();
  params.set('limit', String(PAGE_SIZE));
  params.set('offset', String(pageIndex * PAGE_SIZE));
  if (hasErrorFilter !== null) {
    params.set('has_error', String(hasErrorFilter));
  }
  return '/api/traces?' + params.toString();
}

async function fetchTraces() {
  setLoading(true);
  clearError();

  try {
    const resp = await fetch(buildApiUrl());
    if (!resp.ok) {
      let detail = resp.statusText || String(resp.status);
      try {
        const body = await resp.json();
        if (body && body.detail) detail = body.detail;
      } catch (_) { /* keep statusText */ }
      throw new Error('HTTP ' + resp.status + ' — ' + detail);
    }
    const data = await resp.json();
    currentTotal = data.total;
    renderTable(data.traces);
    renderPagination();
    renderCount(data.traces.length);
  } catch (err) {
    showError(err.message);
  } finally {
    setLoading(false);
  }
}

function setLoading(loading) {
  if (loading) {
    statusRegion.textContent = 'Loading traces…';
    prevBtn.setAttribute('aria-disabled', 'true');
    nextBtn.setAttribute('aria-disabled', 'true');
  } else {
    statusRegion.textContent = '';
  }
}

function showError(msg) {
  errorMessage.textContent =
    'Could not load traces — ' + msg + '. Check that the API is running.';
  errorBanner.classList.remove('hidden');
  tracesBody.replaceChildren();
  countDisplay.textContent = '';
  pageIndicator.textContent = '';
}

function clearError() {
  errorBanner.classList.add('hidden');
  errorMessage.textContent = '';
}

function renderTable(traces) {
  if (traces.length === 0) {
    renderEmpty();
    return;
  }
  const fragment = document.createDocumentFragment();
  for (const t of traces) {
    fragment.appendChild(makeRow(t));
  }
  tracesBody.replaceChildren(fragment);
}

function renderEmpty() {
  const tr = document.createElement('tr');
  tr.className = 'empty-row';
  const td = document.createElement('td');
  td.setAttribute('colspan', '8');
  td.textContent = 'No traces found.';
  if (hasErrorFilter !== null) {
    const link = document.createElement('a');
    link.href = '#';
    link.className = 'clear-filter-link';
    link.textContent = 'Show all traces';
    link.addEventListener('click', e => {
      e.preventDefault();
      applyFilter(null);
    });
    td.appendChild(link);
  }
  tr.appendChild(td);
  tracesBody.replaceChildren(tr);
}

function makeRow(t) {
  const tr = document.createElement('tr');

  // Navigate to detail view when the row is clicked.
  tr.addEventListener('click', () => openTraceDetail(t.trace_id));

  // Status
  const statusTd = document.createElement('td');
  const dot       = document.createElement('span');
  dot.className   = 'status-dot ' + (t.has_error ? 'error' : 'ok');
  dot.setAttribute('aria-label', t.has_error ? 'Error' : 'OK');
  dot.setAttribute('role', 'img');
  statusTd.appendChild(dot);
  tr.appendChild(statusTd);

  // Started
  tr.appendChild(makeTextCell(formatTimestamp(t.trace_start_time)));

  // Duration
  tr.appendChild(makeTextCell(formatDuration(t.trace_duration_ms)));

  // Root operation
  const opTd    = document.createElement('td');
  const opSpan  = document.createElement('span');
  opSpan.className   = 'op-name';
  opSpan.textContent = t.root_span_name || '—';
  if (t.root_span_name) opSpan.title = t.root_span_name;
  opTd.appendChild(opSpan);
  tr.appendChild(opTd);

  // Service
  tr.appendChild(makeTextCell(t.root_service_name || '—'));

  // Spans
  const spansTd = makeTextCell(String(t.span_count));
  spansTd.className = 'align-right';
  tr.appendChild(spansTd);

  // Breakdown
  const bkTd = document.createElement('td');
  bkTd.appendChild(makeBreakdown(t));
  tr.appendChild(bkTd);

  // Trace ID + copy (copy button stops propagation so it does not trigger row nav)
  const idTd   = document.createElement('td');
  const idCell = document.createElement('div');
  idCell.className = 'trace-id-cell';
  const idDisplay = document.createElement('span');
  idDisplay.className   = 'trace-id-display';
  idDisplay.textContent = t.trace_id.slice(0, 8);
  idDisplay.title       = t.trace_id;
  idCell.appendChild(idDisplay);
  idCell.appendChild(makeCopyButton(t.trace_id));
  idTd.appendChild(idCell);
  tr.appendChild(idTd);

  return tr;
}

function makeTextCell(text) {
  const td = document.createElement('td');
  td.textContent = text;
  return td;
}

function makeBreakdown(t) {
  const container = document.createElement('div');
  container.className = 'breakdown';

  const defs = [
    { label: 'A', title: 'Agent spans',     count: t.agent_span_count,     isError: false },
    { label: 'T', title: 'Tool spans',      count: t.tool_span_count,      isError: false },
    { label: 'R', title: 'Retrieval spans', count: t.retrieval_span_count, isError: false },
    { label: 'E', title: 'Error spans',     count: t.error_span_count,     isError: true  },
  ];

  const anyNonZero = defs.some(d => d.count > 0);
  if (!anyNonZero) return container;

  for (const d of defs) {
    const span       = document.createElement('span');
    span.className   = 'badge' + (d.isError && d.count > 0 ? ' error-badge' : '');
    span.title       = d.title;
    span.textContent = d.label + ' ' + d.count;
    container.appendChild(span);
  }

  return container;
}

function makeCopyButton(fullId) {
  const btn = document.createElement('button');
  btn.className = 'copy-btn';
  btn.textContent = 'copy';
  btn.type = 'button';
  btn.setAttribute('aria-label', 'Copy full trace ID');

  btn.addEventListener('click', e => {
    e.stopPropagation(); // do not bubble to the row click handler
    if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') {
      showCopyFeedback(btn, false);
      return;
    }
    navigator.clipboard.writeText(fullId).then(
      ()  => showCopyFeedback(btn, true),
      ()  => showCopyFeedback(btn, false)
    );
  });

  return btn;
}

function showCopyFeedback(btn, success) {
  const prev = btn.textContent;
  btn.textContent = success ? '✓' : '✗';
  btn.classList.add(success ? 'copied' : 'failed');
  setTimeout(() => {
    btn.textContent = prev;
    btn.classList.remove('copied', 'failed');
  }, 1500);
}

function renderPagination() {
  const totalPages = Math.max(1, Math.ceil(currentTotal / PAGE_SIZE));
  pageIndicator.textContent = 'Page ' + (pageIndex + 1) + ' of ' + totalPages;

  prevBtn.setAttribute('aria-disabled', pageIndex === 0             ? 'true' : 'false');
  nextBtn.setAttribute('aria-disabled', pageIndex >= totalPages - 1 ? 'true' : 'false');
}

function renderCount(rowCount) {
  if (currentTotal === 0) {
    countDisplay.textContent = '';
    return;
  }
  const start = pageIndex * PAGE_SIZE + 1;
  const end   = pageIndex * PAGE_SIZE + rowCount;
  countDisplay.textContent =
    'Showing ' + start + '–' + end + ' of ' + currentTotal + ' traces';
}

function applyFilter(value) {
  hasErrorFilter = value;
  pageIndex = 0;

  document.getElementById('filter-all')
    .setAttribute('aria-pressed', value === null  ? 'true' : 'false');
  document.getElementById('filter-errors')
    .setAttribute('aria-pressed', value === true  ? 'true' : 'false');
  document.getElementById('filter-healthy')
    .setAttribute('aria-pressed', value === false ? 'true' : 'false');

  fetchTraces();
}

// ==========================================================================
// View switching
// ==========================================================================

function showListView() {
  document.body.classList.remove('detail-mode');
  listViewEl.classList.remove('hidden');
  detailViewEl.classList.add('hidden');
  currentTraceId = null;
}

function showDetailView() {
  document.body.classList.add('detail-mode');
  listViewEl.classList.add('hidden');
  detailViewEl.classList.remove('hidden');
}

// ==========================================================================
// Detail view — navigation & fetch
// ==========================================================================

async function openTraceDetail(traceId) {
  currentTraceId = traceId;
  showDetailView();

  // Immediately populate partial info so the toolbar is not blank during load
  detailTraceIdEl.textContent = traceId.slice(0, 8) + '…' + traceId.slice(-8);
  detailTraceIdEl.title = traceId;
  detailCopyBtn.dataset.traceId = traceId;
  detailSpanCount.textContent = '';
  detailHealthEl.textContent = '';
  detailHealthEl.className = 'detail-meta-item';

  // Clear previous tree content
  cycleWarningEl.classList.add('hidden');
  rootsContainer.replaceChildren();
  orphansSection.classList.add('hidden');
  orphansContainer.replaceChildren();
  cycleMembersSection.classList.add('hidden');
  cycleMembersContainer.replaceChildren();
  clearSpanPanel();

  await fetchDetail();
}

async function fetchDetail() {
  if (!currentTraceId) return;
  showDetailLoading();
  clearDetailError();

  try {
    const resp = await fetch('/api/traces/' + currentTraceId);
    if (resp.status === 404) {
      detailStatusEl.textContent = 'Trace not found.';
      return;
    }
    if (!resp.ok) {
      let detail = resp.statusText || String(resp.status);
      try {
        const body = await resp.json();
        if (body && body.detail) detail = body.detail;
      } catch (_) { /* keep statusText */ }
      showDetailError('HTTP ' + resp.status + ' — ' + detail);
      return;
    }
    const data = await resp.json();
    detailStatusEl.textContent = '';
    renderDetail(data);
  } catch (err) {
    showDetailError(err.message);
  }
}

function showDetailLoading() {
  detailStatusEl.textContent = 'Loading trace…';
}

function showDetailError(msg) {
  detailStatusEl.textContent = '';
  detailErrMsg.textContent = 'Could not load trace — ' + msg;
  detailErrBanner.classList.remove('hidden');
}

function clearDetailError() {
  detailErrBanner.classList.add('hidden');
  detailErrMsg.textContent = '';
}

// ==========================================================================
// Detail view — rendering
// ==========================================================================

function renderDetail(data) {
  // Trace-level header
  detailTraceIdEl.textContent = data.trace_id.slice(0, 8) + '…' + data.trace_id.slice(-8);
  detailTraceIdEl.title = data.trace_id;
  detailCopyBtn.dataset.traceId = data.trace_id;

  detailSpanCount.textContent = data.span_count + ' span' + (data.span_count === 1 ? '' : 's');

  const hasErr = anySpanHasError(data.roots) ||
                 anySpanHasError(data.orphans) ||
                 anySpanHasError(data.cycle_members);
  detailHealthEl.textContent = hasErr ? 'Error' : 'Healthy';
  detailHealthEl.className   = 'detail-meta-item ' + (hasErr ? 'health-error' : 'health-ok');

  cycleWarningEl.classList.toggle('hidden', !data.has_cycle);

  // Execution tree (roots — always shown even when empty)
  renderTreeSection(data.roots, rootsContainer, 'No root spans found.');

  // Orphans section (only when present)
  orphansSection.classList.toggle('hidden', data.orphans.length === 0);
  if (data.orphans.length > 0) {
    renderTreeSection(data.orphans, orphansContainer, 'No orphan spans.');
  }

  // Cycle members section (only when present)
  cycleMembersSection.classList.toggle('hidden', data.cycle_members.length === 0);
  if (data.cycle_members.length > 0) {
    renderTreeSection(data.cycle_members, cycleMembersContainer, 'No cycle members.');
  }

  clearSpanPanel();
}

function renderTreeSection(nodes, container, emptyMsg) {
  container.replaceChildren();
  if (nodes.length === 0) {
    const p = document.createElement('p');
    p.className = 'tree-empty';
    p.textContent = emptyMsg;
    container.appendChild(p);
    return;
  }
  const frag = document.createDocumentFragment();
  for (const node of nodes) {
    const el = renderTreeNode(node, 0);
    if (el) frag.appendChild(el);
  }
  container.appendChild(frag);
}

function renderTreeNode(node, depth) {
  // Safety guard: Phase 9.2 reconstruction removes back-edges, so cycles
  // cannot appear in children[]. The depth cap defends against unexpected data.
  if (depth > MAX_TREE_DEPTH) return null;

  const span   = node.span;
  const isErr  = isErrorSpan(span);
  const isCycle = node.is_cycle_member;
  const type   = getSpanType(span);

  const item = document.createElement('div');
  item.setAttribute('role', 'treeitem');
  if (node.children.length > 0) item.setAttribute('aria-expanded', 'true');

  const row = document.createElement('div');
  row.className = 'tree-node-row' + (isErr ? ' error-row' : '');
  row.setAttribute('tabindex', '0');
  row.setAttribute('aria-selected', 'false');

  // Span-type badge (A / T / R / L), omitted for generic spans
  if (type !== 'generic') {
    const badge = document.createElement('span');
    badge.className = 'span-type-badge span-type-' + type;
    badge.textContent = spanTypeLetter(type);
    badge.title = type;
    badge.setAttribute('aria-hidden', 'true');
    row.appendChild(badge);
  }

  // Span name
  const nameEl = document.createElement('span');
  nameEl.className = 'span-name';
  nameEl.textContent = span.span_name;
  nameEl.title = span.span_name;
  row.appendChild(nameEl);

  // Duration
  const durEl = document.createElement('span');
  durEl.className = 'span-duration';
  durEl.textContent = formatDuration(span.duration_ms);
  row.appendChild(durEl);

  // Error pill
  if (isErr) {
    const pill = document.createElement('span');
    pill.className = 'span-error-pill';
    pill.textContent = 'error';
    pill.setAttribute('aria-label', 'error span');
    row.appendChild(pill);
  }

  // Cycle member pill
  if (isCycle) {
    const pill = document.createElement('span');
    pill.className = 'span-cycle-pill';
    pill.textContent = 'cycle';
    pill.setAttribute('aria-label', 'cycle member');
    row.appendChild(pill);
  }

  // Select span on click
  row.addEventListener('click', () => {
    document.querySelectorAll('.tree-node-row.selected').forEach(r => {
      r.classList.remove('selected');
      r.setAttribute('aria-selected', 'false');
    });
    row.classList.add('selected');
    row.setAttribute('aria-selected', 'true');
    selectSpan(span);
  });

  // Keyboard: Enter/Space activates
  row.addEventListener('keydown', e => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      row.click();
    }
  });

  item.appendChild(row);

  // Recurse into children
  if (node.children.length > 0) {
    const group = document.createElement('div');
    group.className = 'tree-children';
    group.setAttribute('role', 'group');
    for (const child of node.children) {
      const childEl = renderTreeNode(child, depth + 1);
      if (childEl) group.appendChild(childEl);
    }
    item.appendChild(group);
  }

  return item;
}

// ==========================================================================
// Span detail panel
// ==========================================================================

function clearSpanPanel() {
  spanPanelEmpty.classList.remove('hidden');
  spanPanelContent.classList.add('hidden');
  spanFieldsDl.replaceChildren();
}

function selectSpan(span) {
  renderSpanPanel(span);
}

function renderSpanPanel(span) {
  spanFieldsDl.replaceChildren();

  function addDt(label, className) {
    const dt = document.createElement('dt');
    dt.textContent = label;
    if (className) dt.className = className;
    spanFieldsDl.appendChild(dt);
  }

  function addDd(value, opts = {}) {
    const dd = document.createElement('dd');
    const classes = [];
    if (opts.mono) classes.push('mono');
    if (opts.error) classes.push('error-value');
    if (classes.length) dd.className = classes.join(' ');
    dd.textContent = (value === null || value === undefined || value === '') ? '—' : String(value);
    spanFieldsDl.appendChild(dd);
  }

  function addSep() {
    const hr = document.createElement('hr');
    hr.className = 'field-sep';
    hr.setAttribute('aria-hidden', 'true');
    spanFieldsDl.appendChild(hr);
  }

  function addField(label, value, opts = {}) {
    addDt(label);
    addDd(value, opts);
  }

  function addOptional(label, value, opts = {}) {
    if (value === null || value === undefined || value === '') return;
    addField(label, value, opts);
  }

  // --- Core identity (always shown) ---
  addField('Span ID',       span.span_id,        { mono: true });
  addField('Parent Span',   span.parent_span_id, { mono: true });

  // --- Timing ---
  addSep();
  addField('Start',    formatTimestamp(span.start_time));
  addField('End',      formatTimestamp(span.end_time));
  addField('Duration', formatDuration(span.duration_ms));

  // --- Classification ---
  addSep();
  addField('Name',        span.span_name);
  addOptional('Trace ID', span.trace_id,        { mono: true });
  addOptional('Service',  span.service_name);
  addOptional('Kind',     span.span_kind);
  addOptional('GenAI Op', span.gen_ai_operation);

  // --- Status ---
  addSep();
  addField('Status', span.status_code);
  addOptional('Status Msg', span.status_message);

  // --- Request / session context ---
  const hasCtx = span.request_id || span.session_id;
  if (hasCtx) {
    addSep();
    addOptional('Request ID', span.request_id, { mono: true });
    addOptional('Session ID', span.session_id, { mono: true });
  }

  // --- Agent-specific ---
  const hasAgent = span.agent_name || span.agent_operation;
  if (hasAgent) {
    addSep();
    addOptional('Agent Name', span.agent_name);
    addOptional('Agent Op',   span.agent_operation);
  }

  // --- Tool-specific ---
  const hasTool = span.tool_name || span.tool_status;
  if (hasTool) {
    addSep();
    addOptional('Tool Name',   span.tool_name);
    addOptional('Tool Status', span.tool_status);
  }

  // --- Retrieval-specific ---
  const hasRetrieval = span.retrieval_result_count !== null ||
                       span.retrieval_top_relevance_score !== null;
  if (hasRetrieval) {
    addSep();
    if (span.retrieval_result_count !== null) {
      addField('Results', String(span.retrieval_result_count));
    }
    if (span.retrieval_top_relevance_score !== null) {
      addField('Top Score', span.retrieval_top_relevance_score.toFixed(4));
    }
  }

  // --- Error details ---
  const hasError = span.error_type || span.error_message;
  if (hasError) {
    addSep();
    addOptional('Error Type', span.error_type,    { error: true });
    addOptional('Error Msg',  span.error_message, { error: true });
  }

  spanPanelEmpty.classList.add('hidden');
  spanPanelContent.classList.remove('hidden');
}

// ==========================================================================
// Event listeners — list view
// ==========================================================================

document.getElementById('filter-all')
  .addEventListener('click', () => applyFilter(null));
document.getElementById('filter-errors')
  .addEventListener('click', () => applyFilter(true));
document.getElementById('filter-healthy')
  .addEventListener('click', () => applyFilter(false));

retryBtn.addEventListener('click', fetchTraces);

prevBtn.addEventListener('click', () => {
  if (prevBtn.getAttribute('aria-disabled') === 'true') return;
  pageIndex -= 1;
  fetchTraces();
});

nextBtn.addEventListener('click', () => {
  if (nextBtn.getAttribute('aria-disabled') === 'true') return;
  pageIndex += 1;
  fetchTraces();
});

// ==========================================================================
// Event listeners — detail view
// ==========================================================================

backBtn.addEventListener('click', showListView);

detailCopyBtn.addEventListener('click', () => {
  const traceId = detailCopyBtn.dataset.traceId;
  if (!traceId) return;
  if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') {
    showCopyFeedback(detailCopyBtn, false);
    return;
  }
  navigator.clipboard.writeText(traceId).then(
    () => showCopyFeedback(detailCopyBtn, true),
    () => showCopyFeedback(detailCopyBtn, false)
  );
});

detailRetryBtn.addEventListener('click', fetchDetail);

// ==========================================================================
// Init
// ==========================================================================

fetchTraces();
