'use strict';

const PAGE_SIZE = 50;

// --- State ---
let pageIndex = 0;
let hasErrorFilter = null; // null | true | false
let currentTotal = 0;

// --- DOM refs ---
const tracesBody    = document.getElementById('traces-body');
const statusRegion  = document.getElementById('status-region');
const errorBanner   = document.getElementById('error-banner');
const errorMessage  = document.getElementById('error-message');
const retryBtn      = document.getElementById('retry-btn');
const prevBtn       = document.getElementById('prev-btn');
const nextBtn       = document.getElementById('next-btn');
const pageIndicator = document.getElementById('page-indicator');
const countDisplay  = document.getElementById('count-display');

// --- Formatting ---

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

// --- API ---

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

// --- State rendering ---

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

// --- Table rendering ---

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
  const tr  = document.createElement('tr');
  tr.className = 'empty-row';
  const td  = document.createElement('td');
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

  // Status
  const statusTd  = document.createElement('td');
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
  const opTd   = document.createElement('td');
  const opSpan = document.createElement('span');
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

  // Trace ID
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
    span.textContent = d.label + ' ' + d.count;
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

  btn.addEventListener('click', () => {
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

// --- Pagination rendering ---

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

// --- Filter ---

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

// --- Event listeners ---

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

// --- Init ---
fetchTraces();
