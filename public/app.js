'use strict';

/*  Config  */
const API_BASE = (window.API_URL || '').replace(/\/$/, '');

/*  State  */
const state = {
  lastData:          null,
  showingUnmatched:  false,
  examplesOpen:      true,
};

/*  DOM helpers  */
const $  = sel => document.querySelector(sel);
const show = (sel, display) => {
  const el = typeof sel === 'string' ? $(sel) : sel;
  el && el.classList.remove('hidden', 'd-none');
  if (display) el.style.display = display;
};
const hide = sel => {
  const el = typeof sel === 'string' ? $(sel) : sel;
  el && el.classList.add('hidden');
};

function esc(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function truncate(s, n) {
  if (!s || s.length <= n) return s;
  return s.slice(0, n).replace(/\s\S*$/, '') + '…';
}
function cap(s) { return String(s).charAt(0).toUpperCase() + String(s).slice(1); }
function fmtNum(n) {
  const v = Number(n); if (isNaN(v)) return String(n);
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'k';
  return String(Math.round(v));
}
function fmtMoney(n) {
  const v = Number(n); if (isNaN(v)) return String(n);
  if (v >= 1e9) return (v / 1e9).toFixed(1) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(0) + 'k';
  return v.toFixed(0);
}
function formatAddress(a) {
  if (!a) return null;
  if (typeof a === 'object')
    return [a.town, a.region_name, (a.country_code || '').toUpperCase()].filter(Boolean).join(', ');
  return String(a);
}

/*  Health check  */
async function checkHealth() {
  const dot  = $('#statusDot');
  const text = $('#statusText');
  const sub  = $('#statusSub');
  try {
    const r = await fetch(`${API_BASE}/health`, { signal: AbortSignal.timeout(6000) });
    const d = await r.json();
    dot.className = 'pulse-dot ok';
    text.textContent = 'API connected';
    sub.textContent  = `${d.environment} · ${d.sql_enabled ? 'SQL' : 'in-memory'}`;
  } catch {
    dot.className = 'pulse-dot err';
    text.textContent = 'Unreachable';
    sub.textContent  = window.location.hostname;
  }
}

/*  Search  */
async function runSearch() {
  const query = $('#query').value.trim();
  if (!query) { $('#query').focus(); return; }

  const topK        = parseInt($('#topK').value);
  const onlyMatched = $('#onlyMatched').checked;

  setLoading(true);
  showSkeleton();

  try {
    const res = await fetch(`${API_BASE}/api/search`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ query, top_k: topK, only_matched: onlyMatched }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({ detail: `HTTP ${res.status}` }));
      throw new Error(err.detail || `HTTP ${res.status}`);
    }

    const data               = await res.json();
    state.lastData           = data;
    state.showingUnmatched   = !onlyMatched;
    renderResults(data, onlyMatched);
  } catch (err) {
    renderError(err.message);
  } finally {
    setLoading(false);
  }
}

function setLoading(on) {
  const btn = $('#runBtn');
  btn.disabled = on;
  $('#runSpinner').classList.toggle('d-none', !on);
  const ico = $('#runIco');
  if (ico) ico.style.display = on ? 'none' : '';
}

/*  Toggle unmatched  */
async function toggleUnmatched() {
  if (!state.lastData) return;

  if (state.showingUnmatched) {
    state.showingUnmatched = false;
    renderResults(state.lastData, true);
    return;
  }

  const query = $('#query').value.trim();
  const topK  = parseInt($('#topK').value);
  setLoading(true); showSkeleton();

  try {
    const res  = await fetch(`${API_BASE}/api/search`, {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ query, top_k: topK, only_matched: false }),
    });
    const data = await res.json();
    state.lastData = data; state.showingUnmatched = true;
    renderResults(data, false);
  } catch (err) { renderError(err.message); }
  finally       { setLoading(false); }
}

/*  Render results  */
function renderResults(data, onlyMatched) {
  $('#statMatched').textContent   = data.matched_count   ?? '—';
  $('#statEvaluated').textContent = data.evaluated_count ?? '—';

  hide('#emptyState');
  hide('#errorBanner');
  show('#resultsHeader');
  $('#resultsList').innerHTML = '';

  $('#resultsQuery').textContent  = `"${data.query}"`;
  $('#resultsCounts').innerHTML   =
    `<span class="hl">${data.matched_count}</span> matched &nbsp;·&nbsp; ${data.evaluated_count} evaluated`;

  renderIntentChips(data.query);

  // Unmatched toggle button
  const unmatched = (data.results || []).filter(r => !r.matched);
  const btn = $('#unmatchedBtn');
  if (unmatched.length > 0) {
    show(btn);
    $('#unmatchedIco').setAttribute('data-showing', state.showingUnmatched);
    $('#unmatchedLabel').textContent = state.showingUnmatched
      ? `Hide ${unmatched.length} unmatched`
      : `Show ${unmatched.length} unmatched`;
  } else {
    hide(btn);
  }

  if (!data.results || data.results.length === 0) {
    $('#resultsList').innerHTML = `
      <div class="empty-state" style="padding-top:0">
        <div class="es-icon"><svg width="28" height="28" viewBox="0 0 24 24" 
        fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" 
        stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 01-2 2H5a2 2 0 01-2-2z"/></svg></div>
        <h3 class="es-title" style="font-size:16px">No companies found</h3>
        <p class="es-body" style="font-size:13px">Try broadening your query or removing constraints.</p>
      </div>`;
    return;
  }

  const matched    = data.results.filter(r =>  r.matched);
  const notMatched = data.results.filter(r => !r.matched);
  const list       = $('#resultsList');

  matched.forEach((r, i)    => list.appendChild(buildCard(r, i + 1)));

  if (!onlyMatched && notMatched.length > 0) {
    const div = document.createElement('div');
    div.className = 'um-divider';
    div.innerHTML = `<div class="um-line"></div>
      <div class="um-lbl">${notMatched.length} not matched</div>
      <div class="um-line"></div>`;
    list.appendChild(div);
    notMatched.forEach((r, i) => list.appendChild(buildCard(r, matched.length + i + 1)));
  }
}

/*  Build card  */
function buildCard(r, rank) {
  const card = document.createElement('div');
  card.className = `result-card ${r.matched ? 'matched' : 'unmatched'}`;
  card.style.animationDelay = `${Math.min(rank - 1, 18) * 28}ms`;

  const url    = r.website ? (r.website.startsWith('http') ? r.website : `https://${r.website}`) : null;
  const addr   = formatAddress(r.address);
  const empStr = r.employee_count != null ? fmtNum(r.employee_count) : null;
  const revStr = r.revenue        != null ? '$' + fmtMoney(r.revenue) : null;
  const desc   = r.description ? truncate(r.description, 240) : null;

  const embedPct = Math.round((r.embedding_score || 0) * 100);
  const lexPct   = Math.round((r.lexical_score   || 0) * 100);
  const llmPct   = Math.round(((r.llm_score      || 0) / 10) * 100);

  const checkSvg = `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>`;
  const xSvg     = `<svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>`;
  const globeSvg = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="2" y1="12" x2="22" y2="12"/><path d="M12 2a15.3 15.3 0 014 10 15.3 15.3 0 01-4 10 15.3 15.3 0 01-4-10 15.3 15.3 0 014-10z"/></svg>`;
  const pinSvg   = `<svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0118 0z"/><circle cx="12" cy="10" r="3"/></svg>`;

  card.innerHTML = `
    <div class="card-rank">#${rank}</div>
    <div class="card-name-row">
      <div class="card-name">${esc(r.operational_name || 'Unknown Company')}</div>
      <span class="match-badge ${r.matched ? 'yes' : 'no'}">
        ${r.matched ? checkSvg : xSvg}
        ${r.matched ? 'Matched' : 'Not matched'}
      </span>
    </div>
    <div class="card-meta">
      ${url  ? `<div class="meta-it">${globeSvg}<a href="${url}" target="_blank" rel="noopener">${esc(r.website)}</a></div>` : ''}
      ${addr ? `<div class="meta-it">${pinSvg}<span>${esc(addr)}</span></div>` : ''}
    </div>
    ${desc ? `<div class="card-desc">${esc(desc)}</div>` : ''}
    ${(empStr || revStr) ? `
      <div class="card-pills">
        ${empStr ? `<div class="d-pill"><span class="pl">employees</span>${empStr}</div>` : ''}
        ${revStr ? `<div class="d-pill"><span class="pl">revenue</span>${revStr}</div>` : ''}
      </div>` : ''}
    ${r.reason ? `<div class="card-reason">${esc(r.reason)}</div>` : ''}
    <div class="card-scores">
      ${scoreBar('Embedding',  (r.embedding_score || 0).toFixed(4), embedPct, 'fill-teal')}
      ${scoreBar('Lexical',    (r.lexical_score   || 0).toFixed(4), lexPct,   'fill-amber')}
      ${scoreBar('LLM score',  `${r.llm_score || 0}/10`,           llmPct,   'fill-purple')}
    </div>`;

  return card;
}

function scoreBar(label, value, pct, cls) {
  return `<div class="score-it">
    <div class="score-lbl">${label}</div>
    <div class="score-track"><div class="score-fill ${cls}" style="width:${pct}%"></div></div>
    <div class="score-val">${value}</div>
  </div>`;
}

/*  Intent chips  */
function renderIntentChips(query) {
  const strip = $('#intentStrip');
  const q     = query.toLowerCase();
  const chips = [];

  ['romania','france','germany','scandinavia','europe','norway','sweden','poland','denmark','finland','switzerland','moldova','bulgaria']
    .forEach(g => { if (q.includes(g)) chips.push(`<span class="i-chip chip-geo"><span class="lbl">geo</span>${cap(g)}</span>`); });
  ['supplier','software','saas','logistics','pharma','b2b','b2c','manufacturer','packaging']
    .forEach(r => { if (q.includes(r)) chips.push(`<span class="i-chip chip-role"><span class="lbl">role</span>${cap(r)}</span>`); });

  const sm = q.match(/(\d[\d,]*)\+?\s*emp/);
  if (sm) chips.push(`<span class="i-chip chip-size"><span class="lbl">size</span>${sm[1]}+ emp</span>`);
  if (q.match(/revenue|\$\d/)) chips.push(`<span class="i-chip chip-rev"><span class="lbl">filter</span>revenue</span>`);
  if (q.includes('public'))    chips.push(`<span class="i-chip chip-tag"><span class="lbl">type</span>public</span>`);

  if (chips.length) { strip.innerHTML = chips.join(''); show('#intentStrip'); }
  else              { hide('#intentStrip'); }
}

/*  Skeleton  */
function showSkeleton() {
  hide('#emptyState'); hide('#errorBanner');
  hide('#resultsHeader'); hide('#intentStrip');
  $('#resultsList').innerHTML = [70, 55, 65, 58].map((w, i) => `
    <div class="skel-card" style="animation-delay:${i * 55}ms">
      <div style="display:flex;gap:10px;margin-bottom:12px">
        <div class="skel" style="height:16px;width:${w}%;border-radius:4px"></div>
      </div>
      <div style="display:flex;gap:10px;margin-bottom:10px">
        <div class="skel" style="height:11px;width:30%;border-radius:3px"></div>
        <div class="skel" style="height:11px;width:22%;border-radius:3px"></div>
      </div>
      <div class="skel" style="height:11px;width:88%;border-radius:3px;margin-bottom:6px"></div>
      <div class="skel" style="height:11px;width:72%;border-radius:3px"></div>
    </div>`).join('');
}

/*  Error  */
function renderError(msg) {
  hide('#emptyState'); hide('#resultsHeader'); hide('#intentStrip');
  $('#resultsList').innerHTML = '';
  $('#errorDetail').textContent = `${msg} — ensure the backend API is running and accessible.`;
  show('#errorBanner');
}

/*  Sidebar helpers  */
function fillQuery(el) {
  const ta = $('#query');
  ta.value = el.textContent.trim();
  autoResize(ta); ta.focus();
}

function toggleExamples() {
  state.examplesOpen = !state.examplesOpen;
  const list = $('#examplesList');
  list.style.display = state.examplesOpen ? '' : 'none';
  $('#exBtn').textContent = state.examplesOpen ? 'hide' : 'show';
}

function autoResize(el) {
  el.style.height = 'auto';
  el.style.height = Math.min(el.scrollHeight, 130) + 'px';
}

/*  Init  */
document.addEventListener('DOMContentLoaded', () => {
  const ta = $('#query');
  ta.addEventListener('input', () => autoResize(ta));
  ta.addEventListener('keydown', e => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); runSearch(); }
    if (e.key === 'Enter' && !e.shiftKey && !e.metaKey && !e.ctrlKey) e.preventDefault();
  });

  checkHealth();
  setInterval(checkHealth, 30_000);
});
