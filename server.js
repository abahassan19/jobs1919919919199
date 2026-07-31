const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const BASE_DIR = process.env.RENDER_PERSISTENT_DISK || __dirname;
const SEARCHES_DIR = path.join(BASE_DIR, 'searches');
const TERMS_FILE = path.join(BASE_DIR, 'terms.json');

if (!fs.existsSync(SEARCHES_DIR)) fs.mkdirSync(SEARCHES_DIR, { recursive: true });

console.log(`Data directory: ${BASE_DIR}`);
console.log(`Persistent storage: ${process.env.RENDER_PERSISTENT_DISK ? 'enabled' : 'disabled'}`);

// ─── State ──────────────────────────────────────────────────────────────
let searchTerms = [];                 // { term, averagePrice, thresholdPercent, interval }
let clients = new Map();              // clientId -> { ws, busy, lastPing }
let jobQueue = [];                   // { term, type }  type = 'scan_new' or 'scan_all'
let activeJobs = new Map();           // term -> { clientId, jobId, startTime, type }
let bargains = {};
let frontendClients = new Set();

// Load persisted terms
if (fs.existsSync(TERMS_FILE)) {
  try { searchTerms = JSON.parse(fs.readFileSync(TERMS_FILE, 'utf8')); } catch (_) { searchTerms = []; }
}
function saveTerms() { fs.writeFileSync(TERMS_FILE, JSON.stringify(searchTerms, null, 2)); }

// ─── File helpers ──────────────────────────────────────────────────────
function getResultsFile(term) {
  const safe = term.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '');
  return path.join(SEARCHES_DIR, `${safe}-results.json`);
}
function getUniqueFile(term) {
  const safe = term.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '');
  return path.join(SEARCHES_DIR, `${safe}-unique.json`);
}
function loadHistory(term) {
  const file = getResultsFile(term);
  try {
    if (fs.existsSync(file)) {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      return data.listings || [];
    }
  } catch (_) {}
  return [];
}
function saveHistory(term, listings) {
  const file = getResultsFile(term);
  const data = { searchTerm: term, lastUpdated: new Date().toISOString(), totalListings: listings.length, listings };
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}
function saveUnique(term, uniqueListings) {
  const file = getUniqueFile(term);
  const data = { searchTerm: term, lastUpdated: new Date().toISOString(), totalUniqueListings: uniqueListings.length, listings: uniqueListings };
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}

// ─── Price analysis ────────────────────────────────────────────────────
function analyzePrices(listings, averagePrice, thresholdPercent) {
  if (!listings || listings.length === 0) return { bargains: [], stats: null };
  
  const prices = listings
    .map(l => parseFloat(l.price.replace(/[^0-9.]/g, '')))
    .filter(p => !isNaN(p) && p > 0);
  if (prices.length === 0) return { bargains: [], stats: null };
  
  const avg = averagePrice || prices.reduce((a, b) => a + b, 0) / prices.length;
  const threshold = thresholdPercent || 20;
  
  const bargains = listings.filter(l => {
    const price = parseFloat(l.price.replace(/[^0-9.]/g, ''));
    if (isNaN(price)) return false;
    const discount = ((avg - price) / avg) * 100;
    return discount >= threshold;
  }).map(l => {
    const price = parseFloat(l.price.replace(/[^0-9.]/g, ''));
    const discount = ((avg - price) / avg) * 100;
    return { ...l, discount: Math.round(discount * 100) / 100 };
  });
  
  return {
    bargains: bargains.sort((a, b) => b.discount - a.discount),
    stats: { average: avg, threshold, totalListings: listings.length, bargainCount: bargains.length }
  };
}

// ─── Process scraped data ─────────────────────────────────────────────
function processScrapedListings(term, scraped, jobType) {
  // For scan_all: compute average from scraped data, don't save or detect bargains
  if (jobType === 'scan_all') {
    const prices = scraped
      .map(l => parseFloat(l.price.replace(/[^0-9.]/g, '')))
      .filter(p => !isNaN(p) && p > 0);
    if (prices.length > 0) {
      const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
      const termObj = searchTerms.find(t => t.term === term);
      if (termObj) {
        termObj.averagePrice = Math.round(avg * 100) / 100;
        saveTerms();
        console.log(`Average for "${term}" set to £${termObj.averagePrice}`);
        broadcastUpdate();
      }
    }
    return { added: 0, newListings: [], averageComputed: true };
  }

  // Normal scan_new flow: deduplicate and save
  const existing = loadHistory(term);
  const existingLinks = new Set(existing.map(item => item.link));
  const newListings = scraped.filter(item => !existingLinks.has(item.link));
  if (newListings.length === 0) return { added: 0, newListings: [] };

  const now = new Date().toISOString();
  newListings.forEach(item => item.firstSeen = now);

  const updated = [...newListings, ...existing];
  saveHistory(term, updated);
  saveUnique(term, newListings);

  // Only detect bargains if average is set
  const termConfig = searchTerms.find(t => t.term === term);
  if (termConfig && termConfig.averagePrice !== null && termConfig.averagePrice !== undefined) {
    const analysis = analyzePrices(updated, termConfig.averagePrice, termConfig.thresholdPercent);
    if (analysis.bargains.length > 0) {
      bargains[term] = analysis.bargains;
      broadcastBargains(term, analysis.bargains);
    }
  }

  return { added: newListings.length, newListings };
}

// ─── Broadcast functions ──────────────────────────────────────────────
function broadcastBargains(term, bargainsList) {
  const msg = JSON.stringify({ type: 'bargain-alert', term, bargains: bargainsList });
  for (const ws of frontendClients) if (ws.readyState === WebSocket.OPEN) ws.send(msg);
}

function broadcastUpdate() {
  const msg = JSON.stringify({
    type: 'update',
    terms: searchTerms.map(t => ({
      ...t,
      active: activeJobs.has(t.term),
      listingCount: loadHistory(t.term).length,
      bargainCount: (bargains[t.term] || []).length
    })),
    jobs: jobQueue,
    active: Array.from(activeJobs.keys()),
    clients: clients.size,
    bargains
  });
  for (const ws of frontendClients) if (ws.readyState === WebSocket.OPEN) ws.send(msg);
}

// ─── Job queue ──────────────────────────────────────────────────────────
function scheduleJobs() {
  const activeTerms = new Set(searchTerms.map(t => t.term));
  jobQueue = jobQueue.filter(job => activeTerms.has(job.term));
  
  for (const termObj of searchTerms) {
    const term = termObj.term;
    if (!activeJobs.has(term) && !jobQueue.some(j => j.term === term && j.type === 'scan_new')) {
      jobQueue.push({ term, type: 'scan_new' });
    }
  }
  processQueue();
}

function processQueue() {
  if (jobQueue.length === 0) return;
  
  let availableClient = null;
  for (let [id, info] of clients) {
    if (!info.busy && info.ws.readyState === WebSocket.OPEN) {
      availableClient = id;
      break;
    }
  }
  
  if (!availableClient) {
    console.log('No available clients, waiting...');
    return;
  }
  
  const job = jobQueue.shift();
  const clientInfo = clients.get(availableClient);
  clientInfo.busy = true;
  const jobId = `${job.term}-${job.type}-${Date.now()}`;
  activeJobs.set(job.term, { clientId: availableClient, startTime: Date.now(), jobId, type: job.type });
  
  clientInfo.ws.send(JSON.stringify({
    type: 'job',
    term: job.term,
    jobId: jobId,
    jobType: job.type
  }));
  
  console.log(`Assigned ${job.type} for "${job.term}" to ${availableClient}`);
  broadcastUpdate();
}

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ─── Embedded Frontend ────────────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Vinted Price Monitor</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, sans-serif; margin: 0; background: #f5f6fa; }
    .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
    h1 { font-weight: 400; color: #2c3e50; }
    .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .flex { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    .flex label { font-weight: 500; min-width: 80px; }
    input, select { padding: 8px 12px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; background: white; }
    input { flex: 1; min-width: 160px; }
    button { padding: 8px 16px; background: #3498db; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: 500; }
    button:hover { background: #2980b9; }
    button.secondary { background: #95a5a6; }
    button.secondary:hover { background: #7f8c8d; }
    button.danger { background: #e74c3c; }
    button.danger:hover { background: #c0392b; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #ecf0f1; }
    th { background: #f8f9fa; font-weight: 600; color: #2c3e50; }
    .badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }
    .badge-active { background: #2ecc71; color: white; }
    .badge-idle { background: #bdc3c7; color: #2c3e50; }
    .badge-bargain { background: #e74c3c; color: white; }
    .tabs { display: flex; gap: 8px; margin-bottom: 20px; border-bottom: 2px solid #ddd; }
    .tab { padding: 10px 16px; cursor: pointer; border: none; background: none; font-weight: 500; color: #7f8c8d; }
    .tab.active { color: #3498db; border-bottom: 2px solid #3498db; }
    .tab-content { display: none; }
    .tab-content.active { display: block; }
    .log { background: #2c3e50; color: #ecf0f1; padding: 10px; border-radius: 4px; font-family: monospace; max-height: 200px; overflow-y: auto; font-size: 12px; }
    .log .timestamp { color: #7f8c8d; }
    .log .info { color: #3498db; }
    .log .success { color: #2ecc71; }
    .log .warning { color: #f1c40f; }
    .log .bargain { color: #e74c3c; font-weight: bold; }
    .bargain-item { background: #fef9e7; border-left: 4px solid #e74c3c; padding: 10px; margin: 5px 0; border-radius: 4px; }
    .bargain-item strong { display: block; margin-bottom: 4px; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 10px 0; }
    .stat-box { background: #f8f9fa; padding: 10px; border-radius: 4px; text-align: center; }
    .stat-box .value { font-size: 20px; font-weight: 600; color: #2c3e50; }
    .stat-box .label { font-size: 12px; color: #7f8c8d; }
    .empty { color: #95a5a6; text-align: center; padding: 20px; }
    .help-text { font-size: 12px; color: #95a5a6; margin-top: 4px; }
    .inline-actions { display: flex; gap: 6px; flex-wrap: wrap; }
  </style>
</head>
<body>
<div class="container">
  <h1>Vinted Price Monitor</h1>

  <div class="card">
    <h3>Add Search Term</h3>
    <div class="flex">
      <div style="flex:1; min-width:200px;">
        <label>Term</label>
        <input type="text" id="newTerm" placeholder="e.g., cortiez hoodie" />
      </div>
      <div style="width:120px;">
        <label>Deal %</label>
        <input type="number" id="threshold" value="20" step="1" min="0" />
        <div class="help-text">% below average to trigger bargain</div>
      </div>
      <div style="width:120px;">
        <label>Interval</label>
        <select id="interval">
          <option value="5">5 min</option>
          <option value="10">10 min</option>
          <option value="15">15 min</option>
          <option value="20">20 min</option>
          <option value="30">30 min</option>
          <option value="45">45 min</option>
          <option value="60">60 min</option>
        </select>
        <div class="help-text">How often to scan for new items</div>
      </div>
      <button id="addBtn">Add Term</button>
    </div>
  </div>

  <div class="tabs">
    <button class="tab active" data-tab="terms">Terms</button>
    <button class="tab" data-tab="bargains">Bargains <span id="bargainCount" class="badge" style="background:#e74c3c;color:white;padding:0 8px;">0</span></button>
    <button class="tab" data-tab="workers">Workers</button>
    <button class="tab" data-tab="log">Log</button>
  </div>

  <div id="tab-terms" class="tab-content active">
    <div class="card">
      <h3>Search Terms</h3>
      <div id="termContainer"></div>
    </div>
  </div>

  <div id="tab-bargains" class="tab-content">
    <div class="card">
      <h3>Bargain Alerts</h3>
      <div id="bargainContainer"><div class="empty">No bargains yet.</div></div>
    </div>
  </div>

  <div id="tab-workers" class="tab-content">
    <div class="card">
      <h3>Connected Workers</h3>
      <div id="workersContainer"><div class="empty">No workers connected.</div></div>
    </div>
  </div>

  <div id="tab-log" class="tab-content">
    <div class="card">
      <h3>Activity Log</h3>
      <div class="log" id="logContainer"><div class="info">System ready.</div></div>
    </div>
  </div>
</div>

<script>
  const API_BASE = window.location.origin;
  let bargains = {};

  // Tabs
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    });
  });

  // WebSocket
  const ws = new WebSocket('ws://' + window.location.host);
  ws.onopen = () => ws.send(JSON.stringify({ type: 'register-frontend' }));
  ws.onmessage = e => {
    const d = JSON.parse(e.data);
    if (d.type === 'bargain-alert') {
      addLog('Bargain found for "' + d.term + '": ' + d.bargains.length + ' items', 'bargain');
      renderAll(d);
    } else if (d.type === 'update') {
      renderAll(d);
    }
  };

  function renderAll(data) {
    renderTerms(data.terms);
    renderWorkers(data.clients, data.active);
    if (data.bargains) {
      bargains = data.bargains;
      renderBargains(data.bargains);
      const total = Object.values(data.bargains).reduce((s, a) => s + a.length, 0);
      document.getElementById('bargainCount').textContent = total;
    }
  }

  function renderTerms(terms) {
    const c = document.getElementById('termContainer');
    if (!terms || terms.length === 0) {
      c.innerHTML = '<div class="empty">No search terms. Add one above.</div>';
      return;
    }
    let html = '<table><thead><tr><th>Term</th><th>Avg Price</th><th>Deal %</th><th>Interval</th><th>Status</th><th>Listings</th><th>Bargains</th><th>Actions</th></tr></thead><tbody>';
    terms.forEach(t => {
      const status = t.active ? '<span class="badge badge-active">Scanning</span>' : '<span class="badge badge-idle">Idle</span>';
      const avgDisplay = t.averagePrice ? '£' + t.averagePrice : 'Not set';
      html += '<tr>'
        + '<td><strong>' + t.term + '</strong></td>'
        + '<td>' + avgDisplay + '</td>'
        + '<td>' + t.thresholdPercent + '%</td>'
        + '<td>' + t.interval + ' min</td>'
        + '<td>' + status + '</td>'
        + '<td>' + (t.listingCount || 0) + '</td>'
        + '<td>' + (t.bargainCount || 0) + '</td>'
        + '<td class="inline-actions">'
        + '<button class="secondary" data-action="avg" data-term="' + t.term + '">Calc Average</button>'
        + '<button class="danger" data-action="remove" data-term="' + t.term + '">Remove</button>'
        + '</td>'
        + '</tr>';
    });
    html += '</tbody></table>';
    c.innerHTML = html;

    // Event listeners
    c.querySelectorAll('[data-action="remove"]').forEach(btn => {
      btn.addEventListener('click', async () => {
        const term = btn.dataset.term;
        if (!confirm('Remove "' + term + '"?')) return;
        await fetch(API_BASE + '/terms/' + encodeURIComponent(term), { method: 'DELETE' });
        addLog('Removed term: "' + term + '"', 'warning');
        fetchData();
      });
    });

    c.querySelectorAll('[data-action="avg"]').forEach(btn => {
      btn.addEventListener('click', async () => {
        const term = btn.dataset.term;
        const res = await fetch(API_BASE + '/calculate-average', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ term })
        });
        const data = await res.json();
        if (data.success) {
          addLog('Average calculation started for "' + term + '"', 'info');
          fetchData();
        } else {
          alert(data.error || 'Failed to start calculation');
        }
      });
    });
  }

  function renderWorkers(clientCount, activeJobs) {
    const c = document.getElementById('workersContainer');
    if (clientCount === 0) {
      c.innerHTML = '<div class="empty">No workers connected.</div>';
      return;
    }
    let html = '<div class="stats-grid">'
      + '<div class="stat-box"><div class="value">' + clientCount + '</div><div class="label">Connected Workers</div></div>'
      + '<div class="stat-box"><div class="value">' + (activeJobs ? activeJobs.length : 0) + '</div><div class="label">Active Jobs</div></div>'
      + '</div>';
    if (activeJobs && activeJobs.length > 0) {
      html += '<table><thead><tr><th>Term</th><th>Type</th></tr></thead><tbody>';
      // activeJobs is an array of term strings, but we need to get type from activeJobs map
      // We'll use the data passed from server which includes type
      // For simplicity, just show term names
      activeJobs.forEach(term => {
        html += '<tr><td>' + term + '</td><td>processing</td></tr>';
      });
      html += '</tbody></table>';
    }
    c.innerHTML = html;
  }

  function renderBargains(b) {
    const c = document.getElementById('bargainContainer');
    let html = '';
    let has = false;
    for (const [term, items] of Object.entries(b)) {
      if (items && items.length > 0) {
        has = true;
        html += '<h4>' + term + ' <span class="badge badge-bargain">' + items.length + ' bargains</span></h4>';
        items.slice(0, 20).forEach(item => {
          html += '<div class="bargain-item">'
            + '<strong>' + item.name + '</strong>'
            + '<div style="display:flex;gap:15px;flex-wrap:wrap;font-size:14px;">'
            + '<span>Price: ' + item.price + '</span>'
            + '<span>Discount: ' + item.discount + '%</span>'
            + '<span>Size: ' + (item.size || 'N/A') + '</span>'
            + '<span>Condition: ' + (item.condition || 'N/A') + '</span>'
            + '<a href="' + item.link + '" target="_blank">View</a>'
            + '</div></div>';
        });
        if (items.length > 20) html += '<p>... and ' + (items.length - 20) + ' more</p>';
      }
    }
    c.innerHTML = has ? html : '<div class="empty">No bargains yet.</div>';
  }

  function addLog(msg, type = 'info') {
    const c = document.getElementById('logContainer');
    const time = new Date().toLocaleTimeString();
    const d = document.createElement('div');
    d.innerHTML = '<span class="timestamp">[' + time + ']</span> <span class="' + type + '">' + msg + '</span>';
    c.prepend(d);
    if (c.children.length > 100) c.removeChild(c.lastChild);
  }

  async function fetchData() {
    try {
      const res = await fetch(API_BASE + '/status');
      const data = await res.json();
      renderAll(data);
    } catch (e) { console.error(e); }
  }

  // Add term
  document.getElementById('addBtn').addEventListener('click', async () => {
    const termInput = document.getElementById('newTerm');
    const thresholdInput = document.getElementById('threshold');
    const intervalSelect = document.getElementById('interval');
    const term = termInput.value.trim();
    const threshold = parseInt(thresholdInput.value) || 20;
    const interval = parseInt(intervalSelect.value) || 5;
    if (!term) return;
    try {
      const res = await fetch(API_BASE + '/terms', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ term, thresholdPercent: threshold, interval })
      });
      if (res.ok) {
        addLog('Added term: "' + term + '"', 'success');
        termInput.value = '';
        fetchData();
      } else {
        const err = await res.json();
        alert(err.error || 'Failed to add term');
      }
    } catch (err) { alert('Network error'); }
  });

  // Poll for updates
  setInterval(fetchData, 3000);
  fetchData();
  addLog('Dashboard ready', 'info');
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

// ─── API endpoints ────────────────────────────────────────────────────
app.get('/status', (req, res) => {
  const termStatus = searchTerms.map(termObj => {
    const term = termObj.term;
    const history = loadHistory(term);
    return {
      ...termObj,
      active: activeJobs.has(term),
      listingCount: history.length,
      bargainCount: (bargains[term] || []).length
    };
  });
  res.json({
    terms: termStatus,
    clients: clients.size,
    active: Array.from(activeJobs.keys()),
    queue: jobQueue,
    bargains
  });
});

app.post('/terms', (req, res) => {
  const { term, thresholdPercent = 20, interval = 5 } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  if (searchTerms.find(t => t.term === term)) {
    return res.status(409).json({ error: 'Term already exists' });
  }
  const obj = { term, averagePrice: null, thresholdPercent: parseInt(thresholdPercent), interval: parseInt(interval) };
  searchTerms.push(obj);
  saveTerms();
  // Schedule a scan_new job immediately
  jobQueue.push({ term, type: 'scan_new' });
  processQueue();
  broadcastUpdate();
  res.json({ success: true, term: obj });
});

app.delete('/terms/:term', (req, res) => {
  const term = req.params.term;
  const idx = searchTerms.findIndex(t => t.term === term);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  searchTerms.splice(idx, 1);
  saveTerms();
  jobQueue = jobQueue.filter(j => j.term !== term);
  activeJobs.delete(term);
  delete bargains[term];
  broadcastUpdate();
  res.json({ success: true });
});

app.post('/calculate-average', (req, res) => {
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const termObj = searchTerms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  
  // Check if already active or queued
  if (activeJobs.has(term)) {
    return res.status(409).json({ error: 'Scan already in progress' });
  }
  if (jobQueue.some(j => j.term === term && j.type === 'scan_all')) {
    return res.status(409).json({ error: 'Average calculation already queued' });
  }
  
  // Add scan_all job to the front of the queue
  jobQueue.unshift({ term, type: 'scan_all' });
  processQueue();
  broadcastUpdate();
  res.json({ success: true, message: 'Average calculation started' });
});

app.get('/searches', (req, res) => {
  const { term, type } = req.query;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  let listings = [];
  if (type === 'unique') {
    const file = getUniqueFile(term);
    try { if (fs.existsSync(file)) { const data = JSON.parse(fs.readFileSync(file, 'utf8')); listings = data.listings || []; } } catch (_) {}
  } else {
    listings = loadHistory(term);
  }
  res.json({ term, type: type || 'all', count: listings.length, listings: listings.slice(0, 100) });
});

// ─── WebSocket server ──────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, clientTracking: true });

wss.on('connection', (ws, req) => {
  let clientId = null;
  ws.isFrontend = false;

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      console.log(`Received: ${data.type} from ${clientId || 'unregistered'}`);
      
      switch (data.type) {
        case 'register-frontend':
          ws.isFrontend = true;
          frontendClients.add(ws);
          console.log('Frontend connected');
          broadcastUpdate();
          break;
          
        case 'register-worker':
          clientId = data.clientId || `worker-${Date.now()}`;
          if (clients.has(clientId)) {
            const old = clients.get(clientId);
            if (old.ws !== ws && old.ws.readyState === WebSocket.OPEN) old.ws.close();
          }
          clients.set(clientId, { ws, busy: false, lastPing: Date.now() });
          console.log(`Worker registered: ${clientId} (${clients.size} total)`);
          ws.send(JSON.stringify({ type: 'registered', clientId }));
          processQueue();
          broadcastUpdate();
          break;
          
        case 'job-complete': {
          const { term, listings, jobId, jobType } = data;
          if (!term || !listings) break;
          
          console.log(`Job complete for "${term}" (${jobType || 'scan_new'}) from ${clientId}`);
          
          // Process based on job type
          const result = processScrapedListings(term, listings, jobType || 'scan_new');
          
          // Mark client as not busy
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).busy = false;
          }
          
          // Remove from active jobs
          if (activeJobs.has(term)) {
            activeJobs.delete(term);
          }
          
          // Acknowledge
          ws.send(JSON.stringify({
            type: 'job-complete-ack',
            term,
            added: result.added || 0,
            newListings: result.newListings || []
          }));
          
          console.log(`Job complete for "${term}" - added ${result.added || 0} new`);
          
          // Schedule next jobs
          scheduleJobs();
          broadcastUpdate();
          break;
        }
        
        case 'job-failed': {
          const { term, error, jobType } = data;
          console.log(`Job failed for "${term}" (${jobType || 'scan_new'}) from ${clientId}: ${error}`);
          
          // Mark client as not busy
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).busy = false;
          }
          
          // Remove from active jobs
          if (activeJobs.has(term)) {
            activeJobs.delete(term);
          }
          
          // Re-add to queue if it was a scan_new job (scan_all should not retry automatically)
          const type = jobType || 'scan_new';
          if (type === 'scan_new' && !jobQueue.some(j => j.term === term && j.type === 'scan_new')) {
            jobQueue.push({ term, type: 'scan_new' });
          }
          
          processQueue();
          broadcastUpdate();
          break;
        }
        
        case 'ping': {
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).lastPing = Date.now();
          }
          ws.send(JSON.stringify({ type: 'pong' }));
          break;
        }
        
        default:
          console.log(`Unknown message type: ${data.type}`);
      }
    } catch (err) {
      console.error('WebSocket message error:', err);
    }
  });

  ws.on('close', (code) => {
    if (ws.isFrontend) {
      frontendClients.delete(ws);
      console.log('Frontend disconnected');
      return;
    }
    if (clientId) {
      // Find any active job for this client
      let lostTerm = null;
      for (const [term, job] of activeJobs) {
        if (job.clientId === clientId) {
          lostTerm = term;
          activeJobs.delete(term);
          break;
        }
      }
      clients.delete(clientId);
      console.log(`Worker disconnected: ${clientId} (${clients.size} remaining) code ${code}`);
      if (lostTerm) {
        // Re-queue scan_new only
        if (!jobQueue.some(j => j.term === lostTerm && j.type === 'scan_new')) {
          jobQueue.push({ term: lostTerm, type: 'scan_new' });
        }
        processQueue();
      }
      broadcastUpdate();
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

// ─── Heartbeat check ──────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (let [id, info] of clients) {
    if (now - info.lastPing > 30000) {
      console.log(`Worker ${id} stale, terminating`);
      info.ws.terminate();
      clients.delete(id);
    }
  }
}, 10000);

// ─── Scheduler ─────────────────────────────────────────────────────────
setInterval(() => {
  for (const termObj of searchTerms) {
    const term = termObj.term;
    const interval = termObj.interval || 5;
    if (!activeJobs.has(term) && !jobQueue.some(j => j.term === term && j.type === 'scan_new')) {
      const history = loadHistory(term);
      if (history.length > 0) {
        const last = new Date(history[0]?.firstSeen || 0);
        const mins = (Date.now() - last.getTime()) / 60000;
        if (mins >= interval) {
          jobQueue.push({ term, type: 'scan_new' });
          console.log(`Scheduling scan_new for "${term}" (last ${Math.round(mins)}m ago)`);
        }
      } else {
        jobQueue.push({ term, type: 'scan_new' });
        console.log(`Scheduling scan_new for "${term}" (first scan)`);
      }
    }
  }
  processQueue();
  broadcastUpdate();
}, 60000);

// ─── Start server ──────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Vinted Price Monitor Server`);
  console.log(`${'='.repeat(60)}`);
  console.log(`HTTP: http://localhost:${PORT}`);
  console.log(`WebSocket: ws://localhost:${PORT}`);
  console.log(`Data directory: ${BASE_DIR}`);
  console.log(`Persistent storage: ${process.env.RENDER_PERSISTENT_DISK ? 'enabled' : 'disabled'}`);
  console.log(`${searchTerms.length} terms loaded`);
  console.log(`${'='.repeat(60)}\n`);
  
  setTimeout(() => {
    scheduleJobs();
    broadcastUpdate();
  }, 2000);
});
