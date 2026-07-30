const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const SEARCHES_DIR = path.join(__dirname, 'searches');

// Ensure directories
if (!fs.existsSync(SEARCHES_DIR)) fs.mkdirSync(SEARCHES_DIR, { recursive: true });

// ─── State ──────────────────────────────────────────────────────────────
let searchTerms = [];
let clients = new Map();
let jobQueue = [];
let activeJobs = new Map();
let bargains = {};
let scrapedHistory = {};
let frontendClients = new Set();

const TERMS_FILE = path.join(__dirname, 'terms.json');

// Load persisted terms
if (fs.existsSync(TERMS_FILE)) {
  try { searchTerms = JSON.parse(fs.readFileSync(TERMS_FILE, 'utf8')); } catch (_) { searchTerms = []; }
}

function saveTerms() {
  fs.writeFileSync(TERMS_FILE, JSON.stringify(searchTerms, null, 2));
}

// ─── File helpers ──────────────────────────────────────────────────
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
  const data = {
    searchTerm: term,
    lastUpdated: new Date().toISOString(),
    totalListings: listings.length,
    listings
  };
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}

function saveUnique(term, uniqueListings) {
  const file = getUniqueFile(term);
  const data = {
    searchTerm: term,
    lastUpdated: new Date().toISOString(),
    totalUniqueListings: uniqueListings.length,
    listings: uniqueListings
  };
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}

// ─── Price analysis ─────────────────────────────────────────────────
function analyzePrices(listings, averagePrice, thresholdPercent) {
  if (!listings || listings.length === 0) return { bargains: [], stats: null };
  
  const prices = listings
    .map(l => parseFloat(l.price.replace(/[^0-9.]/g, '')))
    .filter(p => !isNaN(p));
  
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
    stats: {
      average: avg,
      threshold: threshold,
      totalListings: listings.length,
      bargainCount: bargains.length,
      lowestPrice: Math.min(...prices),
      highestPrice: Math.max(...prices)
    }
  };
}

function processScrapedListings(term, scraped) {
  const existing = loadHistory(term);
  const existingLinks = new Set(existing.map(item => item.link));
  const newListings = scraped.filter(item => !existingLinks.has(item.link));
  
  if (newListings.length === 0) return { added: 0, newListings: [] };

  const now = new Date().toISOString();
  newListings.forEach(item => item.firstSeen = now);

  const updated = [...newListings, ...existing];
  saveHistory(term, updated);
  saveUnique(term, newListings);
  
  // Store in memory
  scrapedHistory[term] = { listings: updated, unique: newListings };
  
  // Analyze for bargains
  const termConfig = searchTerms.find(t => t.term === term);
  if (termConfig) {
    const analysis = analyzePrices(updated, termConfig.averagePrice, termConfig.thresholdPercent);
    if (analysis.bargains.length > 0) {
      bargains[term] = analysis.bargains;
      broadcastBargains(term, analysis.bargains);
    }
  }

  return { added: newListings.length, newListings };
}

// ─── Broadcast ────────────────────────────────────────────────────
function broadcastBargains(term, bargainsList) {
  const message = JSON.stringify({
    type: 'bargain-alert',
    term,
    bargains: bargainsList
  });
  for (const ws of frontendClients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  }
}

function broadcastUpdate() {
  const message = JSON.stringify({
    type: 'update',
    terms: searchTerms.map(t => ({
      ...t,
      active: activeJobs.has(t.term),
      listingCount: scrapedHistory[t.term]?.listings?.length || 0,
      bargainCount: bargains[t.term]?.length || 0
    })),
    jobs: jobQueue,
    active: Array.from(activeJobs.keys()),
    clients: clients.size,
    bargains: bargains
  });
  for (const ws of frontendClients) {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(message);
    }
  }
}

// ─── Job management ─────────────────────────────────────────────────
function scheduleJobs() {
  const activeTerms = new Set(searchTerms.map(t => t.term));
  jobQueue = jobQueue.filter(term => activeTerms.has(term));
  
  for (const termObj of searchTerms) {
    const term = termObj.term;
    if (!activeJobs.has(term) && !jobQueue.includes(term)) {
      jobQueue.push(term);
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
    console.log('⏳ No available clients, waiting...');
    return;
  }
  
  const term = jobQueue.shift();
  const clientInfo = clients.get(availableClient);
  clientInfo.busy = true;
  const jobId = `${term}-${Date.now()}`;
  activeJobs.set(term, { clientId: availableClient, startTime: Date.now(), jobId });
  
  clientInfo.ws.send(JSON.stringify({
    type: 'job',
    term: term,
    jobId: jobId
  }));
  
  console.log(`📤 Assigned job "${term}" to ${availableClient}`);
  broadcastUpdate();
}

// ─── Express app ────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ─── Embedded Frontend ──────────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Vinted Scraper Central</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 0; background: #f0f2f5; }
    .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
    h1 { color: #1a1a2e; margin-top: 0; }
    h1 small { font-size: 14px; font-weight: normal; color: #666; }
    .card { background: white; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
    .flex { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    input, select { padding: 8px 12px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px; }
    input { flex: 1; min-width: 150px; }
    button { padding: 8px 16px; background: #4a6cf7; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: 500; font-size: 14px; }
    button:hover { background: #3a5bd9; }
    button.danger { background: #dc3545; }
    button.danger:hover { background: #c82333; }
    button.success { background: #28a745; }
    button.success:hover { background: #218838; }
    button.warning { background: #ffc107; color: #333; }
    button.warning:hover { background: #e0a800; }
    button.small { padding: 4px 12px; font-size: 12px; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th, td { padding: 10px 12px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #f8f9fa; font-weight: 600; }
    .badge { display: inline-block; padding: 3px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; }
    .badge-active { background: #28a745; color: white; }
    .badge-idle { background: #6c757d; color: white; }
    .badge-busy { background: #ffc107; color: #333; }
    .badge-bargain { background: #dc3545; color: white; animation: pulse 1s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
    .tabs { display: flex; gap: 10px; margin-bottom: 20px; border-bottom: 2px solid #e0e0e0; flex-wrap: wrap; }
    .tab { padding: 10px 20px; cursor: pointer; border: none; background: none; font-weight: 500; color: #666; font-size: 14px; }
    .tab.active { color: #4a6cf7; border-bottom: 3px solid #4a6cf7; }
    .tab-content { display: none; }
    .tab-content.active { display: block; }
    .log { background: #1e1e1e; color: #d4d4d4; padding: 10px; border-radius: 6px; font-family: monospace; max-height: 200px; overflow-y: auto; font-size: 12px; }
    .log .timestamp { color: #6a9955; }
    .log .info { color: #569cd6; }
    .log .success { color: #4ec9b0; }
    .log .warning { color: #dcdcaa; }
    .log .bargain { color: #ce9178; font-weight: bold; }
    .bargain-item { background: #fff5f5; border-left: 4px solid #dc3545; padding: 10px; margin: 5px 0; border-radius: 4px; }
    .bargain-item strong { display: block; margin-bottom: 4px; }
    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 10px 0; }
    .stat-box { background: #f8f9fa; padding: 10px; border-radius: 6px; text-align: center; }
    .stat-box .value { font-size: 20px; font-weight: bold; color: #1a1a2e; }
    .stat-box .label { font-size: 12px; color: #666; }
    .term-status { display: flex; gap: 5px; flex-wrap: wrap; }
    .empty { color: #999; text-align: center; padding: 20px; }
    @media (max-width: 768px) { .container { padding: 10px; } table { font-size: 12px; } th, td { padding: 6px; } }
  </style>
</head>
<body>
<div class="container">
  <h1>🛍️ Vinted Scraper <small>Centralized Price Monitor</small></h1>

  <div class="card">
    <h2>➕ Add Search Term</h2>
    <div class="flex">
      <input type="text" id="newTerm" placeholder="e.g., cortiez hoodie" />
      <input type="number" id="avgPrice" placeholder="Avg price (GBP)" step="0.01" style="width:140px;" />
      <input type="number" id="threshold" placeholder="Deal %" value="20" step="1" style="width:110px;" />
      <input type="number" id="interval" placeholder="Min" value="5" step="1" style="width:100px;" />
      <button id="addBtn">Add Term</button>
    </div>
  </div>

  <div class="tabs">
    <button class="tab active" data-tab="terms">📋 Terms</button>
    <button class="tab" data-tab="bargains">💎 Bargains <span id="bargainCount" class="badge" style="background:#dc3545;color:white;padding:2px 10px;">0</span></button>
    <button class="tab" data-tab="workers">🤖 Workers</button>
    <button class="tab" data-tab="history">📜 History</button>
  </div>

  <div id="tab-terms" class="tab-content active">
    <div class="card">
      <h2>📋 Search Terms</h2>
      <div id="termContainer"></div>
    </div>
  </div>

  <div id="tab-bargains" class="tab-content">
    <div class="card">
      <h2>💎 Bargain Alerts</h2>
      <div id="bargainContainer"><div class="empty">No bargains found yet. Keep scraping!</div></div>
    </div>
  </div>

  <div id="tab-workers" class="tab-content">
    <div class="card">
      <h2>🤖 Connected Workers</h2>
      <div id="workersContainer"><div class="empty">No workers connected.</div></div>
    </div>
  </div>

  <div id="tab-history" class="tab-content">
    <div class="card">
      <h2>📜 Recent Activity</h2>
      <div class="log" id="logContainer">
        <div class="info">Waiting for events...</div>
      </div>
    </div>
  </div>
</div>

<script>
  const API_BASE = window.location.origin;
  let bargains = {};

  // ─── Tabs ──────────────────────────────────────────────────────────
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
    });
  });

  // ─── WebSocket ─────────────────────────────────────────────────────
  const ws = new WebSocket(\`ws://\${window.location.host}\`);
  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    if (data.type === 'bargain-alert') {
      addLog(\`💰 BARGAIN FOUND for "\${data.term}": \${data.bargains.length} items!\`, 'bargain');
      renderAll(data);
    } else if (data.type === 'update') {
      renderAll(data);
    }
  };

  function renderAll(data) {
    renderTerms(data.terms);
    renderWorkers(data.clients, data.active);
    if (data.bargains) {
      bargains = data.bargains;
      renderBargains(data.bargains);
      const total = Object.values(data.bargains).reduce((sum, arr) => sum + arr.length, 0);
      document.getElementById('bargainCount').textContent = total;
    }
  }

  // ─── Render Functions ─────────────────────────────────────────────
  function renderTerms(terms) {
    const container = document.getElementById('termContainer');
    if (!terms || terms.length === 0) {
      container.innerHTML = '<div class="empty">No search terms configured. Add one above!</div>';
      return;
    }
    let html = '<table><thead><tr><th>Term</th><th>Avg Price</th><th>Deal %</th><th>Interval</th><th>Status</th><th>Listings</th><th>Bargains</th><th>Action</th></tr></thead><tbody>';
    terms.forEach(t => {
      const status = t.active ? '<span class="badge badge-active">Scraping</span>' : '<span class="badge badge-idle">Idle</span>';
      html += \`
        <tr>
          <td><strong>\${t.term}</strong></td>
          <td>£\${t.averagePrice || 'auto'}</td>
          <td>\${t.thresholdPercent || 20}%</td>
          <td>\${t.interval || 5}m</td>
          <td>\${status}</td>
          <td>\${t.listingCount || 0}</td>
          <td>\${t.bargainCount || 0}</td>
          <td><button class="danger small" data-term="\${t.term}">Remove</button></td>
        </tr>
      \`;
    });
    html += '</tbody></table>';
    container.innerHTML = html;

    document.querySelectorAll('[data-term]').forEach(btn => {
      btn.addEventListener('click', async () => {
        const term = btn.dataset.term;
        if (!confirm(\`Remove "\${term}"?\`)) return;
        await fetch(\`\${API_BASE}/terms/\${encodeURIComponent(term)}\`, { method: 'DELETE' });
        addLog(\`🗑️ Removed term: "\${term}"\`, 'warning');
        fetchData();
      });
    });
  }

  function renderWorkers(clientCount, activeJobs) {
    const container = document.getElementById('workersContainer');
    container.innerHTML = \`
      <div class="stats-grid">
        <div class="stat-box"><div class="value">\${clientCount}</div><div class="label">Connected Workers</div></div>
        <div class="stat-box"><div class="value">\${activeJobs ? activeJobs.length : 0}</div><div class="label">Active Jobs</div></div>
        <div class="stat-box"><div class="value">\${clientCount - (activeJobs ? activeJobs.length : 0)}</div><div class="label">Idle Workers</div></div>
      </div>
    \`;
  }

  function renderBargains(bargainsData) {
    const container = document.getElementById('bargainContainer');
    let html = '';
    let hasBargains = false;
    for (const [term, items] of Object.entries(bargainsData)) {
      if (items && items.length > 0) {
        hasBargains = true;
        html += \`<h3 style="margin:15px 0 5px;">\${term} <span class="badge badge-bargain">\${items.length} bargains</span></h3>\`;
        items.slice(0, 20).forEach(item => {
          html += \`
            <div class="bargain-item">
              <strong>\${item.name}</strong>
              <div style="display:flex;gap:15px;flex-wrap:wrap;font-size:14px;">
                <span>💰 Price: \${item.price}</span>
                <span>📉 \${item.discount}% below average</span>
                <span>📏 Size: \${item.size || 'N/A'}</span>
                <span>📦 Condition: \${item.condition || 'N/A'}</span>
                <a href="\${item.link}" target="_blank" style="color:#4a6cf7;">View on Vinted →</a>
              </div>
            </div>
          \`;
        });
        if (items.length > 20) {
          html += \`<p style="color:#666;">... and \${items.length - 20} more bargains</p>\`;
        }
      }
    }
    container.innerHTML = hasBargains ? html : '<div class="empty">No bargains found yet. Keep scraping!</div>';
  }

  function addLog(message, type = 'info') {
    const container = document.getElementById('logContainer');
    const time = new Date().toLocaleTimeString();
    const div = document.createElement('div');
    div.innerHTML = \`<span class="timestamp">[\${time}]</span> <span class="\${type}">\${message}</span>\`;
    container.prepend(div);
    if (container.children.length > 100) container.removeChild(container.lastChild);
  }

  // ─── API Calls ─────────────────────────────────────────────────────
  async function fetchData() {
    try {
      const res = await fetch(\`\${API_BASE}/status\`);
      const data = await res.json();
      renderAll(data);
    } catch (err) {
      console.error('Fetch error:', err);
    }
  }

  document.getElementById('addBtn').addEventListener('click', async () => {
    const termInput = document.getElementById('newTerm');
    const avgInput = document.getElementById('avgPrice');
    const thresholdInput = document.getElementById('threshold');
    const intervalInput = document.getElementById('interval');
    
    const term = termInput.value.trim();
    const averagePrice = parseFloat(avgInput.value) || null;
    const thresholdPercent = parseInt(thresholdInput.value) || 20;
    const interval = parseInt(intervalInput.value) || 5;
    
    if (!term) return;
    
    try {
      const res = await fetch(\`\${API_BASE}/terms\`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ term, averagePrice, thresholdPercent, interval })
      });
      if (res.ok) {
        addLog(\`➕ Added term: "\${term}" (deal: \${thresholdPercent}% below avg)\`, 'success');
        termInput.value = '';
        fetchData();
      } else {
        const err = await res.json();
        alert(err.error || 'Failed to add term');
      }
    } catch (err) {
      alert('Network error');
    }
  });

  // Poll every 3 seconds
  setInterval(fetchData, 3000);
  fetchData();
  addLog('🚀 Dashboard started', 'info');
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

// ─── API endpoints ─────────────────────────────────────────────────

app.get('/status', (req, res) => {
  const termStatus = searchTerms.map(termObj => {
    const term = termObj.term;
    const history = loadHistory(term);
    const bargainList = bargains[term] || [];
    return {
      ...termObj,
      active: activeJobs.has(term),
      listingCount: history.length,
      bargainCount: bargainList.length
    };
  });
  
  res.json({
    terms: termStatus,
    clients: clients.size,
    active: Array.from(activeJobs.keys()),
    queue: jobQueue,
    bargains: bargains
  });
});

app.post('/terms', (req, res) => {
  const { term, averagePrice, thresholdPercent = 20, interval = 5 } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  if (searchTerms.find(t => t.term === term)) {
    return res.status(409).json({ error: 'Term already exists' });
  }
  
  const termObj = { 
    term, 
    averagePrice: averagePrice || null, 
    thresholdPercent: parseInt(thresholdPercent) || 20,
    interval: parseInt(interval) || 5
  };
  searchTerms.push(termObj);
  saveTerms();
  
  jobQueue.push(term);
  processQueue();
  broadcastUpdate();
  
  res.json({ success: true, term: termObj });
});

app.delete('/terms/:term', (req, res) => {
  const term = req.params.term;
  const idx = searchTerms.findIndex(t => t.term === term);
  if (idx === -1) return res.status(404).json({ error: 'Term not found' });
  
  searchTerms.splice(idx, 1);
  saveTerms();
  
  jobQueue = jobQueue.filter(t => t !== term);
  activeJobs.delete(term);
  delete bargains[term];
  
  broadcastUpdate();
  res.json({ success: true });
});

app.get('/searches', (req, res) => {
  const { term, type } = req.query;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  
  let listings = [];
  if (type === 'unique') {
    const file = getUniqueFile(term);
    try {
      if (fs.existsSync(file)) {
        const data = JSON.parse(fs.readFileSync(file, 'utf8'));
        listings = data.listings || [];
      }
    } catch (_) {}
  } else {
    listings = loadHistory(term);
  }
  
  const termConfig = searchTerms.find(t => t.term === term);
  const analysis = analyzePrices(listings, termConfig?.averagePrice, termConfig?.thresholdPercent);
  
  res.json({ 
    term, 
    type: type || 'all', 
    count: listings.length,
    listings: listings.slice(0, 100), // Limit for performance
    bargains: analysis?.bargains || [],
    stats: analysis?.stats || null
  });
});

// ─── WebSocket server ──────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  let clientId = null;
  ws.isFrontend = false;
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      switch (data.type) {
        case 'register-frontend':
          ws.isFrontend = true;
          frontendClients.add(ws);
          console.log('🌐 Frontend client connected');
          broadcastUpdate();
          break;
          
        case 'register-worker': {
          clientId = data.clientId || `worker-${Date.now()}`;
          if (clients.has(clientId)) {
            const old = clients.get(clientId);
            if (old.ws !== ws && old.ws.readyState === WebSocket.OPEN) {
              old.ws.close();
            }
          }
          clients.set(clientId, { ws, busy: false, lastPing: Date.now() });
          console.log(`🤖 Worker registered: ${clientId} (${clients.size} total)`);
          ws.send(JSON.stringify({ type: 'registered', clientId }));
          processQueue();
          broadcastUpdate();
          break;
        }
        
        case 'job-complete': {
          const { term, listings, jobId } = data;
          if (!term || !listings) break;
          
          const result = processScrapedListings(term, listings);
          
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).busy = false;
          }
          
          if (activeJobs.has(term)) {
            activeJobs.delete(term);
          }
          
          ws.send(JSON.stringify({
            type: 'job-complete-ack',
            term,
            added: result.added,
            newListings: result.newListings
          }));
          
          console.log(`✅ Job complete for "${term}" - added ${result.added} new listings`);
          
          scheduleJobs();
          broadcastUpdate();
          break;
        }
        
        case 'job-failed': {
          const { term, error } = data;
          console.log(`❌ Job failed for "${term}": ${error}`);
          
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).busy = false;
          }
          
          if (activeJobs.has(term)) {
            activeJobs.delete(term);
          }
          
          if (!jobQueue.includes(term)) {
            jobQueue.push(term);
          }
          
          processQueue();
          broadcastUpdate();
          break;
        }
        
        case 'ping':
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).lastPing = Date.now();
          }
          break;
      }
    } catch (err) {
      console.error('WebSocket error:', err);
    }
  });

  ws.on('close', () => {
    if (ws.isFrontend) {
      frontendClients.delete(ws);
      console.log('🌐 Frontend client disconnected');
    } else if (clientId) {
      let lostJob = null;
      for (let [term, job] of activeJobs) {
        if (job.clientId === clientId) {
          lostJob = term;
          break;
        }
      }
      
      clients.delete(clientId);
      console.log(`❌ Worker disconnected: ${clientId} (${clients.size} remaining)`);
      
      if (lostJob) {
        activeJobs.delete(lostJob);
        if (!jobQueue.includes(lostJob)) {
          jobQueue.push(lostJob);
        }
        console.log(`🔄 Re-queued job "${lostJob}" from disconnected worker`);
      }
      
      processQueue();
      broadcastUpdate();
    }
  });
});

// ─── Heartbeat ────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (let [id, info] of clients) {
    if (now - info.lastPing > 30000) {
      console.log(`⚠️ Worker ${id} stale, closing`);
      info.ws.terminate();
      clients.delete(id);
    }
  }
}, 10000);

// ─── Job scheduler ──────────────────────────────────────────────────
setInterval(() => {
  for (const termObj of searchTerms) {
    const term = termObj.term;
    const interval = termObj.interval || 5;
    
    if (!activeJobs.has(term) && !jobQueue.includes(term)) {
      const history = loadHistory(term);
      if (history.length > 0) {
        const lastScraped = new Date(history[0]?.firstSeen || 0);
        const minutesSince = (Date.now() - lastScraped.getTime()) / 60000;
        if (minutesSince >= interval) {
          jobQueue.push(term);
          console.log(`⏰ Scheduling "${term}" (last scraped ${Math.round(minutesSince)}m ago)`);
        }
      } else {
        jobQueue.push(term);
        console.log(`⏰ Scheduling "${term}" for first scrape`);
      }
    }
  }
  processQueue();
  broadcastUpdate();
}, 60000);

// ─── Start server ──────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`🚀 Vinted Scraper Central Server`);
  console.log(`${'='.repeat(60)}`);
  console.log(`📡 HTTP: http://localhost:${PORT}`);
  console.log(`🔌 WebSocket: ws://localhost:${PORT}`);
  console.log(`📁 Search data: ${SEARCHES_DIR}`);
  console.log(`📊 ${searchTerms.length} terms loaded`);
  console.log(`${'='.repeat(60)}\n`);
  
  setTimeout(() => {
    scheduleJobs();
    broadcastUpdate();
  }, 2000);
});
