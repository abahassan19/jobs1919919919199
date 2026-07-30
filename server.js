const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const SEARCHES_DIR = path.join(__dirname, 'searches');
const TERMS_FILE = path.join(__dirname, 'terms.json');
const CONFIG_FILE = path.join(__dirname, 'config.json');

// Ensure directories
if (!fs.existsSync(SEARCHES_DIR)) fs.mkdirSync(SEARCHES_DIR, { recursive: true });

// ─── State ──────────────────────────────────────────────────────────────
let searchTerms = [];                     // list of term strings
let clients = new Map();                 // clientId -> { ws, assigned: [], dedicatedTerm: null }
let assignments = new Map();             // term -> Set of clientIds
let config = {};                         // term -> { workers: number, dedicated: [clientId] }

// Load persisted data
if (fs.existsSync(TERMS_FILE)) {
  try { searchTerms = JSON.parse(fs.readFileSync(TERMS_FILE, 'utf8')); } catch (_) { searchTerms = []; }
}
if (fs.existsSync(CONFIG_FILE)) {
  try { config = JSON.parse(fs.readFileSync(CONFIG_FILE, 'utf8')); } catch (_) { config = {}; }
}

// Initialize config for terms that don't have one
function ensureConfig(term) {
  if (!config[term]) {
    config[term] = { workers: 1, dedicated: [] };
    saveConfig();
  }
}

function saveTerms() {
  fs.writeFileSync(TERMS_FILE, JSON.stringify(searchTerms, null, 2));
}

function saveConfig() {
  fs.writeFileSync(CONFIG_FILE, JSON.stringify(config, null, 2));
}

// ─── File helpers for search history ──────────────────────────────────
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

  return { added: newListings.length, newListings };
}

// ─── Assignment logic ──────────────────────────────────────────────────
function rebalance() {
  // Clear current assignments (except dedicated ones we'll reassign)
  assignments.clear();
  for (let [id, info] of clients) {
    info.assigned = [];
  }

  // 1. Handle dedicated workers: assign them to their dedicated term
  const dedicatedMap = new Map(); // term -> Set of clientIds
  for (let [id, info] of clients) {
    if (info.dedicatedTerm && searchTerms.includes(info.dedicatedTerm)) {
      if (!dedicatedMap.has(info.dedicatedTerm)) dedicatedMap.set(info.dedicatedTerm, new Set());
      dedicatedMap.get(info.dedicatedTerm).add(id);
    }
  }
  for (let [term, clientSet] of dedicatedMap) {
    // Ensure config exists
    ensureConfig(term);
    // Assign all dedicated workers to this term
    for (let id of clientSet) {
      const info = clients.get(id);
      if (info) {
        info.assigned.push(term);
        if (!assignments.has(term)) assignments.set(term, new Set());
        assignments.get(term).add(id);
      }
    }
  }

  // 2. For each term, compute how many additional workers are needed
  const needs = [];
  for (const term of searchTerms) {
    ensureConfig(term);
    const desired = config[term].workers || 1;
    const current = assignments.has(term) ? assignments.get(term).size : 0;
    const needed = Math.max(0, desired - current);
    if (needed > 0) {
      needs.push({ term, needed });
    }
  }

  // 3. Get list of idle workers (not assigned to any term)
  const idleWorkers = [];
  for (let [id, info] of clients) {
    if (info.assigned.length === 0 && !info.dedicatedTerm) {
      idleWorkers.push(id);
    }
  }

  // 4. Assign idle workers to terms that need them (round‑robin or fill highest need)
  // Sort by needed descending
  needs.sort((a, b) => b.needed - a.needed);
  let workerIdx = 0;
  for (const { term, needed } of needs) {
    for (let i = 0; i < needed && workerIdx < idleWorkers.length; i++) {
      const clientId = idleWorkers[workerIdx++];
      const info = clients.get(clientId);
      if (info) {
        info.assigned.push(term);
        if (!assignments.has(term)) assignments.set(term, new Set());
        assignments.get(term).add(clientId);
      }
    }
  }

  // 5. Notify each client of their assigned terms
  for (let [id, info] of clients) {
    if (info.ws.readyState === WebSocket.OPEN) {
      info.ws.send(JSON.stringify({
        type: 'assign',
        terms: info.assigned
      }));
    }
  }

  // 6. Log current state
  console.log(`📊 Rebalance complete: ${clients.size} clients, ${searchTerms.length} terms`);
  for (const [term, set] of assignments) {
    console.log(`   ${term} -> ${set.size} worker(s)`);
  }
}

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ─── Embedded Frontend ─────────────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Vinted Scraper Central</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; margin: 20px; background: #f5f5f5; }
    .container { max-width: 1200px; margin: 0 auto; }
    h1 { color: #333; }
    .card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .flex { display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }
    input { padding: 8px 12px; border: 1px solid #ddd; border-radius: 4px; flex: 1; min-width: 150px; }
    button { padding: 8px 16px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; }
    button:hover { background: #0056b3; }
    button.danger { background: #dc3545; }
    button.danger:hover { background: #c82333; }
    button.success { background: #28a745; }
    button.success:hover { background: #218838; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #eee; }
    th { background: #f8f9fa; }
    .badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 12px; font-weight: bold; }
    .badge-assigned { background: #17a2b8; color: white; }
    .badge-idle { background: #6c757d; color: white; }
    .badge-dedicated { background: #ffc107; color: #333; }
    .log { background: #1e1e1e; color: #d4d4d4; padding: 10px; border-radius: 4px; font-family: monospace; max-height: 300px; overflow-y: auto; }
    .log .timestamp { color: #6a9955; }
    .log .info { color: #569cd6; }
    .log .new { color: #ce9178; }
    .log .warn { color: #dcdcaa; }
    .inline-edit { display: flex; gap: 5px; flex-wrap: wrap; }
    .inline-edit input { width: 60px; }
  </style>
</head>
<body>
<div class="container">
  <h1>🛍️ Vinted Scraper Central</h1>

  <div class="card">
    <h2>➕ Add Search Term</h2>
    <div class="flex">
      <input type="text" id="newTerm" placeholder="e.g., cortiez hoodie" />
      <input type="number" id="newWorkers" value="1" min="1" style="width:80px;" />
      <button id="addBtn">Add</button>
    </div>
  </div>

  <div class="card">
    <h2>📋 Search Terms & Workers</h2>
    <table id="termTable">
      <thead>
        <tr><th>Term</th><th>Workers (desired / assigned)</th><th>Dedicated Workers</th><th>Actions</th></tr>
      </thead>
      <tbody id="termBody"></tbody>
    </table>
  </div>

  <div class="card">
    <h2>🔗 Connected Clients</h2>
    <div id="clientsList">Loading...</div>
  </div>

  <div class="card">
    <h2>📜 Activity Log</h2>
    <div class="log" id="logContainer">
      <div class="info">Waiting for events...</div>
    </div>
  </div>
</div>

<script>
  const API_BASE = window.location.origin;
  let logCount = 0;

  async function fetchData() {
    try {
      const [termsRes, healthRes] = await Promise.all([
        fetch(\`\${API_BASE}/terms\`),
        fetch(\`\${API_BASE}/health\`)
      ]);
      const terms = await termsRes.json();
      const health = await healthRes.json();
      renderTerms(terms);
      document.getElementById('clientsList').textContent = \`\${health.clients} connected client(s)\`;
    } catch (err) {
      console.error('Fetch error:', err);
    }
  }

  function renderTerms(terms) {
    const tbody = document.getElementById('termBody');
    tbody.innerHTML = '';
    terms.forEach(t => {
      const tr = document.createElement('tr');
      const dedicatedList = t.dedicated && t.dedicated.length ? t.dedicated.join(', ') : 'None';
      tr.innerHTML = \`
        <td><strong>\${t.term}</strong></td>
        <td>\${t.workers} desired / \${t.assignedCount} assigned</td>
        <td>\${dedicatedList}</td>
        <td>
          <button class="danger" data-term="\${t.term}">Remove</button>
          <button class="success" data-term="\${t.term}" data-action="edit">Edit</button>
        </td>
      \`;
      tbody.appendChild(tr);
    });

    // Remove handlers
    document.querySelectorAll('[data-term]').forEach(btn => {
      btn.addEventListener('click', async () => {
        const term = btn.dataset.term;
        if (btn.textContent.trim() === 'Remove') {
          if (!confirm(\`Remove "\${term}"?\`)) return;
          await fetch(\`\${API_BASE}/terms/\${encodeURIComponent(term)}\`, { method: 'DELETE' });
          addLog(\`🗑️ Removed term: "\${term}"\`, 'info');
          fetchData();
        } else if (btn.textContent.trim() === 'Edit') {
          // Show inline edit form
          const row = btn.closest('tr');
          const td = row.querySelector('td:first-child');
          const currentTerm = td.textContent.trim();
          const currentConfig = await fetch(\`\${API_BASE}/config?term=\${encodeURIComponent(currentTerm)}\`).then(r => r.json());
          const workers = currentConfig.workers || 1;
          const dedicated = (currentConfig.dedicated || []).join(',');
          const html = \`
            <div class="inline-edit">
              <input type="number" id="editWorkers-\${currentTerm}" value="\${workers}" min="1" style="width:60px;" />
              <input type="text" id="editDedicated-\${currentTerm}" placeholder="worker1,worker2" value="\${dedicated}" style="flex:1;" />
              <button class="success" data-term="\${currentTerm}" data-action="save-edit">Save</button>
              <button data-term="\${currentTerm}" data-action="cancel-edit">Cancel</button>
            </div>
          \`;
          td.innerHTML = html;
          // Attach save/cancel
          td.querySelector('[data-action="save-edit"]').addEventListener('click', async () => {
            const workersVal = document.getElementById(\`editWorkers-\${currentTerm}\`).value;
            const dedicatedVal = document.getElementById(\`editDedicated-\${currentTerm}\`).value;
            const dedicatedArr = dedicatedVal.split(',').map(s => s.trim()).filter(Boolean);
            await fetch(\`\${API_BASE}/config\`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ term: currentTerm, workers: parseInt(workersVal) || 1, dedicated: dedicatedArr })
            });
            addLog(\`⚙️ Updated config for "\${currentTerm}"\`, 'info');
            fetchData();
          });
          td.querySelector('[data-action="cancel-edit"]').addEventListener('click', () => {
            fetchData(); // reload
          });
        }
      });
    });
  }

  function addLog(message, type = 'info') {
    const container = document.getElementById('logContainer');
    const time = new Date().toLocaleTimeString();
    const div = document.createElement('div');
    div.innerHTML = \`<span class="timestamp">[\${time}]</span> <span class="\${type}">\${message}</span>\`;
    container.prepend(div);
    if (container.children.length > 50) container.removeChild(container.lastChild);
  }

  document.getElementById('addBtn').addEventListener('click', async () => {
    const termInput = document.getElementById('newTerm');
    const workersInput = document.getElementById('newWorkers');
    const term = termInput.value.trim();
    const workers = parseInt(workersInput.value) || 1;
    if (!term) return;
    try {
      const res = await fetch(\`\${API_BASE}/terms\`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ term, workers })
      });
      if (res.ok) {
        addLog(\`➕ Added term: "\${term}" with \${workers} workers\`, 'new');
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

  // Also listen for WebSocket updates? We'll keep simple polling.
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

// ─── API endpoints ─────────────────────────────────────────────────────

// GET /terms - list terms with assignment info
app.get('/terms', (req, res) => {
  const list = searchTerms.map(term => {
    ensureConfig(term);
    const assigned = assignments.has(term) ? assignments.get(term).size : 0;
    return {
      term,
      workers: config[term].workers || 1,
      assignedCount: assigned,
      dedicated: config[term].dedicated || []
    };
  });
  res.json(list);
});

// POST /terms - add a new term with optional workers count
app.post('/terms', (req, res) => {
  const { term, workers = 1 } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  if (searchTerms.includes(term)) {
    return res.status(409).json({ error: 'Term already exists' });
  }
  searchTerms.push(term);
  saveTerms();
  config[term] = { workers: parseInt(workers) || 1, dedicated: [] };
  saveConfig();
  rebalance();
  res.json({ success: true, term });
});

// DELETE /terms/:term - remove term
app.delete('/terms/:term', (req, res) => {
  const term = req.params.term;
  const idx = searchTerms.indexOf(term);
  if (idx === -1) return res.status(404).json({ error: 'Term not found' });
  searchTerms.splice(idx, 1);
  saveTerms();
  delete config[term];
  saveConfig();
  // Remove from assignments
  assignments.delete(term);
  // Clear from clients' assigned lists
  for (let [id, info] of clients) {
    info.assigned = info.assigned.filter(t => t !== term);
  }
  rebalance();
  res.json({ success: true });
});

// GET /config?term=... - get config for a specific term
app.get('/config', (req, res) => {
  const term = req.query.term;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  ensureConfig(term);
  res.json(config[term]);
});

// POST /config - update config for a term
app.post('/config', (req, res) => {
  const { term, workers, dedicated } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  if (!searchTerms.includes(term)) return res.status(404).json({ error: 'Term not found' });
  ensureConfig(term);
  if (workers !== undefined) config[term].workers = parseInt(workers) || 1;
  if (dedicated !== undefined) config[term].dedicated = Array.isArray(dedicated) ? dedicated : [];
  saveConfig();
  rebalance(); // reassign based on new config
  res.json({ success: true, config: config[term] });
});

// GET /searches?term=...&type=... - get search history
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
  res.json({ term, type: type || 'all', count: listings.length, listings });
});

// GET /health - server status
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    terms: searchTerms.length,
    clients: clients.size,
    assignments: Object.fromEntries(
      Array.from(assignments.entries()).map(([k, v]) => [k, v.size])
    )
  });
});

// ─── WebSocket server ──────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  let clientId = null;
  let dedicatedTerm = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      switch (data.type) {
        case 'register': {
          clientId = data.clientId || `client-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
          dedicatedTerm = data.dedicatedTerm || null; // optional
          // Remove existing client with same ID if any
          if (clients.has(clientId)) {
            const old = clients.get(clientId);
            if (old.ws !== ws && old.ws.readyState === WebSocket.OPEN) {
              old.ws.close();
            }
          }
          clients.set(clientId, { ws, assigned: [], dedicatedTerm });
          console.log(`✅ Client registered: ${clientId}${dedicatedTerm ? ` (dedicated to "${dedicatedTerm}")` : ''}`);
          rebalance(); // reassign
          break;
        }
        case 'scraped': {
          const { term, listings } = data;
          if (!term || !listings) break;
          // Verify this client is assigned to this term
          const info = clients.get(clientId);
          if (!info || !info.assigned.includes(term)) {
            ws.send(JSON.stringify({ type: 'error', message: 'Not assigned this term' }));
            break;
          }
          const result = processScrapedListings(term, listings);
          ws.send(JSON.stringify({
            type: 'scraped-ack',
            term,
            added: result.added,
            newListings: result.newListings
          }));
          break;
        }
        default:
          console.log(`Unknown message type: ${data.type}`);
      }
    } catch (err) {
      console.error('WebSocket message error:', err);
    }
  });

  ws.on('close', () => {
    if (clientId) {
      clients.delete(clientId);
      console.log(`❌ Client disconnected: ${clientId} (${clients.size} remaining)`);
      // Remove from assignments
      for (let [term, set] of assignments) {
        set.delete(clientId);
      }
      rebalance();
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

// ─── Start server ──────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Server running on port ${PORT}`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}`);
  // Initial rebalance
  setTimeout(rebalance, 1000);
});
