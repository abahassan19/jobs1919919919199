const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');
const { createClient } = require('@supabase/supabase-js');

// ─── Supabase config ──────────────────────────────────────────────────
const SUPABASE_URL = 'https://wxtkdejncsumglqpytzy.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind4dGtkZWpuY3N1bWdscXB5dHp5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3ODU1MDEyODQsImV4cCI6MjEwMTA3NzI4NH0.gM-hyPEZvwby43_5efR5W2WFDz8xb76nFGEHo9o7Q38';
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ─── Server config ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const BASE_DIR = process.env.RENDER_PERSISTENT_DISK || __dirname;
const DATA_DIR = path.join(BASE_DIR, 'userdata');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

console.log(`Data directory: ${DATA_DIR}`);
console.log(`Persistent storage: ${process.env.RENDER_PERSISTENT_DISK ? 'enabled' : 'disabled'}`);

// ─── Constants ──────────────────────────────────────────────────────
const MASTER_ID = 'mastermaster1234';
const MAX_TERMS = 30;
const MAX_ATTEMPTS = 5;
const LOCKOUT_MINUTES = 15;

// ─── In‑memory state ──────────────────────────────────────────────────
const sessions = new Map();           // userId -> { lastActive, membership }
const userCache = new Map();          // userId -> { terms, bargains }
const loginAttempts = new Map();      // ip -> { attempts, firstAttempt, blockedUntil }

// ─── User data helpers ──────────────────────────────────────────────
function getUserDir(userId) {
  return path.join(DATA_DIR, userId);
}
function getTermsFile(userId) {
  return path.join(getUserDir(userId), 'terms.json');
}
function getResultsFile(userId, term) {
  const safe = term.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '');
  return path.join(getUserDir(userId), `${safe}-results.json`);
}
function getUniqueFile(userId, term) {
  const safe = term.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_-]/g, '');
  return path.join(getUserDir(userId), `${safe}-unique.json`);
}
function ensureUserDir(userId) {
  const dir = getUserDir(userId);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
function loadUserTerms(userId) {
  const file = getTermsFile(userId);
  try {
    if (fs.existsSync(file)) {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      return data.terms || [];
    }
  } catch (_) {}
  return [];
}
function saveUserTerms(userId, terms) {
  ensureUserDir(userId);
  const file = getTermsFile(userId);
  fs.writeFileSync(file, JSON.stringify({ terms }, null, 2), 'utf8');
}
function loadUserHistory(userId, term) {
  const file = getResultsFile(userId, term);
  try {
    if (fs.existsSync(file)) {
      const data = JSON.parse(fs.readFileSync(file, 'utf8'));
      return data.listings || [];
    }
  } catch (_) {}
  return [];
}
function saveUserHistory(userId, term, listings) {
  ensureUserDir(userId);
  const file = getResultsFile(userId, term);
  const data = { searchTerm: term, lastUpdated: new Date().toISOString(), totalListings: listings.length, listings };
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}
function saveUserUnique(userId, term, uniqueListings) {
  ensureUserDir(userId);
  const file = getUniqueFile(userId, term);
  const data = { searchTerm: term, lastUpdated: new Date().toISOString(), totalUniqueListings: uniqueListings.length, listings: uniqueListings };
  fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8');
}
function getUserData(userId) {
  if (!userCache.has(userId)) {
    userCache.set(userId, {
      terms: loadUserTerms(userId),
      bargains: {}
    });
  }
  return userCache.get(userId);
}

// ─── Price analysis ──────────────────────────────────────────────────
function analyzePrices(listings, averagePrice, thresholdPercent) {
  if (!listings || listings.length === 0 || !averagePrice) return { bargains: [], stats: null };
  const threshold = thresholdPercent || 20;
  const bargains = listings.filter(l => {
    const price = parseFloat(l.price.replace(/[^0-9.]/g, ''));
    if (isNaN(price)) return false;
    const discount = ((averagePrice - price) / averagePrice) * 100;
    return discount >= threshold;
  }).map(l => {
    const price = parseFloat(l.price.replace(/[^0-9.]/g, ''));
    const discount = ((averagePrice - price) / averagePrice) * 100;
    return { ...l, discount: Math.round(discount * 100) / 100 };
  });
  return {
    bargains: bargains.sort((a, b) => b.discount - a.discount),
    stats: { average: averagePrice, threshold, totalListings: listings.length, bargainCount: bargains.length }
  };
}

function processScrapedListings(userId, term, scraped, jobType) {
  const userData = getUserData(userId);
  const existing = loadUserHistory(userId, term);
  const existingLinks = new Set(existing.map(item => item.link));

  if (jobType === 'scan_all') {
    const prices = scraped
      .map(l => parseFloat(l.price.replace(/[^0-9.]/g, '')))
      .filter(p => !isNaN(p) && p > 0);
    if (prices.length > 0) {
      const avg = prices.reduce((a, b) => a + b, 0) / prices.length;
      const termObj = userData.terms.find(t => t.term === term);
      if (termObj) {
        termObj.averagePrice = Math.round(avg * 100) / 100;
        saveUserTerms(userId, userData.terms);
        console.log(`User ${userId} average for "${term}" set to £${termObj.averagePrice}`);
      }
    }
    return { added: 0, newListings: [], averageComputed: true };
  }

  // scan_new
  const newListings = scraped.filter(item => !existingLinks.has(item.link));
  if (newListings.length === 0) return { added: 0, newListings: [] };

  const now = new Date().toISOString();
  newListings.forEach(item => item.firstSeen = now);

  const updated = [...newListings, ...existing];
  saveUserHistory(userId, term, updated);
  saveUserUnique(userId, term, newListings);

  const termConfig = userData.terms.find(t => t.term === term);
  if (termConfig && termConfig.averagePrice) {
    const analysis = analyzePrices(updated, termConfig.averagePrice, termConfig.thresholdPercent);
    if (analysis.bargains.length > 0) {
      userData.bargains[term] = analysis.bargains;
      broadcastBargains(userId, term, analysis.bargains);
    }
  }
  return { added: newListings.length, newListings };
}

// ─── Rate limiting ──────────────────────────────────────────────────
function checkRateLimit(ip) {
  const now = Date.now();
  const record = loginAttempts.get(ip);
  if (!record) {
    loginAttempts.set(ip, { attempts: 1, firstAttempt: now });
    return true;
  }
  if (record.blockedUntil && now < record.blockedUntil) return false;
  if (record.blockedUntil && now >= record.blockedUntil) {
    loginAttempts.delete(ip);
    return true;
  }
  if (record.attempts >= MAX_ATTEMPTS) {
    record.blockedUntil = now + LOCKOUT_MINUTES * 60 * 1000;
    loginAttempts.set(ip, record);
    return false;
  }
  record.attempts++;
  loginAttempts.set(ip, record);
  return true;
}

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// ─── Frontend HTML (login + dashboard) ──────────────────────────────
const HTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Vinted Price Monitor</title>
<style>
*{box-sizing:border-box}body{font-family:system-ui,sans-serif;margin:0;background:#f5f6fa}.container{max-width:1400px;margin:0 auto;padding:20px}.login-container{max-width:400px;margin:100px auto;background:#fff;border-radius:8px;padding:30px;box-shadow:0 2px 10px rgba(0,0,0,0.1)}.login-container h2{margin-top:0}.login-container input{width:100%;padding:12px;margin:10px 0;border:1px solid #ddd;border-radius:4px}.login-container button{width:100%;padding:12px;background:#3498db;color:#fff;border:none;border-radius:4px;cursor:pointer}.login-container .error{color:#e74c3c;font-size:14px;margin-top:5px}.hidden{display:none}h1{font-weight:400;color:#2c3e50}.card{background:#fff;border-radius:8px;padding:20px;margin-bottom:20px;box-shadow:0 2px 4px rgba(0,0,0,0.1)}.flex{display:flex;gap:12px;flex-wrap:wrap;align-items:center}.flex label{font-weight:500;min-width:80px}input,select{padding:8px 12px;border:1px solid #ddd;border-radius:4px;font-size:14px;background:#fff}input{flex:1;min-width:160px}button{padding:8px 16px;background:#3498db;color:#fff;border:none;border-radius:4px;cursor:pointer;font-weight:500}button:hover{background:#2980b9}button.secondary{background:#95a5a6}button.secondary:hover{background:#7f8c8d}button.danger{background:#e74c3c}button.danger:hover{background:#c0392b}button.success{background:#2ecc71}button.success:hover{background:#27ae60}table{width:100%;border-collapse:collapse;margin-top:10px}th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #ecf0f1}th{background:#f8f9fa;font-weight:600;color:#2c3e50}.badge{display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600}.badge-active{background:#2ecc71;color:#fff}.badge-idle{background:#bdc3c7;color:#2c3e50}.badge-bargain{background:#e74c3c;color:#fff}.tabs{display:flex;gap:8px;margin-bottom:20px;border-bottom:2px solid #ddd}.tab{padding:10px 16px;cursor:pointer;border:none;background:none;font-weight:500;color:#7f8c8d}.tab.active{color:#3498db;border-bottom:2px solid #3498db}.tab-content{display:none}.tab-content.active{display:block}.log{background:#2c3e50;color:#ecf0f1;padding:10px;border-radius:4px;font-family:monospace;max-height:200px;overflow-y:auto;font-size:12px}.log .timestamp{color:#7f8c8d}.log .info{color:#3498db}.log .success{color:#2ecc71}.log .warning{color:#f1c40f}.log .bargain{color:#e74c3c;font-weight:700}.bargain-item{background:#fef9e7;border-left:4px solid #e74c3c;padding:10px;margin:5px 0;border-radius:4px}.bargain-item strong{display:block;margin-bottom:4px}.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:10px;margin:10px 0}.stat-box{background:#f8f9fa;padding:10px;border-radius:4px;text-align:center}.stat-box .value{font-size:20px;font-weight:600;color:#2c3e50}.stat-box .label{font-size:12px;color:#7f8c8d}.empty{color:#95a5a6;text-align:center;padding:20px}.help-text{font-size:12px;color:#95a5a6;margin-top:4px}.inline-actions{display:flex;gap:6px;flex-wrap:wrap}.header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap}.logout-btn{background:#e74c3c;color:#fff;padding:6px 12px;border:none;border-radius:4px;cursor:pointer}.master-actions{display:flex;gap:8px;margin:10px 0}.master-actions button{background:#f39c12;color:#fff}
</style>
</head>
<body>

<!-- Login Screen -->
<div id="loginScreen" class="login-container">
<h2>Login</h2>
<p>Enter your unique ID (from the keys table)</p>
<input type="text" id="loginId" placeholder="Your ID" autocomplete="username" />
<div id="loginError" class="error"></div>
<button id="loginBtn">Login</button>
</div>

<!-- Dashboard (hidden until login) -->
<div id="dashboard" class="container hidden">
<div class="header">
<h1>Vinted Price Monitor</h1>
<div><span id="userDisplay"></span> <button class="logout-btn" id="logoutBtn">Logout</button></div>
</div>
<!-- Master export/import buttons (hidden by default) -->
<div id="masterActions" class="master-actions" style="display:none;">
<button id="exportBtn">Export All Data</button>
<button id="importBtn">Import Data</button>
<input type="file" id="importFile" style="display:none;" accept=".json" />
</div>
<div class="card">
<h3>Add Search Term</h3>
<div class="flex">
<div style="flex:1;min-width:200px"><label>Term</label><input type="text" id="newTerm" placeholder="e.g., cortiez hoodie" /></div>
<div style="width:120px"><label>Deal %</label><input type="number" id="threshold" value="20" step="1" min="0" /><div class="help-text">% below average to trigger bargain</div></div>
<div style="width:120px"><label>Interval</label><select id="interval"><option value="5">5 min</option><option value="10">10 min</option><option value="15">15 min</option><option value="20">20 min</option><option value="30">30 min</option><option value="45">45 min</option><option value="60">60 min</option></select><div class="help-text">How often to scan for new items</div></div>
<button id="addBtn">Add Term</button>
</div>
<div id="termLimitWarning" style="color:#e67e22;margin-top:5px;display:none;">You have reached the maximum of 30 terms. Remove some to add more.</div>
</div>
<div class="tabs">
<button class="tab active" data-tab="terms">Terms</button>
<button class="tab" data-tab="bargains">Bargains <span id="bargainCount" class="badge" style="background:#e74c3c;color:#fff;padding:0 8px;">0</span></button>
<button class="tab" data-tab="workers">Workers</button>
<button class="tab" data-tab="log">Log</button>
</div>
<div id="tab-terms" class="tab-content active"><div class="card"><h3>Search Terms</h3><div id="termContainer"></div></div></div>
<div id="tab-bargains" class="tab-content"><div class="card"><h3>Bargain Alerts</h3><div id="bargainContainer"><div class="empty">No bargains yet.</div></div></div></div>
<div id="tab-workers" class="tab-content"><div class="card"><h3>Connected Workers</h3><div id="workersContainer"><div class="empty">No workers connected.</div></div></div></div>
<div id="tab-log" class="tab-content"><div class="card"><h3>Activity Log</h3><div class="log" id="logContainer"><div class="info">System ready.</div></div></div></div>
</div>

<script>
// ─── Global state ──────────────────────────────────────────────────
let userId = null;
let bargains = {};
const API_BASE = window.location.origin;

// ─── DOM refs ──────────────────────────────────────────────────────
const loginScreen = document.getElementById('loginScreen');
const dashboard = document.getElementById('dashboard');
const loginIdInput = document.getElementById('loginId');
const loginError = document.getElementById('loginError');
const loginBtn = document.getElementById('loginBtn');
const logoutBtn = document.getElementById('logoutBtn');
const userDisplay = document.getElementById('userDisplay');
const termLimitWarning = document.getElementById('termLimitWarning');
const masterActions = document.getElementById('masterActions');
const exportBtn = document.getElementById('exportBtn');
const importBtn = document.getElementById('importBtn');
const importFile = document.getElementById('importFile');

// ─── Helper: fetch with auth header ──────────────────────────────
async function authFetch(url, options = {}) {
  const headers = { ...options.headers, 'X-User-Id': userId };
  return fetch(url, { ...options, headers });
}

// ─── Login ──────────────────────────────────────────────────────────
async function login() {
  const id = loginIdInput.value.trim();
  if (!id) { loginError.textContent = 'Please enter an ID.'; return; }
  loginError.textContent = '';
  try {
    const res = await fetch(API_BASE + '/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ id })
    });
    const data = await res.json();
    if (res.ok) {
      userId = data.userId;
      // Save to localStorage for autofill
      localStorage.setItem('vinted_userId', userId);
      showDashboard();
    } else {
      loginError.textContent = data.error || 'Login failed. Please try again.';
    }
  } catch (err) {
    loginError.textContent = 'Network error. Please try again.';
  }
}

// ─── Logout ──────────────────────────────────────────────────────────
function logout() {
  userId = null;
  localStorage.removeItem('vinted_userId');
  loginScreen.classList.remove('hidden');
  dashboard.classList.add('hidden');
  loginIdInput.value = '';
  loginError.textContent = '';
  if (window.pollInterval) clearInterval(window.pollInterval);
  if (ws) ws.close();
}

// ─── Show dashboard ──────────────────────────────────────────────────
function showDashboard() {
  loginScreen.classList.add('hidden');
  dashboard.classList.remove('hidden');
  userDisplay.textContent = 'User: ' + userId;
  // Show master actions if master
  if (userId === 'mastermaster1234') {
    masterActions.style.display = 'flex';
  } else {
    masterActions.style.display = 'none';
  }
  // Start WebSocket
  initWebSocket();
  fetchData();
  if (window.pollInterval) clearInterval(window.pollInterval);
  window.pollInterval = setInterval(fetchData, 3000);
}

// ─── WebSocket ──────────────────────────────────────────────────────
let ws = null;
function initWebSocket() {
  if (ws) ws.close();
  ws = new WebSocket('ws://' + window.location.host);
  ws.onopen = () => {
    ws.send(JSON.stringify({ type: 'register-frontend', userId: userId }));
  };
  ws.onmessage = (e) => {
    const d = JSON.parse(e.data);
    if (d.type === 'bargain-alert' && d.userId === userId) {
      addLog('Bargain found for "' + d.term + '": ' + d.bargains.length + ' items', 'bargain');
      fetchData();
    } else if (d.type === 'update') {
      renderAll(d);
    }
  };
  ws.onclose = () => {
    setTimeout(initWebSocket, 5000);
  };
}

// ─── Render functions ──────────────────────────────────────────────
function renderAll(data) {
  renderTerms(data.terms);
  renderWorkers(data.clients, data.active);
  if (data.bargains) {
    bargains = data.bargains;
    renderBargains(data.bargains);
    const total = Object.values(data.bargains).reduce((s, a) => s + a.length, 0);
    document.getElementById('bargainCount').textContent = total;
  }
  if (data.terms && data.terms.length >= 30) {
    termLimitWarning.style.display = 'block';
  } else {
    termLimitWarning.style.display = 'none';
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
    let scanBtn = '';
    if (t.averagePrice) {
      if (t.scanning) {
        scanBtn = '<button class="danger" data-action="stop" data-term="' + t.term + '">Stop Scanning</button>';
      } else {
        scanBtn = '<button class="success" data-action="scan" data-term="' + t.term + '">Start Scanning</button>';
      }
    } else {
      scanBtn = '<button class="secondary" data-action="avg" data-term="' + t.term + '">Calc Average</button>';
    }
    html += '<tr><td><strong>' + t.term + '</strong></td><td>' + avgDisplay + '</td><td>' + t.thresholdPercent + '%</td><td>' + t.interval + ' min</td><td>' + status + '</td><td>' + (t.listingCount || 0) + '</td><td>' + (t.bargainCount || 0) + '</td><td class="inline-actions">' + scanBtn + '<button class="danger" data-action="remove" data-term="' + t.term + '">Remove</button></td></tr>';
  });
  html += '</tbody></table>';
  c.innerHTML = html;

  // Attach event listeners
  c.querySelectorAll('[data-action="remove"]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const term = btn.dataset.term;
      if (!confirm('Remove "' + term + '"?')) return;
      const res = await authFetch(API_BASE + '/terms/' + encodeURIComponent(term), { method: 'DELETE' });
      if (res.ok) {
        addLog('Removed term: "' + term + '"', 'warning');
        fetchData();
      } else {
        alert('Failed to remove term.');
      }
    });
  });
  c.querySelectorAll('[data-action="avg"]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const term = btn.dataset.term;
      const res = await authFetch(API_BASE + '/calculate-average', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ term })
      });
      const data = await res.json();
      if (res.ok) {
        addLog('Average calculation started for "' + term + '"', 'info');
        fetchData();
      } else {
        alert(data.error || 'Failed to start calculation');
      }
    });
  });
  c.querySelectorAll('[data-action="scan"]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const term = btn.dataset.term;
      const res = await authFetch(API_BASE + '/start-scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ term })
      });
      const data = await res.json();
      if (res.ok) {
        addLog('Scanning started for "' + term + '"', 'success');
        fetchData();
      } else {
        alert(data.error || 'Failed to start scanning');
      }
    });
  });
  c.querySelectorAll('[data-action="stop"]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const term = btn.dataset.term;
      const res = await authFetch(API_BASE + '/stop-scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ term })
      });
      const data = await res.json();
      if (res.ok) {
        addLog('Scanning stopped for "' + term + '"', 'warning');
        fetchData();
      } else {
        alert(data.error || 'Failed to stop scanning');
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
  let html = '<div class="stats-grid"><div class="stat-box"><div class="value">' + clientCount + '</div><div class="label">Connected Workers</div></div><div class="stat-box"><div class="value">' + (activeJobs ? activeJobs.length : 0) + '</div><div class="label">Active Jobs</div></div></div>';
  if (activeJobs && activeJobs.length > 0) {
    html += '<table><thead><tr><th>Term</th><th>Type</th></tr></thead><tbody>';
    activeJobs.forEach(job => {
      html += '<tr><td>' + job.term + '</td><td>' + job.type + '</td></tr>';
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
        html += '<div class="bargain-item"><strong>' + item.name + '</strong><div style="display:flex;gap:15px;flex-wrap:wrap;font-size:14px;"><span>Price: ' + item.price + '</span><span>Discount: ' + item.discount + '%</span><span>Size: ' + (item.size || 'N/A') + '</span><span>Condition: ' + (item.condition || 'N/A') + '</span><a href="' + item.link + '" target="_blank">View</a></div></div>';
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
  if (!userId) return;
  try {
    const res = await authFetch(API_BASE + '/status');
    if (!res.ok) {
      if (res.status === 401) {
        // Unauthorized – logout
        logout();
        loginError.textContent = 'Session expired. Please login again.';
      }
      return;
    }
    const data = await res.json();
    renderAll(data);
  } catch (e) { console.error(e); }
}

// ─── Add term ──────────────────────────────────────────────────────
document.getElementById('addBtn').addEventListener('click', async () => {
  const termInput = document.getElementById('newTerm');
  const thresholdInput = document.getElementById('threshold');
  const intervalSelect = document.getElementById('interval');
  const term = termInput.value.trim();
  const threshold = parseInt(thresholdInput.value) || 20;
  const interval = parseInt(intervalSelect.value) || 5;
  if (!term) return;
  try {
    const res = await authFetch(API_BASE + '/terms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ term, thresholdPercent: threshold, interval })
    });
    const data = await res.json();
    if (res.ok) {
      addLog('Added term: "' + term + '"', 'success');
      termInput.value = '';
      fetchData();
    } else {
      alert(data.error || 'Failed to add term');
    }
  } catch (err) {
    alert('Network error');
  }
});

// ─── Master export/import ──────────────────────────────────────────
exportBtn.addEventListener('click', async () => {
  if (userId !== 'mastermaster1234') return;
  const res = await authFetch(API_BASE + '/master/export');
  const data = await res.json();
  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = 'vinted_export_' + new Date().toISOString() + '.json';
  a.click();
  URL.revokeObjectURL(url);
  addLog('Export completed', 'success');
});

importBtn.addEventListener('click', () => {
  importFile.click();
});
importFile.addEventListener('change', async (e) => {
  const file = e.target.files[0];
  if (!file) return;
  const text = await file.text();
  try {
    const data = JSON.parse(text);
    const res = await authFetch(API_BASE + '/master/import', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    });
    const result = await res.json();
    if (res.ok) {
      addLog('Import completed: ' + result.imported + ' users restored', 'success');
      fetchData();
    } else {
      alert(result.error || 'Import failed');
    }
  } catch (err) {
    alert('Invalid JSON file');
  }
  importFile.value = '';
});

// ─── Login button ──────────────────────────────────────────────────
loginBtn.addEventListener('click', login);
loginIdInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') login(); });
logoutBtn.addEventListener('click', logout);

// ─── Auto-login if userId in localStorage ─────────────────────────
const savedId = localStorage.getItem('vinted_userId');
if (savedId) {
  loginIdInput.value = savedId;
  login();
}

// ─── Tabs ──────────────────────────────────────────────────────────
document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
  });
});

// ─── Initial log ──────────────────────────────────────────────────
addLog('Dashboard ready. Please log in.', 'info');
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

// ─── Login endpoint ──────────────────────────────────────────────────
app.post('/login', async (req, res) => {
  const { id } = req.body;
  if (!id) return res.status(400).json({ error: 'Missing ID' });

  const ip = req.ip || req.connection.remoteAddress;
  if (!checkRateLimit(ip)) {
    return res.status(429).json({ error: 'Too many failed attempts. Try again later.' });
  }

  // Special case: master ID
  if (id === MASTER_ID) {
    sessions.set(id, { lastActive: Date.now(), membership: 'master' });
    loginAttempts.delete(ip);
    return res.json({ userId: id, membership: 'master' });
  }

  try {
    const { data, error } = await supabase
      .from('keys')
      .select('id, membership')
      .eq('id', id)
      .maybeSingle();

    if (error || !data) {
      loginAttempts.set(ip, { attempts: (loginAttempts.get(ip)?.attempts || 0) + 1, firstAttempt: Date.now() });
      return res.status(401).json({ error: 'Invalid ID' });
    }

    sessions.set(id, { lastActive: Date.now(), membership: data.membership || null });
    loginAttempts.delete(ip);
    res.json({ userId: id, membership: data.membership });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Middleware: authenticate all /api/* routes ────────────────────
app.use('/api', (req, res, next) => {
  const userId = req.headers['x-user-id'];
  if (!userId || !sessions.has(userId)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  // Update session activity
  sessions.set(userId, { ...sessions.get(userId), lastActive: Date.now() });
  req.userId = userId;
  next();
});

// ─── API endpoints ──────────────────────────────────────────────────

// GET /api/status
app.get('/api/status', (req, res) => {
  const userId = req.userId;
  const userData = getUserData(userId);
  const termsWithStatus = userData.terms.map(t => ({
    ...t,
    active: Array.from(globalActiveJobs.values()).some(j => j.userId === userId && j.term === t.term),
    listingCount: loadUserHistory(userId, t.term).length,
    bargainCount: (userData.bargains[t.term] || []).length,
    scanning: t.scanning || false
  }));
  res.json({
    terms: termsWithStatus,
    clients: globalClients.size,
    active: Array.from(globalActiveJobs.values()).filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    queue: globalJobQueue.filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    bargains: userData.bargains
  });
});

// POST /api/terms
app.post('/api/terms', (req, res) => {
  const userId = req.userId;
  const { term, thresholdPercent = 20, interval = 5 } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  // Check membership
  if (!checkMembership(userId)) {
    return res.status(403).json({ error: 'No active membership. Please upgrade to add terms.' });
  }
  if (userData.terms.length >= MAX_TERMS) {
    return res.status(409).json({ error: 'Maximum 30 terms reached. Remove some to add more.' });
  }
  if (userData.terms.find(t => t.term === term)) {
    return res.status(409).json({ error: 'Term already exists' });
  }
  const obj = { term, averagePrice: null, thresholdPercent: parseInt(thresholdPercent), interval: parseInt(interval), scanning: false };
  userData.terms.push(obj);
  saveUserTerms(userId, userData.terms);
  broadcastUpdate(userId);
  res.json({ success: true, term: obj });
});

// DELETE /api/terms/:term
app.delete('/api/terms/:term', (req, res) => {
  const userId = req.userId;
  const term = req.params.term;
  const userData = getUserData(userId);
  const idx = userData.terms.findIndex(t => t.term === term);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  userData.terms.splice(idx, 1);
  saveUserTerms(userId, userData.terms);
  delete userData.bargains[term];
  broadcastUpdate(userId);
  res.json({ success: true });
});

// POST /api/calculate-average
app.post('/api/calculate-average', (req, res) => {
  const userId = req.userId;
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  if (!checkMembership(userId)) {
    return res.status(403).json({ error: 'No active membership.' });
  }
  // Queue scan_all
  globalJobQueue.push({ userId, term, type: 'scan_all' });
  processQueue();
  broadcastUpdate(userId);
  res.json({ success: true });
});

// POST /api/start-scan
app.post('/api/start-scan', (req, res) => {
  const userId = req.userId;
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  if (!termObj.averagePrice) {
    return res.status(400).json({ error: 'Average price not set. Please calculate average first.' });
  }
  if (!checkMembership(userId)) {
    return res.status(403).json({ error: 'No active membership.' });
  }
  if (termObj.scanning) {
    return res.status(409).json({ error: 'Scanning already active' });
  }
  termObj.scanning = true;
  saveUserTerms(userId, userData.terms);
  globalJobQueue.push({ userId, term, type: 'scan_new' });
  processQueue();
  broadcastUpdate(userId);
  res.json({ success: true });
});

// POST /api/stop-scan
app.post('/api/stop-scan', (req, res) => {
  const userId = req.userId;
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  termObj.scanning = false;
  saveUserTerms(userId, userData.terms);
  // Remove queued scan_new jobs for this user+term
  globalJobQueue = globalJobQueue.filter(j => !(j.userId === userId && j.term === term && j.type === 'scan_new'));
  broadcastUpdate(userId);
  res.json({ success: true });
});

// GET /api/searches
app.get('/api/searches', (req, res) => {
  const userId = req.userId;
  const { term, type } = req.query;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  let listings = [];
  if (type === 'unique') {
    const file = getUniqueFile(userId, term);
    try { if (fs.existsSync(file)) { const data = JSON.parse(fs.readFileSync(file, 'utf8')); listings = data.listings || []; } } catch (_) {}
  } else {
    listings = loadUserHistory(userId, term);
  }
  res.json({ term, type: type || 'all', count: listings.length, listings: listings.slice(0, 100) });
});

// ─── Master export/import ──────────────────────────────────────────
app.get('/api/master/export', (req, res) => {
  if (req.userId !== MASTER_ID) return res.status(403).json({ error: 'Forbidden' });
  const allData = {};
  const userDirs = fs.readdirSync(DATA_DIR).filter(f => fs.statSync(path.join(DATA_DIR, f)).isDirectory());
  for (const uid of userDirs) {
    const terms = loadUserTerms(uid);
    const history = {};
    for (const t of terms) {
      history[t.term] = loadUserHistory(uid, t.term);
    }
    allData[uid] = { terms, history };
  }
  res.json(allData);
});

app.post('/api/master/import', express.json({ limit: '50mb' }), (req, res) => {
  if (req.userId !== MASTER_ID) return res.status(403).json({ error: 'Forbidden' });
  const data = req.body;
  if (!data || typeof data !== 'object') return res.status(400).json({ error: 'Invalid data' });
  let imported = 0;
  for (const [uid, userData] of Object.entries(data)) {
    if (!userData.terms) continue;
    ensureUserDir(uid);
    saveUserTerms(uid, userData.terms);
    if (userData.history) {
      for (const [term, listings] of Object.entries(userData.history)) {
        saveUserHistory(uid, term, listings);
      }
    }
    imported++;
    userCache.delete(uid);
  }
  res.json({ success: true, imported });
});

// ─── Global job queue and workers (shared) ──────────────────────────
let globalJobQueue = [];
let globalActiveJobs = new Map();
let globalClients = new Map();
let globalFrontendClients = new Set();

// ─── WebSocket server ──────────────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, clientTracking: true });

wss.on('connection', (ws, req) => {
  let clientId = null;
  let userId = null;
  ws.isFrontend = false;

  ws.on('message', (msg) => {
    try {
      const data = JSON.parse(msg);
      switch (data.type) {
        case 'register-frontend':
          ws.isFrontend = true;
          userId = data.userId;
          if (userId && sessions.has(userId)) {
            ws.userId = userId;
            globalFrontendClients.add(ws);
            console.log(`Frontend connected for user ${userId}`);
            broadcastUpdate(userId);
          } else {
            ws.close();
          }
          break;
        case 'register-worker':
          clientId = data.clientId || 'worker-' + Date.now();
          if (globalClients.has(clientId)) {
            const old = globalClients.get(clientId);
            if (old.ws !== ws && old.ws.readyState === WebSocket.OPEN) old.ws.close();
          }
          globalClients.set(clientId, { ws, busy: false, lastPing: Date.now() });
          console.log('Worker registered: ' + clientId + ' (' + globalClients.size + ' total)');
          ws.send(JSON.stringify({ type: 'registered', clientId }));
          processQueue();
          break;
        case 'job-complete': {
          const { term, listings, jobId, jobType } = data;
          if (!term || !listings) break;
          let job = null;
          let jobKey = null;
          for (const [key, j] of globalActiveJobs) {
            if (j.jobId === jobId) {
              job = j;
              jobKey = key;
              break;
            }
          }
          if (!job) {
            console.log('Received job-complete for unknown jobId: ' + jobId);
            break;
          }
          const uid = job.userId;
          console.log('Job complete for "' + term + '" (' + (jobType || 'scan_new') + ') from ' + clientId + ' for user ' + uid);
          const result = processScrapedListings(uid, term, listings, jobType || 'scan_new');
          if (clientId && globalClients.has(clientId)) {
            globalClients.get(clientId).busy = false;
          }
          if (jobKey) {
            globalActiveJobs.delete(jobKey);
          }
          ws.send(JSON.stringify({
            type: 'job-complete-ack',
            term,
            added: result.added || 0,
            newListings: result.newListings || []
          }));
          console.log('Job complete for "' + term + '" - added ' + (result.added || 0) + ' new');
          broadcastUpdate(uid);
          processQueue();
          break;
        }
        case 'job-failed': {
          const { term, error, jobId, jobType } = data;
          let job = null;
          let jobKey = null;
          for (const [key, j] of globalActiveJobs) {
            if (j.jobId === jobId) {
              job = j;
              jobKey = key;
              break;
            }
          }
          if (job) {
            const uid = job.userId;
            console.log('Job failed for "' + term + '" (' + (jobType || 'scan_new') + ') from ' + clientId + ' for user ' + uid + ': ' + error);
            if (clientId && globalClients.has(clientId)) {
              globalClients.get(clientId).busy = false;
            }
            if (jobKey) {
              globalActiveJobs.delete(jobKey);
            }
            if ((jobType || 'scan_new') === 'scan_new') {
              globalJobQueue.push({ userId: uid, term, type: 'scan_new' });
            }
            processQueue();
            broadcastUpdate(uid);
          } else {
            console.log('Received job-failed for unknown jobId: ' + jobId);
          }
          break;
        }
        case 'ping': {
          if (clientId && globalClients.has(clientId)) {
            globalClients.get(clientId).lastPing = Date.now();
          }
          ws.send(JSON.stringify({ type: 'pong' }));
          break;
        }
        default:
          console.log('Unknown message type: ' + data.type);
      }
    } catch (err) {
      console.error('WebSocket message error:', err);
    }
  });

  ws.on('close', (code) => {
    if (ws.isFrontend) {
      globalFrontendClients.delete(ws);
      console.log('Frontend disconnected' + (userId ? ' for user ' + userId : ''));
      return;
    }
    if (clientId) {
      let lostJobKey = null;
      for (const [key, job] of globalActiveJobs) {
        if (job.clientId === clientId) {
          lostJobKey = key;
          break;
        }
      }
      globalClients.delete(clientId);
      console.log('Worker disconnected: ' + clientId + ' (' + globalClients.size + ' remaining) code ' + code);
      if (lostJobKey) {
        const job = globalActiveJobs.get(lostJobKey);
        if (job) {
          if (job.type === 'scan_new') {
            globalJobQueue.push({ userId: job.userId, term: job.term, type: 'scan_new' });
          }
          globalActiveJobs.delete(lostJobKey);
          processQueue();
          broadcastUpdate(job.userId);
        }
      }
    }
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err);
  });
});

// ─── Job queue processor ──────────────────────────────────────────────
function processQueue() {
  if (globalJobQueue.length === 0) return;
  let availableClient = null;
  for (let [id, info] of globalClients) {
    if (!info.busy && info.ws.readyState === WebSocket.OPEN) {
      availableClient = id;
      break;
    }
  }
  if (!availableClient) return;
  let jobIndex = -1;
  for (let i = 0; i < globalJobQueue.length; i++) {
    const job = globalJobQueue[i];
    let active = false;
    for (const [key, act] of globalActiveJobs) {
      if (act.userId === job.userId && act.term === job.term) {
        active = true;
        break;
      }
    }
    if (!active) {
      jobIndex = i;
      break;
    }
  }
  if (jobIndex === -1) return;
  const job = globalJobQueue.splice(jobIndex, 1)[0];
  const clientInfo = globalClients.get(availableClient);
  clientInfo.busy = true;
  const jobId = job.userId + '-' + job.term + '-' + job.type + '-' + Date.now();
  const jobKey = jobId;
  globalActiveJobs.set(jobKey, { userId: job.userId, term: job.term, type: job.type, clientId: availableClient, startTime: Date.now(), jobId });
  clientInfo.ws.send(JSON.stringify({
    type: 'job',
    term: job.term,
    jobId: jobId,
    jobType: job.type
  }));
  console.log('Assigned ' + job.type + ' for "' + job.term + '" (user ' + job.userId + ') to ' + availableClient);
}

// ─── Broadcast update to a specific user's frontend ──────────────────
function broadcastUpdate(userId) {
  const userData = getUserData(userId);
  const termsWithStatus = userData.terms.map(t => ({
    ...t,
    active: Array.from(globalActiveJobs.values()).some(j => j.userId === userId && j.term === t.term),
    listingCount: loadUserHistory(userId, t.term).length,
    bargainCount: (userData.bargains[t.term] || []).length
  }));
  const msg = JSON.stringify({
    type: 'update',
    userId: userId,
    terms: termsWithStatus,
    clients: globalClients.size,
    active: Array.from(globalActiveJobs.values()).filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    queue: globalJobQueue.filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    bargains: userData.bargains
  });
  for (const ws of globalFrontendClients) {
    if (ws.userId === userId && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

// ─── Broadcast bargains ──────────────────────────────────────────────
function broadcastBargains(userId, term, bargainsList) {
  const msg = JSON.stringify({
    type: 'bargain-alert',
    userId: userId,
    term,
    bargains: bargainsList
  });
  for (const ws of globalFrontendClients) {
    if (ws.userId === userId && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

// ─── Membership check helper (synchronous) ───────────────────────────
function checkMembership(userId) {
  const session = sessions.get(userId);
  if (!session) return false;
  // Master always allowed
  if (userId === MASTER_ID) return true;
  return !!session.membership; // null/empty means no membership
}

// ─── Scheduler (runs every minute) ────────────────────────────────────
setInterval(() => {
  for (const [userId, session] of sessions) {
    if (!session.membership && userId !== MASTER_ID) continue; // skip non‑members
    const userData = getUserData(userId);
    if (!userData) continue;
    for (const termObj of userData.terms) {
      if (!termObj.scanning) continue;
      const term = termObj.term;
      const interval = termObj.interval || 5;
      const active = Array.from(globalActiveJobs.values()).some(j => j.userId === userId && j.term === term && j.type === 'scan_new');
      const queued = globalJobQueue.some(j => j.userId === userId && j.term === term && j.type === 'scan_new');
      if (!active && !queued) {
        const history = loadUserHistory(userId, term);
        if (history.length > 0) {
          const last = new Date(history[0]?.firstSeen || 0);
          const mins = (Date.now() - last.getTime()) / 60000;
          if (mins >= interval) {
            globalJobQueue.push({ userId, term, type: 'scan_new' });
            console.log('Scheduling scan_new for "' + term + '" (user ' + userId + ') (last ' + Math.round(mins) + 'm ago)');
          }
        } else {
          globalJobQueue.push({ userId, term, type: 'scan_new' });
          console.log('Scheduling first scan_new for "' + term + '" (user ' + userId + ')');
        }
      }
    }
  }
  processQueue();
  // Broadcast updates to all active users
  for (const [userId] of sessions) {
    broadcastUpdate(userId);
  }
}, 60000);

// ─── Start server ──────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log('\n' + '='.repeat(60));
  console.log('Vinted Price Monitor Server (with Auth)');
  console.log('='.repeat(60));
  console.log('HTTP: http://localhost:' + PORT);
  console.log('WebSocket: ws://localhost:' + PORT);
  console.log('Data directory: ' + DATA_DIR);
  console.log('Persistent storage: ' + (process.env.RENDER_PERSISTENT_DISK ? 'enabled' : 'disabled'));
  console.log('Master ID: ' + MASTER_ID);
  console.log('='.repeat(60) + '\n');
});
