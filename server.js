const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const SEARCHES_DIR = path.join(__dirname, 'searches');
const TERMS_FILE = path.join(__dirname, 'terms.json');

// Ensure directories
if (!fs.existsSync(SEARCHES_DIR)) fs.mkdirSync(SEARCHES_DIR, { recursive: true });

// ─── State ──────────────────────────────────────────────────────────────
let searchTerms = [];                 // array of strings
let clients = new Map();              // clientId -> { ws, assigned: [] }
let termAssignments = new Map();      // term -> clientId (who is monitoring)

// Load persisted terms
if (fs.existsSync(TERMS_FILE)) {
  try {
    searchTerms = JSON.parse(fs.readFileSync(TERMS_FILE, 'utf8'));
  } catch (_) { searchTerms = []; }
}

// ─── File helpers (shared history) ────────────────────────────────────
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

// ─── Process scraped listings ─────────────────────────────────────────
function processScrapedListings(term, scraped) {
  const existing = loadHistory(term);
  const existingLinks = new Set(existing.map(item => item.link));
  
  const newListings = scraped.filter(item => !existingLinks.has(item.link));
  if (newListings.length === 0) return { added: 0, newListings: [] };

  const now = new Date().toISOString();
  newListings.forEach(item => item.firstSeen = now);

  // Update main history (newest first)
  const updated = [...newListings, ...existing];
  saveHistory(term, updated);

  // Update unique file (only most recent unique)
  saveUnique(term, newListings);

  return { added: newListings.length, newListings };
}

// ─── Term assignment logic ────────────────────────────────────────────
function assignTerms() {
  // Clear previous assignments
  termAssignments.clear();
  for (let [id, info] of clients) {
    info.assigned = [];
  }

  // Assign each term to a client (round-robin or simple)
  const clientIds = Array.from(clients.keys());
  if (clientIds.length === 0) return;

  let idx = 0;
  for (const term of searchTerms) {
    const clientId = clientIds[idx % clientIds.length];
    termAssignments.set(term, clientId);
    clients.get(clientId).assigned.push(term);
    idx++;
  }

  // Notify each client of their assigned terms
  for (let [id, info] of clients) {
    if (info.ws.readyState === WebSocket.OPEN) {
      info.ws.send(JSON.stringify({
        type: 'assign',
        terms: info.assigned
      }));
    }
  }
}

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// API: get all terms with assignments
app.get('/terms', (req, res) => {
  const list = searchTerms.map(term => ({
    term,
    assignedTo: termAssignments.get(term) || null
  }));
  res.json(list);
});

// API: add a term
app.post('/terms', (req, res) => {
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  if (searchTerms.includes(term)) {
    return res.status(409).json({ error: 'Term already exists' });
  }
  searchTerms.push(term);
  fs.writeFileSync(TERMS_FILE, JSON.stringify(searchTerms, null, 2));
  assignTerms(); // reassign
  res.json({ success: true, term });
});

// API: remove a term
app.delete('/terms/:term', (req, res) => {
  const term = req.params.term;
  const idx = searchTerms.indexOf(term);
  if (idx === -1) return res.status(404).json({ error: 'Term not found' });
  searchTerms.splice(idx, 1);
  fs.writeFileSync(TERMS_FILE, JSON.stringify(searchTerms, null, 2));
  termAssignments.delete(term);
  // Reassign to remove from clients
  assignTerms();
  res.json({ success: true });
});

// API: get search history (all or unique)
app.get('/searches', (req, res) => {
  const { term, type } = req.query;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  let listings = [];
  let file = '';
  if (type === 'unique') {
    file = getUniqueFile(term);
    try {
      if (fs.existsSync(file)) {
        const data = JSON.parse(fs.readFileSync(file, 'utf8'));
        listings = data.listings || [];
      }
    } catch (_) {}
  } else {
    listings = loadHistory(term);
  }
  res.json({
    term,
    type: type || 'all',
    count: listings.length,
    listings
  });
});

// Health
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    terms: searchTerms.length,
    clients: clients.size,
    assignments: Object.fromEntries(termAssignments)
  });
});

// ─── HTTP Server + WebSocket ──────────────────────────────────────────
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  let clientId = null;

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      switch (data.type) {
        case 'register': {
          clientId = data.clientId || `client-${Date.now()}-${Math.random().toString(36).substr(2, 6)}`;
          if (clients.has(clientId)) {
            // Re-register: close old connection? We'll replace.
            const old = clients.get(clientId);
            if (old.ws !== ws && old.ws.readyState === WebSocket.OPEN) {
              old.ws.close();
            }
          }
          clients.set(clientId, { ws, assigned: [] });
          console.log(`✅ Client registered: ${clientId} (${clients.size} total)`);
          // Reassign terms to include this new client
          assignTerms();
          break;
        }
        case 'scraped': {
          // Client sends scraped listings for a term
          const { term, listings } = data;
          if (!term || !listings) break;
          // Only accept if this client is assigned this term
          const clientInfo = clients.get(clientId);
          if (!clientInfo || !clientInfo.assigned.includes(term)) {
            ws.send(JSON.stringify({ type: 'error', message: 'Not assigned this term' }));
            break;
          }
          // Process and store
          const result = processScrapedListings(term, listings);
          // Respond with confirmation and new listings count
          ws.send(JSON.stringify({
            type: 'scraped-ack',
            term,
            added: result.added,
            newListings: result.newListings
          }));
          // Optionally broadcast to frontend or other clients
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
      // Reassign terms to remaining clients
      assignTerms();
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
  // Initial assignment if any clients are already connected? They'll register later.
});