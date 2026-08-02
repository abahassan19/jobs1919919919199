const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');
const { createClient } = require('@supabase/supabase-js');
const webpush = require('web-push');

// ─── Supabase config ──────────────────────────────────────────────────
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

// ─── Server config ────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
const BASE_DIR = process.env.RENDER_PERSISTENT_DISK || __dirname;
const DATA_DIR = path.join(BASE_DIR, 'userdata');
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

console.log(`Data directory: ${DATA_DIR}`);
console.log(`Persistent storage: ${process.env.RENDER_PERSISTENT_DISK ? 'enabled' : 'disabled'}`);

// ─── VAPID keys — PERSISTED so push keeps working across restarts ────
// Priority: env vars → saved file → generate & save to disk.
// If you don't set env vars, the keys are saved to userdata/vapid-keys.json
// so subscriptions survive restarts (on ephemeral disks they still won't
// survive a redeploy — set the env vars for that).
const VAPID_FILE = path.join(DATA_DIR, 'vapid-keys.json');
let VAPID_PUBLIC_KEY;
let VAPID_PRIVATE_KEY;

(function initVapid() {
  if (process.env.VAPID_PUBLIC_KEY && process.env.VAPID_PRIVATE_KEY) {
    VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
    VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;
    console.log('VAPID keys loaded from environment variables.');
    return;
  }
  try {
    if (fs.existsSync(VAPID_FILE)) {
      const saved = JSON.parse(fs.readFileSync(VAPID_FILE, 'utf8'));
      if (saved.publicKey && saved.privateKey) {
        VAPID_PUBLIC_KEY = saved.publicKey;
        VAPID_PRIVATE_KEY = saved.privateKey;
        console.log('VAPID keys loaded from ' + VAPID_FILE);
        return;
      }
    }
  } catch (_) {}
  const keys = webpush.generateVAPIDKeys();
  VAPID_PUBLIC_KEY = keys.publicKey;
  VAPID_PRIVATE_KEY = keys.privateKey;
  try {
    fs.writeFileSync(VAPID_FILE, JSON.stringify(keys, null, 2), 'utf8');
    console.log('Generated new VAPID keys and persisted to ' + VAPID_FILE);
  } catch (err) {
    console.log('Could not persist VAPID keys to disk: ' + err.message);
  }
  console.log('Set VAPID_PUBLIC_KEY / VAPID_PRIVATE_KEY env vars to keep keys stable across deploys.');
})();

webpush.setVapidDetails(
  'mailto:admin@vintedmonitor.com', // change to your email
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY
);

const ICON_URL = 'https://cdn-icons-png.flaticon.com/512/2331/2331966.png';

// ─── Constants ──────────────────────────────────────────────────────────
const MAX_TERMS = 30;
const MEMBERSHIP_SYNC_INTERVAL = 60000; // 1 minute

// ─── In-memory state ──────────────────────────────────────────────────
const sessions = new Map();           // userId -> { lastActive, membership }
const userCache = new Map();          // userId -> { terms, bargains, notified }
const pushSubscriptions = new Map();  // userId -> [ { endpoint, keys } ]

// ─── Global job queue and workers ──────────────────────────────────
let jobQueue = [];
let activeJobs = new Map();
let clients = new Map();
let frontendClients = new Set();

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
function getSubscriptionsFile(userId) {
  return path.join(getUserDir(userId), 'push-subs.json');
}
function getNotifiedFile(userId) {
  return path.join(getUserDir(userId), 'notified.json');
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

// ─── Push subscription storage ──────────────────────────────────────
function loadPushSubscriptions(userId) {
  const file = getSubscriptionsFile(userId);
  try {
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    }
  } catch (_) {}
  return [];
}
function savePushSubscriptions(userId, subs) {
  ensureUserDir(userId);
  const file = getSubscriptionsFile(userId);
  fs.writeFileSync(file, JSON.stringify(subs, null, 2), 'utf8');
}

// ─── Notified bargains tracking (deduplication) ─────────────────────
function loadNotified(userId) {
  const file = getNotifiedFile(userId);
  try {
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    }
  } catch (_) {}
  return {}; // { term: [link1, link2] }
}
function saveNotified(userId, notified) {
  ensureUserDir(userId);
  const file = getNotifiedFile(userId);
  fs.writeFileSync(file, JSON.stringify(notified, null, 2), 'utf8');
}

function getUserData(userId) {
  if (!userCache.has(userId)) {
    userCache.set(userId, {
      terms: loadUserTerms(userId),
      bargains: {},
      notified: loadNotified(userId)
    });
  }
  return userCache.get(userId);
}

// ─── Price analysis and average calculation ──────────────────────────
function computeAverage(listings) {
  const prices = listings
    .map(l => parseFloat(l.price.replace(/[^0-9.]/g, '')))
    .filter(p => !isNaN(p) && p > 0);
  if (prices.length === 0) return null;
  const sum = prices.reduce((a, b) => a + b, 0);
  return Math.round((sum / prices.length) * 100) / 100;
}

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

// ─── Process scraped data ─────────────────────────────────────────────
function processScrapedListings(userId, term, scraped, jobType) {
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) {
    console.error(`Term "${term}" not found for user ${userId}`);
    return { added: 0, newListings: [], averageComputed: false };
  }

  if (jobType === 'scan_all') {
    const avg = computeAverage(scraped);
    if (avg !== null) {
      termObj.averagePrice = avg;
      saveUserTerms(userId, userData.terms);
      console.log(`User ${userId} average for "${term}" set to £${avg}`);
      broadcastUpdate(userId);
      return { added: 0, newListings: [], averageComputed: true };
    } else {
      console.log(`No valid prices found for "${term}" to compute average`);
      return { added: 0, newListings: [], averageComputed: false };
    }
  }

  const existing = loadUserHistory(userId, term);
  const existingLinks = new Set(existing.map(item => item.link));
  const newListings = scraped.filter(item => !existingLinks.has(item.link));
  if (newListings.length === 0) return { added: 0, newListings: [] };

  const now = new Date().toISOString();
  newListings.forEach(item => item.firstSeen = now);

  const updated = [...newListings, ...existing];
  saveUserHistory(userId, term, updated);
  saveUserUnique(userId, term, newListings);

  if (termObj.averagePrice) {
    const analysis = analyzePrices(updated, termObj.averagePrice, termObj.thresholdPercent);
    if (analysis.bargains.length > 0) {
      const notified = userData.notified[term] || [];
      const notifiedSet = new Set(notified);
      const freshBargains = analysis.bargains.filter(b => !notifiedSet.has(b.link));

      if (freshBargains.length > 0) {
        const newNotified = [...notified, ...freshBargains.map(b => b.link)];
        userData.notified[term] = newNotified;
        saveNotified(userId, userData.notified);

        userData.bargains[term] = freshBargains;
        broadcastBargains(userId, term, freshBargains);
        // One push popup PER bargain — this is what triggers the notification
        sendPushNotifications(userId, term, freshBargains);
      }
    }
  }
  return { added: newListings.length, newListings };
}

// ─── Push notification sender ──────────────────────────────────────────
function handlePushError(userId, sub, err) {
  const code = err.statusCode;
  // 404/410 = subscription expired, 401/403 = VAPID keys changed/invalid.
  // In all cases the stored subscription is useless — drop it so the
  // client is told to re-enable (status shows pushEnabled = false).
  if (code === 404 || code === 410 || code === 401 || code === 403) {
    console.log(`Removing stale push subscription for ${userId} (status ${code})`);
    let subs = loadPushSubscriptions(userId);
    const before = subs.length;
    subs = subs.filter(s => s.endpoint !== sub.endpoint);
    if (subs.length !== before) {
      savePushSubscriptions(userId, subs);
      broadcastUpdate(userId);
    }
  } else {
    console.error(`Push send failed for ${userId}:`, code || err.message, err.body || '');
  }
}

function sendPushNotifications(userId, term, bargainsList) {
  const subs = loadPushSubscriptions(userId);
  if (!subs || subs.length === 0) return;

  console.log(`Sending ${bargainsList.length} push notification(s) for "${term}" to user ${userId} (${subs.length} device(s))`);

  // One notification per bargain so the user gets a popup for every deal.
  for (const item of bargainsList) {
    const name = (item.name || 'New bargain').substring(0, 60);
    const title = `${item.price} · ${name}`;
    const parts = [`${item.discount}% below average`];
    if (item.size) parts.push(`Size ${item.size}`);
    if (item.condition) parts.push(item.condition);
    const body = parts.join(' · ');

    const payload = JSON.stringify({
      title,
      body,
      icon: ICON_URL,
      badge: ICON_URL,
      url: item.link || '/',
      data: { term }
    });

    for (const sub of subs) {
      webpush.sendNotification(sub, payload).catch(err => handlePushError(userId, sub, err));
    }
  }
}

// ─── Broadcast functions ──────────────────────────────────────────────
function broadcastBargains(userId, term, bargainsList) {
  const msg = JSON.stringify({
    type: 'bargain-alert',
    userId,
    term,
    bargains: bargainsList
  });
  for (const ws of frontendClients) {
    if (ws.userId === userId && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

function broadcastUpdate(userId) {
  const userData = getUserData(userId);
  const termsWithStatus = userData.terms.map(t => ({
    ...t,
    active: Array.from(activeJobs.values()).some(j => j.userId === userId && j.term === t.term),
    listingCount: loadUserHistory(userId, t.term).length,
    bargainCount: (userData.bargains[t.term] || []).length
  }));
  const msg = JSON.stringify({
    type: 'update',
    userId,
    terms: termsWithStatus,
    clients: clients.size,
    active: Array.from(activeJobs.values()).filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    queue: jobQueue.filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    bargains: userData.bargains,
    pushEnabled: loadPushSubscriptions(userId).length > 0
  });
  for (const ws of frontendClients) {
    if (ws.userId === userId && ws.readyState === WebSocket.OPEN) {
      ws.send(msg);
    }
  }
}

// ─── Job queue ──────────────────────────────────────────────────────────
function processQueue() {
  if (jobQueue.length === 0) return;
  let availableClient = null;
  for (let [id, info] of clients) {
    if (!info.busy && info.ws.readyState === WebSocket.OPEN) {
      availableClient = id;
      break;
    }
  }
  if (!availableClient) return;
  let jobIndex = -1;
  for (let i = 0; i < jobQueue.length; i++) {
    const job = jobQueue[i];
    let active = false;
    for (const [key, act] of activeJobs) {
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
  const job = jobQueue.splice(jobIndex, 1)[0];
  const clientInfo = clients.get(availableClient);
  clientInfo.busy = true;
  const jobId = job.userId + '-' + job.term + '-' + job.type + '-' + Date.now();
  activeJobs.set(jobId, { userId: job.userId, term: job.term, type: job.type, clientId: availableClient, startTime: Date.now(), jobId });
  clientInfo.ws.send(JSON.stringify({
    type: 'job',
    term: job.term,
    jobId: jobId,
    jobType: job.type
  }));
  console.log('Assigned ' + job.type + ' for "' + job.term + '" (user ' + job.userId + ') to ' + availableClient);
}

function queueJob(userId, term, type) {
  if (jobQueue.some(j => j.userId === userId && j.term === term && j.type === type)) return;
  if (Array.from(activeJobs.values()).some(j => j.userId === userId && j.term === term && j.type === type)) return;
  jobQueue.push({ userId, term, type });
  processQueue();
}

// ─── Membership management ──────────────────────────────────────────
function hasMembership(userId) {
  const session = sessions.get(userId);
  return session && !!session.membership;
}

function cancelUserJobs(userId) {
  jobQueue = jobQueue.filter(j => j.userId !== userId);
  for (const [key, job] of activeJobs) {
    if (job.userId === userId) {
      activeJobs.delete(key);
    }
  }
  const userData = getUserData(userId);
  userData.terms.forEach(t => { t.scanning = false; });
  saveUserTerms(userId, userData.terms);
  broadcastUpdate(userId);
  console.log(`Cancelled all jobs for user ${userId} (membership revoked)`);
}

async function refreshMembership(userId) {
  try {
    const { data, error } = await supabase
      .from('keys')
      .select('membership')
      .eq('id', userId)
      .maybeSingle();
    if (error) {
      console.error(`Error refreshing membership for ${userId}:`, error);
      return;
    }
    const newMembership = data ? data.membership : null;
    const session = sessions.get(userId);
    if (session) {
      const oldMembership = session.membership;
      session.membership = newMembership;
      sessions.set(userId, session);
      if (oldMembership && !newMembership) {
        cancelUserJobs(userId);
      }
    }
  } catch (err) {
    console.error(`Error refreshing membership for ${userId}:`, err);
  }
}

async function syncAllMemberships() {
  const userIds = Array.from(sessions.keys());
  for (const userId of userIds) {
    await refreshMembership(userId);
  }
}

// ─── Express app ──────────────────────────────────────────────────────
const app = express();
app.use(cors());
app.use(express.json());

// ─── Serve Service Worker ──────────────────────────────────────────────
const SW_SCRIPT = `
self.addEventListener('push', function (event) {
  let data = {};
  try {
    data = event.data.json();
  } catch (e) {
    data = { title: 'Bargain Alert', body: 'A new bargain was found!', url: '/' };
  }
  const options = {
    body: data.body || 'Check your dashboard!',
    icon: data.icon || 'https://cdn-icons-png.flaticon.com/512/2331/2331966.png',
    badge: data.badge || 'https://cdn-icons-png.flaticon.com/512/2331/2331966.png',
    data: data.url || '/',
    vibrate: [200, 100, 200],
    requireInteraction: true
  };
  event.waitUntil(self.registration.showNotification(data.title || 'Vinted Monitor', options));
});

self.addEventListener('notificationclick', function (event) {
  event.notification.close();
  const url = event.notification.data || '/';
  event.waitUntil((async function () {
    const allClients = await clients.matchAll({ type: 'window', includeUncontrolled: true });
    for (const client of allClients) {
      if ('focus' in client) { await client.focus(); return; }
    }
    if (clients.openWindow) { await clients.openWindow(url); }
  })());
});
`;
app.get('/sw.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript');
  res.setHeader('Service-Worker-Allowed', '/');
  res.setHeader('Cache-Control', 'no-cache');
  res.send(SW_SCRIPT);
});

// ─── Responsive Frontend HTML ──────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=yes">
<title>Vinted Price Monitor</title>
<style>
/* ── Reset & base ───────────────────────────────────────────── */
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,-apple-system,sans-serif;background:#f5f6fa;color:#2c3e50;padding:12px;min-height:100vh}

/* ── Container ──────────────────────────────────────────────── */
.container{max-width:1200px;margin:0 auto}
.hidden{display:none !important}

/* ── Login ──────────────────────────────────────────────────── */
.login-container{max-width:400px;margin:60px auto;background:#fff;border-radius:12px;padding:24px;box-shadow:0 4px 20px rgba(0,0,0,0.08)}
.login-container h2{margin-top:0;font-weight:500;font-size:24px}
.login-container input{width:100%;padding:14px;margin:10px 0;border:1px solid #ddd;border-radius:8px;font-size:16px}
.login-container button{width:100%;padding:14px;background:#3498db;color:#fff;border:none;border-radius:8px;font-size:16px;font-weight:600;cursor:pointer}
.login-container .error{color:#e74c3c;font-size:14px;margin-top:5px}

/* ── Header ────────────────────────────────────────────────── */
.header{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px;margin-bottom:16px}
.header h1{font-size:22px;font-weight:600;letter-spacing:-0.3px}
.header .user{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.header .user span{font-size:14px;background:#ecf0f1;padding:4px 12px;border-radius:20px}
.logout-btn{background:#e74c3c;color:#fff;padding:6px 14px;border:none;border-radius:6px;cursor:pointer;font-weight:500;font-size:14px}

/* ── Cards ──────────────────────────────────────────────────── */
.card{background:#fff;border-radius:12px;padding:16px;margin-bottom:16px;box-shadow:0 2px 8px rgba(0,0,0,0.06)}
.card h3{font-size:17px;font-weight:600;margin-bottom:12px;color:#34495e}

/* ── Push card ─────────────────────────────────────────────── */
.push-card{background:#e8f4fd;border:1px solid #b8d4e8;border-radius:12px;padding:16px;display:flex;flex-direction:column;gap:12px;margin-bottom:16px}
.push-card .row{display:flex;flex-wrap:wrap;align-items:center;gap:10px}
.push-card .status{display:flex;align-items:center;gap:8px;font-size:14px}
.push-card .status .badge{display:inline-block;padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.badge-push-on{background:#2ecc71;color:#fff}
.badge-push-off{background:#95a5a6;color:#fff}
.push-card .instructions{font-size:14px;background:#fff;padding:12px 14px;border-radius:8px;border-left:4px solid #3498db;display:none}
.push-card .instructions.show{display:block}
.push-card .instructions strong{color:#e67e22}
button{background:#3498db;color:#fff;border:none;border-radius:8px;padding:10px 18px;font-size:14px;font-weight:500;cursor:pointer;touch-action:manipulation}
button:hover{background:#2980b9}
button.secondary{background:#95a5a6}
button.secondary:hover{background:#7f8c8d}
button.danger{background:#e74c3c}
button.danger:hover{background:#c0392b}
button.success{background:#2ecc71}
button.success:hover{background:#27ae60}
button:disabled{opacity:0.6;cursor:not-allowed}

/* ── Add term form ──────────────────────────────────────────── */
.add-form{display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}
.add-form .field{flex:1 1 160px;min-width:130px}
.add-form .field label{display:block;font-size:13px;font-weight:500;margin-bottom:3px;color:#7f8c8d}
.add-form .field input,.add-form .field select{width:100%;padding:10px 12px;border:1px solid #ddd;border-radius:8px;font-size:14px;background:#fff}
.add-form .field input{flex:1}
.add-form .field .help{font-size:11px;color:#95a5a6;margin-top:2px}
.add-form button{align-self:center;padding:10px 20px}
.term-limit-warning{color:#e67e22;font-size:14px;margin-top:6px;display:none}

/* ── Tabs ──────────────────────────────────────────────────── */
.tabs{display:flex;gap:6px;border-bottom:2px solid #ddd;margin-bottom:16px;flex-wrap:wrap}
.tab{padding:10px 14px;cursor:pointer;border:none;background:none;font-weight:500;color:#7f8c8d;font-size:15px;border-bottom:2px solid transparent;margin-bottom:-2px}
.tab.active{color:#3498db;border-bottom-color:#3498db}
.tab-content{display:none}
.tab-content.active{display:block}

/* ── Terms – Desktop table ─────────────────────────────────── */
.table-wrap{overflow-x:auto;margin-top:4px}
.terms-table{width:100%;border-collapse:collapse;font-size:14px}
.terms-table th{text-align:left;padding:10px 8px;background:#f8f9fa;font-weight:600;color:#2c3e50;border-bottom:2px solid #ecf0f1}
.terms-table td{padding:10px 8px;border-bottom:1px solid #ecf0f1;vertical-align:middle}
.terms-table .actions{display:flex;flex-wrap:wrap;gap:6px}
.badge{display:inline-block;padding:2px 10px;border-radius:20px;font-size:12px;font-weight:600}
.badge-active{background:#2ecc71;color:#fff}
.badge-idle{background:#bdc3c7;color:#2c3e50}
.badge-bargain{background:#e74c3c;color:#fff}

/* ── Terms – Mobile card layout ────────────────────────────── */
.term-card{background:#f8f9fa;border-radius:8px;padding:12px;margin-bottom:10px;border-left:4px solid #3498db;display:none}
.term-card .row{display:flex;justify-content:space-between;flex-wrap:wrap;margin:2px 0;font-size:14px}
.term-card .label{color:#7f8c8d;font-weight:500;min-width:80px}
.term-card .value{font-weight:500}
.term-card .actions{margin-top:8px;display:flex;flex-wrap:wrap;gap:6px}

/* ── Bargains ───────────────────────────────────────────────── */
.bargain-item{background:#fef9e7;border-left:4px solid #e74c3c;padding:12px;margin-bottom:8px;border-radius:6px;font-size:14px}
.bargain-item strong{display:block;margin-bottom:4px}
.bargain-item .meta{display:flex;flex-wrap:wrap;gap:12px;font-size:13px}
.bargain-item .meta a{color:#3498db;text-decoration:none;font-weight:500}

/* ── Workers ────────────────────────────────────────────────── */
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(100px,1fr));gap:10px;margin:8px 0}
.stat-box{background:#f8f9fa;padding:12px;border-radius:8px;text-align:center}
.stat-box .value{font-size:20px;font-weight:600;color:#2c3e50}
.stat-box .label{font-size:12px;color:#7f8c8d}
.workers-table{width:100%;border-collapse:collapse;font-size:14px;margin-top:8px}
.workers-table th{text-align:left;padding:8px;background:#f8f9fa;border-bottom:2px solid #ecf0f1}
.workers-table td{padding:8px;border-bottom:1px solid #ecf0f1}

/* ── Log ────────────────────────────────────────────────────── */
.log{background:#2c3e50;color:#ecf0f1;padding:12px;border-radius:8px;font-family:monospace;max-height:200px;overflow-y:auto;font-size:13px;line-height:1.5}
.log .timestamp{color:#7f8c8d}
.log .info{color:#3498db}
.log .success{color:#2ecc71}
.log .warning{color:#f1c40f}
.log .bargain{color:#e74c3c;font-weight:700}

.empty{color:#95a5a6;text-align:center;padding:20px;font-size:15px}

/* ── Password popup ─────────────────────────────────────────── */
.password-popup-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;z-index:9999;backdrop-filter:blur(4px)}
.password-popup{background:#fff;border-radius:16px;padding:30px 24px;max-width:420px;width:90%;text-align:center;box-shadow:0 20px 60px rgba(0,0,0,0.4);animation:popIn 0.25s ease-out}
.password-popup h2{color:#e74c3c;margin-top:0;font-size:26px}
.password-popup .password{background:#f8f9fa;padding:14px;border-radius:8px;font-size:28px;font-weight:700;font-family:monospace;letter-spacing:2px;color:#2c3e50;margin:16px 0;border:2px dashed #3498db}
.password-popup p{color:#555;line-height:1.5;margin-bottom:12px}
.password-popup .warning{color:#e74c3c;font-weight:600;font-size:14px}
.password-popup button{padding:12px 32px;background:#3498db;color:#fff;border:none;border-radius:8px;font-size:16px;font-weight:600;cursor:pointer}
@keyframes popIn{0%{transform:scale(0.9);opacity:0}100%{transform:scale(1);opacity:1}}

/* ─── Responsive breakpoints ────────────────────────────────── */
@media (max-width: 767px) {
  .header h1{font-size:20px}
  .push-card .row{flex-direction:column;align-items:stretch}
  .push-card .status{justify-content:center}
  .push-card button{width:100%}
  .add-form .field{flex:1 1 100%}
  .add-form button{width:100%}
  .tab{padding:8px 12px;font-size:14px}
  .terms-table{display:none}
  .term-card{display:block}
  .stats-grid{grid-template-columns:1fr 1fr}
}
@media (min-width: 768px) {
  .term-card{display:none}
  .terms-table{display:table}
}
@media (max-width: 480px) {
  body{padding:8px}
  .card{padding:12px}
  .bargain-item .meta{flex-direction:column;gap:4px}
}
</style>
</head>
<body>

<div id="passwordPopup" class="password-popup-overlay" style="display:none;">
<div class="password-popup">
<h2>🔑 Your Access Code</h2>
<p>Your password is:</p>
<div class="password" id="displayPassword"></div>
<p><strong>Write this down so it won't get lost!</strong></p>
<p class="warning">⚠️ You will need this code to log in.</p>
<br>
<button id="dismissPopup">I've saved my password</button>
</div>
</div>

<div id="loginScreen" class="login-container">
<h2>Login</h2>
<p>Enter your unique ID to log in.</p>
<input type="text" id="loginId" placeholder="Your ID" autocomplete="username" />
<div id="loginError" class="error"></div>
<button id="loginBtn">Login</button>
</div>

<div id="dashboard" class="container hidden">
<div class="header">
<h1>🛍️ Vinted Monitor</h1>
<div class="user">
<span id="userDisplay"></span>
<button class="logout-btn" id="logoutBtn">Logout</button>
</div>
</div>

<!-- Push Card -->
<div class="push-card" id="pushCard">
  <div class="row">
    <div class="status">
      <span id="pushStatusIcon">🔔</span>
      <span id="pushStatusText">Notifications: Checking...</span>
      <span id="pushStatusBadge" class="badge badge-push-off">Off</span>
    </div>
    <div style="display:flex;flex-wrap:wrap;gap:8px;">
      <button id="enablePushBtn" class="success">🔔 Enable Notifications</button>
      <button id="testPushBtn" class="success" style="display:none;">🔔 Test</button>
      <button id="refreshPushBtn" class="secondary" style="display:none;">🔄 Refresh</button>
    </div>
  </div>
  <div id="pushInstructions" class="instructions">
    <strong>📱 Device instructions:</strong> <span id="deviceInstructions">Loading...</span>
  </div>
</div>

<!-- Add Term -->
<div class="card">
<h3>Add Search Term</h3>
<div class="add-form">
  <div class="field">
    <label>Term</label>
    <input type="text" id="newTerm" placeholder="e.g., cortiez hoodie" />
  </div>
  <div class="field">
    <label>Deal %</label>
    <input type="number" id="threshold" value="20" step="1" min="0" />
    <div class="help">% below average</div>
  </div>
  <div class="field">
    <label>Interval</label>
    <select id="interval">
      <option value="5">5 min</option><option value="10">10 min</option>
      <option value="15">15 min</option><option value="20">20 min</option>
      <option value="30">30 min</option><option value="45">45 min</option>
      <option value="60">60 min</option>
    </select>
    <div class="help">scan frequency</div>
  </div>
  <button id="addBtn">Add Term</button>
</div>
<div id="termLimitWarning" class="term-limit-warning">⚠️ You have reached the maximum of 30 terms. Remove some to add more.</div>
</div>

<!-- Tabs -->
<div class="tabs">
  <button class="tab active" data-tab="terms">Terms</button>
  <button class="tab" data-tab="bargains">Bargains <span id="bargainCount" class="badge" style="background:#e74c3c;color:#fff;padding:0 8px;margin-left:4px;">0</span></button>
  <button class="tab" data-tab="workers">Workers</button>
  <button class="tab" data-tab="log">Log</button>
</div>

<!-- Tab contents -->
<div id="tab-terms" class="tab-content active">
  <div class="card">
    <h3>Your Search Terms</h3>
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
// ─── VAPID public key (base64url, must be converted to Uint8Array) ────
const VAPID_PUBLIC_KEY = '${VAPID_PUBLIC_KEY}';

let userId = null;
let bargains = {};
let pushSubscription = null;
let swRegistration = null;
const API_BASE = window.location.origin;

const loginScreen = document.getElementById('loginScreen');
const dashboard = document.getElementById('dashboard');
const loginIdInput = document.getElementById('loginId');
const loginError = document.getElementById('loginError');
const loginBtn = document.getElementById('loginBtn');
const logoutBtn = document.getElementById('logoutBtn');
const userDisplay = document.getElementById('userDisplay');
const termLimitWarning = document.getElementById('termLimitWarning');
const passwordPopup = document.getElementById('passwordPopup');
const dismissPopup = document.getElementById('dismissPopup');
const enablePushBtn = document.getElementById('enablePushBtn');
const testPushBtn = document.getElementById('testPushBtn');
const refreshPushBtn = document.getElementById('refreshPushBtn');
const pushStatusText = document.getElementById('pushStatusText');
const pushStatusBadge = document.getElementById('pushStatusBadge');
const pushStatusIcon = document.getElementById('pushStatusIcon');
const pushInstructions = document.getElementById('pushInstructions');
const deviceInstructions = document.getElementById('deviceInstructions');

// ─── Referral popup ────────────────────────────────────────────────────
const urlParams = new URLSearchParams(window.location.search);
const referral = urlParams.get('referral');
if (referral) {
  document.getElementById('displayPassword').textContent = referral;
  passwordPopup.style.display = 'flex';
}
dismissPopup.addEventListener('click', () => {
  passwordPopup.style.display = 'none';
  const newUrl = window.location.origin + window.location.pathname;
  window.history.pushState({}, '', newUrl);
});

// ─── Auth helpers ──────────────────────────────────────────────────────
async function authFetch(url, options = {}) {
  const headers = { ...options.headers, 'X-User-Id': userId };
  return fetch(url, { ...options, headers });
}

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
      localStorage.setItem('vinted_userId', userId);
      showDashboard();
    } else {
      loginError.textContent = data.error || 'Login failed. Please try again.';
    }
  } catch (err) {
    loginError.textContent = 'Network error. Please try again.';
  }
}

function logout() {
  userId = null;
  localStorage.removeItem('vinted_userId');
  loginScreen.classList.remove('hidden');
  dashboard.classList.add('hidden');
  loginIdInput.value = '';
  loginError.textContent = '';
  if (window.pollInterval) clearInterval(window.pollInterval);
  if (ws) ws.close();
  if (pushSubscription) {
    pushSubscription.unsubscribe().catch(console.error);
    pushSubscription = null;
  }
}

function showDashboard() {
  loginScreen.classList.add('hidden');
  dashboard.classList.remove('hidden');
  userDisplay.textContent = 'User: ' + userId;
  initWebSocket();
  fetchData();
  if (window.pollInterval) clearInterval(window.pollInterval);
  window.pollInterval = setInterval(fetchData, 3000);
  initPushNotifications();
}

// ─── WebSocket ──────────────────────────────────────────────────────────
let ws = null;
function initWebSocket() {
  if (ws) ws.close();
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(protocol + '//' + window.location.host);
  ws.onopen = () => ws.send(JSON.stringify({ type: 'register-frontend', userId }));
  ws.onmessage = (e) => {
    const d = JSON.parse(e.data);
    if (d.type === 'bargain-alert' && d.userId === userId) {
      addLog('Bargain found for "' + d.term + '": ' + d.bargains.length + ' items', 'bargain');
      fetchData();
    } else if (d.type === 'update') {
      renderAll(d);
    }
  };
  ws.onclose = () => setTimeout(initWebSocket, 5000);
}

// ─── PUSH NOTIFICATIONS (rewritten for cross-browser support) ──────────

// The subscribe() API requires a Uint8Array key — a raw base64 string
// makes subscribe() throw in Chrome/Firefox/Safari. This converts it.
function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const rawData = window.atob(base64);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; ++i) outputArray[i] = rawData.charCodeAt(i);
  return outputArray;
}

// iOS only allows Web Push from an installed PWA (Add to Home Screen).
function isStandalonePWA() {
  return window.matchMedia('(display-mode: standalone)').matches || (navigator.standalone === true);
}

function isPushSupported() {
  if (!('serviceWorker' in navigator)) { setPushStatus('❌', 'Service Workers not supported', 'badge-push-off'); return false; }
  if (!('PushManager' in window)) { setPushStatus('❌', 'Web Push not supported', 'badge-push-off'); return false; }
  if (!('Notification' in window)) { setPushStatus('❌', 'Notifications not supported', 'badge-push-off'); return false; }
  if (window.isSecureContext !== true) { setPushStatus('🔒', 'HTTPS (or localhost) is required for push', 'badge-push-off'); return false; }
  return true;
}

function setPushStatus(icon, text, badgeClass) {
  pushStatusIcon.textContent = icon;
  pushStatusText.textContent = text;
  pushStatusBadge.className = 'badge ' + badgeClass;
  pushStatusBadge.textContent = text.includes('enabled') ? 'On' : 'Off';
}

function showDeviceInstructions(force) {
  const isIOS = /iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 1);
  const isAndroid = /Android/.test(navigator.userAgent);
  let msg = '';
  if (isIOS && !isStandalonePWA()) {
    msg = '📱 <strong>iPhone / iPad:</strong> Web Push only works from an <strong>installed app</strong>. Tap the Share button (square with arrow) → <strong>"Add to Home Screen"</strong>, then open the app from your Home Screen and tap "Enable Notifications".';
  } else if (isIOS) {
    msg = '📱 <strong>iPhone / iPad:</strong> Tap "Enable Notifications", then tap <strong>"Allow"</strong> when the browser asks.';
  } else if (isAndroid) {
    msg = '📱 <strong>Android:</strong> Tap "Enable Notifications", then tap <strong>"Allow"</strong> on the browser prompt.';
  } else {
    msg = '💻 <strong>Desktop:</strong> Tap "Enable Notifications" and then click <strong>"Allow"</strong> on the browser prompt.';
  }
  deviceInstructions.innerHTML = msg;
  if (force || Notification.permission === 'denied' || Notification.permission === 'default') {
    pushInstructions.classList.add('show');
  } else {
    pushInstructions.classList.remove('show');
  }
}

async function sendSubscriptionToServer(sub) {
  const res = await authFetch(API_BASE + '/subscribe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(sub)
  });
  return res;
}

async function initPushNotifications() {
  if (!isPushSupported()) { enablePushBtn.style.display = 'none'; return; }

  try {
    swRegistration = await navigator.serviceWorker.register('/sw.js', { scope: '/' });
    console.log('Service Worker registered');

    const perm = Notification.permission;
    if (perm === 'granted') {
      const sub = await swRegistration.pushManager.getSubscription();
      if (sub) {
        pushSubscription = sub;
        setPushStatus('✅', 'Notifications enabled', 'badge-push-on');
        enablePushBtn.textContent = '✅ Enabled';
        enablePushBtn.disabled = true;
        testPushBtn.style.display = 'inline-block';
        // Re-sync with the server (covers server restarts / DB loss)
        await sendSubscriptionToServer(sub);
        return;
      }
    }
    if (perm === 'denied') {
      setPushStatus('🚫', 'Notifications blocked in browser settings', 'badge-push-off');
      enablePushBtn.style.display = 'inline-block';
      enablePushBtn.disabled = true;
      enablePushBtn.textContent = '🔔 Enable (blocked)';
      showDeviceInstructions(true);
      return;
    }
    setPushStatus('🔔', 'Click enable to get alerts', 'badge-push-off');
    enablePushBtn.style.display = 'inline-block';
    enablePushBtn.disabled = false;
    enablePushBtn.textContent = '🔔 Enable Notifications';
    showDeviceInstructions(false);
  } catch (err) {
    console.error('Push init error:', err);
    setPushStatus('⚠️', 'Error: ' + err.message, 'badge-push-off');
  }
}

enablePushBtn.addEventListener('click', onEnablePush);
refreshPushBtn.addEventListener('click', onEnablePush);

async function onEnablePush() {
  if (!userId) return;
  if (!isPushSupported()) return;

  // 1) Ask permission FIRST — must stay inside the click gesture or
  //    Safari/Chrome may silently refuse to show the prompt.
  let perm = Notification.permission;
  if (perm === 'default') {
    try {
      perm = await Notification.requestPermission();
    } catch (e) {
      alert('Could not request permission: ' + e.message);
      return;
    }
  }
  if (perm !== 'granted') {
    alert('Notification permission was denied. Enable it manually in your browser settings.');
    setPushStatus('🚫', 'Notifications blocked in browser settings', 'badge-push-off');
    showDeviceInstructions(true);
    return;
  }

  // 2) Register the service worker (if not already registered)
  if (!swRegistration) {
    try {
      swRegistration = await navigator.serviceWorker.register('/sw.js', { scope: '/' });
    } catch (e) {
      alert('Service Worker registration failed: ' + e.message);
      return;
    }
  }

  // 3) Get existing subscription or create a new one.
  //    applicationServerKey MUST be a Uint8Array (not the base64 string).
  try {
    let sub = await swRegistration.pushManager.getSubscription();
    if (!sub) {
      sub = await swRegistration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(VAPID_PUBLIC_KEY)
      });
    }
    pushSubscription = sub;

    const res = await sendSubscriptionToServer(sub);
    if (res.ok) {
      setPushStatus('✅', 'Notifications enabled', 'badge-push-on');
      enablePushBtn.textContent = '✅ Enabled';
      enablePushBtn.disabled = true;
      testPushBtn.style.display = 'inline-block';
      refreshPushBtn.style.display = 'none';
      pushInstructions.classList.remove('show');
      addLog('Push notifications enabled', 'success');

      // Immediately send a test popup so the user sees it works right away
      try {
        const t = await authFetch(API_BASE + '/test-push', { method: 'POST' });
        const td = await t.json();
        if (td.success) addLog('Test notification sent — check for the popup', 'success');
      } catch (_) {}
    } else {
      const errData = await res.json();
      alert('Server rejected the subscription: ' + (errData.error || 'unknown error'));
    }
  } catch (err) {
    console.error('Subscribe error:', err);
    if (/iPad|iPhone|iPod/.test(navigator.userAgent) && !isStandalonePWA()) {
      alert('On iPhone/iPad you must add this site to your Home Screen first, then open it from there and enable notifications.');
      showDeviceInstructions(true);
    } else {
      alert('Failed to enable notifications: ' + err.message);
    }
  }
}

testPushBtn.addEventListener('click', async () => {
  if (!userId) return;
  testPushBtn.disabled = true;
  testPushBtn.textContent = 'Sending…';
  try {
    const res = await authFetch(API_BASE + '/test-push', { method: 'POST' });
    const data = await res.json();
    if (data.success) {
      addLog('Test notification sent to ' + data.sent + ' device(s)', 'success');
      alert('Test notification sent to ' + data.sent + ' device(s). Check for the popup!');
    } else {
      alert(data.error || 'Failed to send test notification.');
    }
  } catch (e) {
    alert('Network error while sending test notification.');
  }
  testPushBtn.disabled = false;
  testPushBtn.textContent = '🔔 Test';
});

// ─── Rendering functions ───────────────────────────────────────────────
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

  // Keep push status in sync with the server
  if (data.pushEnabled) {
    setPushStatus('✅', 'Notifications enabled', 'badge-push-on');
    enablePushBtn.textContent = '✅ Enabled';
    enablePushBtn.disabled = true;
    testPushBtn.style.display = 'inline-block';
  } else if (pushSubscription) {
    // We have a local subscription but the server lost it (expired/restart)
    setPushStatus('⚠️', 'Re-enable notifications', 'badge-push-off');
    enablePushBtn.style.display = 'inline-block';
    enablePushBtn.disabled = false;
    enablePushBtn.textContent = '🔔 Re-enable';
    testPushBtn.style.display = 'none';
    refreshPushBtn.style.display = 'inline-block';
  }
}

function renderTerms(terms) {
  const container = document.getElementById('termContainer');
  if (!terms || terms.length === 0) {
    container.innerHTML = '<div class="empty">No search terms. Add one above.</div>';
    return;
  }

  // ── Desktop table ──
  let tableHtml = '<div class="table-wrap"><table class="terms-table"><thead><tr><th>Term</th><th>Avg Price</th><th>Deal %</th><th>Interval</th><th>Status</th><th>Listings</th><th>Bargains</th><th>Actions</th></tr></thead><tbody>';
  terms.forEach(t => {
    const status = t.active ? '<span class="badge badge-active">Scanning</span>' : '<span class="badge badge-idle">Idle</span>';
    const avgDisplay = t.averagePrice ? '£' + t.averagePrice : 'Not set';
    let scanBtn = '';
    if (t.averagePrice) {
      if (t.scanning) {
        scanBtn = '<button class="danger" data-action="stop" data-term="' + t.term + '">Stop</button>';
      } else {
        scanBtn = '<button class="success" data-action="scan" data-term="' + t.term + '">Scan</button>';
      }
    } else {
      scanBtn = '<button class="secondary" data-action="avg" data-term="' + t.term + '">Calc Avg</button>';
    }
    tableHtml += '<tr><td><strong>' + t.term + '</strong></td><td>' + avgDisplay + '</td><td>' + t.thresholdPercent + '%</td><td>' + t.interval + ' min</td><td>' + status + '</td><td>' + (t.listingCount || 0) + '</td><td>' + (t.bargainCount || 0) + '</td><td class="actions">' + scanBtn + '<button class="danger" data-action="remove" data-term="' + t.term + '">✕</button></td></tr>';
  });
  tableHtml += '</tbody></table></div>';

  // ── Mobile cards ──
  let cardHtml = '';
  terms.forEach(t => {
    const status = t.active ? '<span class="badge badge-active">Scanning</span>' : '<span class="badge badge-idle">Idle</span>';
    const avgDisplay = t.averagePrice ? '£' + t.averagePrice : 'Not set';
    let scanBtn = '';
    if (t.averagePrice) {
      if (t.scanning) {
        scanBtn = '<button class="danger" data-action="stop" data-term="' + t.term + '">Stop</button>';
      } else {
        scanBtn = '<button class="success" data-action="scan" data-term="' + t.term + '">Scan</button>';
      }
    } else {
      scanBtn = '<button class="secondary" data-action="avg" data-term="' + t.term + '">Calc Avg</button>';
    }
    cardHtml += '<div class="term-card" data-term="' + t.term + '">';
    cardHtml += '<div class="row"><span class="label">Term</span><span class="value"><strong>' + t.term + '</strong></span></div>';
    cardHtml += '<div class="row"><span class="label">Avg Price</span><span class="value">' + avgDisplay + '</span></div>';
    cardHtml += '<div class="row"><span class="label">Deal %</span><span class="value">' + t.thresholdPercent + '%</span></div>';
    cardHtml += '<div class="row"><span class="label">Interval</span><span class="value">' + t.interval + ' min</span></div>';
    cardHtml += '<div class="row"><span class="label">Status</span><span class="value">' + status + '</span></div>';
    cardHtml += '<div class="row"><span class="label">Listings</span><span class="value">' + (t.listingCount || 0) + '</span></div>';
    cardHtml += '<div class="row"><span class="label">Bargains</span><span class="value">' + (t.bargainCount || 0) + '</span></div>';
    cardHtml += '<div class="actions">' + scanBtn + '<button class="danger" data-action="remove" data-term="' + t.term + '">Remove</button></div>';
    cardHtml += '</div>';
  });

  container.innerHTML = tableHtml + cardHtml;

  // ── Attach event listeners ──
  container.querySelectorAll('[data-action="remove"]').forEach(btn => {
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
  container.querySelectorAll('[data-action="avg"]').forEach(btn => {
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
  container.querySelectorAll('[data-action="scan"]').forEach(btn => {
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
  container.querySelectorAll('[data-action="stop"]').forEach(btn => {
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
    html += '<table class="workers-table"><thead><tr><th>Term</th><th>Type</th></tr></thead><tbody>';
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
        html += '<div class="bargain-item"><strong>' + item.name + '</strong><div class="meta"><span>Price: ' + item.price + '</span><span>Discount: ' + item.discount + '%</span><span>Size: ' + (item.size || 'N/A') + '</span><span>Condition: ' + (item.condition || 'N/A') + '</span><a href="' + item.link + '" target="_blank">View</a></div></div>';
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
      if (res.status === 401) { logout(); loginError.textContent = 'Session expired.'; }
      return;
    }
    const data = await res.json();
    renderAll(data);
  } catch (e) { console.error(e); }
}

// ─── Event listeners ──────────────────────────────────────────────────
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

loginBtn.addEventListener('click', login);
loginIdInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') login(); });
logoutBtn.addEventListener('click', logout);

const savedId = localStorage.getItem('vinted_userId');
if (savedId) {
  loginIdInput.value = savedId;
  login();
}

document.querySelectorAll('.tab').forEach(tab => {
  tab.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    tab.classList.add('active');
    document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
  });
});

addLog('Dashboard ready. Please log in.', 'info');

setTimeout(() => {
  showDeviceInstructions(false);
}, 1000);
</script>
</body>
</html>`;

app.get('/', (req, res) => res.send(HTML));

// ─── Referral route ──────────────────────────────────────────────────
app.get('/referral', (req, res) => {
  const referralCode = req.query.referral || '3498374473';
  res.redirect('/?referral=' + encodeURIComponent(referralCode));
});

// ─── Login endpoint ──────────────────────────────────────────────────
app.post('/login', async (req, res) => {
  const { id } = req.body;
  if (!id) return res.status(400).json({ error: 'Missing ID' });

  try {
    const { data, error } = await supabase
      .from('keys')
      .select('id, membership')
      .eq('id', id)
      .maybeSingle();

    if (error || !data) {
      return res.status(401).json({ error: 'Invalid ID' });
    }

    sessions.set(id, { lastActive: Date.now(), membership: data.membership || null });
    res.json({ userId: id });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// ─── Middleware: require user ID header ──────────────────────────────
app.use((req, res, next) => {
  if (req.path === '/login' || req.path === '/' || req.path === '/referral' || req.path === '/sw.js') return next();
  const userId = req.headers['x-user-id'];
  if (!userId || !sessions.has(userId)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  sessions.set(userId, { ...sessions.get(userId), lastActive: Date.now() });
  req.userId = userId;
  next();
});

// ─── API endpoints ──────────────────────────────────────────────────

// GET /status
app.get('/status', (req, res) => {
  const userId = req.userId;
  const userData = getUserData(userId);
  const termStatus = userData.terms.map(t => ({
    ...t,
    active: Array.from(activeJobs.values()).some(j => j.userId === userId && j.term === t.term),
    listingCount: loadUserHistory(userId, t.term).length,
    bargainCount: (userData.bargains[t.term] || []).length
  }));
  res.json({
    terms: termStatus,
    clients: clients.size,
    active: Array.from(activeJobs.values()).filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    queue: jobQueue.filter(j => j.userId === userId).map(j => ({ term: j.term, type: j.type })),
    bargains: userData.bargains,
    pushEnabled: loadPushSubscriptions(userId).length > 0
  });
});

// POST /subscribe (push subscription)
app.post('/subscribe', (req, res) => {
  const userId = req.userId;
  const subscription = req.body;
  if (!subscription || !subscription.endpoint) {
    return res.status(400).json({ error: 'Invalid subscription' });
  }
  let subs = loadPushSubscriptions(userId);
  subs = subs.filter(s => s.endpoint !== subscription.endpoint);
  subs.push(subscription);
  savePushSubscriptions(userId, subs);
  console.log(`Push subscription saved for user ${userId} (${subs.length} total)`);
  broadcastUpdate(userId);
  res.json({ success: true, count: subs.length });
});

// POST /test-push — sends a test notification so the user can verify push works
app.post('/test-push', (req, res) => {
  const userId = req.userId;
  const subs = loadPushSubscriptions(userId);
  if (subs.length === 0) {
    return res.status(400).json({ error: 'No push subscription found. Enable notifications first.' });
  }
  const payload = JSON.stringify({
    title: '🔔 Test notification',
    body: 'Push notifications are working! You will now get a popup for every bargain.',
    icon: ICON_URL,
    badge: ICON_URL,
    url: '/',
    data: { term: null }
  });
  let sent = 0;
  let failed = 0;
  Promise.all(subs.map(sub =>
    webpush.sendNotification(sub, payload)
      .then(() => { sent++; })
      .catch(err => { handlePushError(userId, sub, err); failed++; })
  )).then(() => {
    res.json({ success: sent > 0, sent, failed });
  });
});

// POST /terms
app.post('/terms', (req, res) => {
  const userId = req.userId;
  const { term, thresholdPercent = 20, interval = 5 } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  if (!hasMembership(userId)) {
    return res.status(403).json({ error: 'No active membership. Please upgrade.' });
  }
  if (userData.terms.length >= MAX_TERMS) {
    return res.status(409).json({ error: 'Maximum 30 terms reached. Remove some.' });
  }
  if (userData.terms.find(t => t.term === term)) {
    return res.status(409).json({ error: 'Term already exists' });
  }
  const obj = { term, averagePrice: null, thresholdPercent: parseInt(thresholdPercent), interval: parseInt(interval), scanning: false };
  userData.terms.push(obj);
  saveUserTerms(userId, userData.terms);
  broadcastUpdate(userId);
  res.json({
    success: true, term: obj
  });
});

// DELETE /terms/:term
app.delete('/terms/:term', (req, res) => {
  const userId = req.userId;
  const term = req.params.term;
  const userData = getUserData(userId);
  const idx = userData.terms.findIndex(t => t.term === term);
  if (idx === -1) return res.status(404).json({ error: 'Not found' });
  userData.terms.splice(idx, 1);
  saveUserTerms(userId, userData.terms);
  delete userData.bargains[term];
  delete userData.notified[term];
  saveNotified(userId, userData.notified);
  broadcastUpdate(userId);
  res.json({ success: true });
});

// POST /calculate-average
app.post('/calculate-average', (req, res) => {
  const userId = req.userId;
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  if (!hasMembership(userId)) {
    return res.status(403).json({ error: 'No active membership.' });
  }
  queueJob(userId, term, 'scan_all');
  res.json({ success: true });
});

// POST /start-scan
app.post('/start-scan', (req, res) => {
  const userId = req.userId;
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  if (!termObj.averagePrice) {
    return res.status(400).json({ error: 'Average price not set. Please calculate average first.' });
  }
  if (!hasMembership(userId)) {
    return res.status(403).json({ error: 'No active membership.' });
  }
  if (termObj.scanning) {
    return res.status(409).json({ error: 'Scanning already active' });
  }
  termObj.scanning = true;
  saveUserTerms(userId, userData.terms);
  queueJob(userId, term, 'scan_new');
  res.json({ success: true });
});

// POST /stop-scan
app.post('/stop-scan', (req, res) => {
  const userId = req.userId;
  const { term } = req.body;
  if (!term) return res.status(400).json({ error: 'Missing term' });
  const userData = getUserData(userId);
  const termObj = userData.terms.find(t => t.term === term);
  if (!termObj) return res.status(404).json({ error: 'Term not found' });
  termObj.scanning = false;
  saveUserTerms(userId, userData.terms);
  jobQueue = jobQueue.filter(j => !(j.userId === userId && j.term === term && j.type === 'scan_new'));
  broadcastUpdate(userId);
  res.json({ success: true });
});

// GET /searches
app.get('/searches', (req, res) => {
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
            frontendClients.add(ws);
            console.log('Frontend connected for user ' + userId);
            broadcastUpdate(userId);
          } else {
            ws.close();
          }
          break;
        case 'register-worker':
          clientId = data.clientId || 'worker-' + Date.now();
          if (clients.has(clientId)) {
            const old = clients.get(clientId);
            if (old.ws !== ws && old.ws.readyState === WebSocket.OPEN) old.ws.close();
          }
          clients.set(clientId, { ws, busy: false, lastPing: Date.now() });
          console.log('Worker registered: ' + clientId + ' (' + clients.size + ' total)');
          ws.send(JSON.stringify({ type: 'registered', clientId }));
          processQueue();
          break;
        case 'job-complete': {
          const { term, listings, jobId, jobType } = data;
          if (!term || !listings) break;
          let job = null;
          let jobKey = null;
          for (const [key, j] of activeJobs) {
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
          if (clientId && clients.has(clientId)) {
            clients.get(clientId).busy = false;
          }
          if (jobKey) {
            activeJobs.delete(jobKey);
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
          for (const [key, j] of activeJobs) {
            if (j.jobId === jobId) {
              job = j;
              jobKey = key;
              break;
            }
          }
          if (job) {
            const uid = job.userId;
            console.log('Job failed for "' + term + '" (' + (jobType || 'scan_new') + ') from ' + clientId + ' for user ' + uid + ': ' + error);
            if (clientId && clients.has(clientId)) {
              clients.get(clientId).busy = false;
            }
            if (jobKey) {
              activeJobs.delete(jobKey);
            }
            if ((jobType || 'scan_new') === 'scan_new') {
              jobQueue.push({ userId: uid, term, type: 'scan_new' });
            }
            processQueue();
            broadcastUpdate(uid);
          } else {
            console.log('Received job-failed for unknown jobId: ' + jobId);
          }
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
          console.log('Unknown message type: ' + data.type);
      }
    } catch (err) {
      console.error('WebSocket message error:', err);
    }
  });

  ws.on('close', (code) => {
    if (ws.isFrontend) {
      frontendClients.delete(ws);
      console.log('Frontend disconnected' + (userId ? ' for user ' + userId : ''));
      return;
    }
    if (clientId) {
      let lostJobKey = null;
      for (const [key, job] of activeJobs) {
        if (job.clientId === clientId) {
          lostJobKey = key;
          break;
        }
      }
      clients.delete(clientId);
      console.log('Worker disconnected: ' + clientId + ' (' + clients.size + ' remaining) code ' + code);
      if (lostJobKey) {
        const job = activeJobs.get(lostJobKey);
        if (job) {
          if (job.type === 'scan_new') {
            jobQueue.push({ userId: job.userId, term: job.term, type: 'scan_new' });
          }
          activeJobs.delete(lostJobKey);
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

// ─── Heartbeat ──────────────────────────────────────────────────────────
setInterval(() => {
  const now = Date.now();
  for (let [id, info] of clients) {
    if (now - info.lastPing > 30000) {
      console.log('Worker ' + id + ' stale, terminating');
      info.ws.terminate();
      clients.delete(id);
    }
  }
}, 10000);

// ─── Scheduler ──────────────────────────────────────────────────────────
setInterval(() => {
  for (const [userId, session] of sessions) {
    if (!hasMembership(userId)) continue;
    const userData = getUserData(userId);
    if (!userData) continue;
    for (const termObj of userData.terms) {
      if (!termObj.scanning) continue;
      const term = termObj.term;
      const interval = termObj.interval || 5;
      const active = Array.from(activeJobs.values()).some(j => j.userId === userId && j.term === term && j.type === 'scan_new');
      const queued = jobQueue.some(j => j.userId === userId && j.term === term && j.type === 'scan_new');
      if (!active && !queued) {
        const history = loadUserHistory(userId, term);
        if (history.length > 0) {
          const last = new Date(history[0]?.firstSeen || 0);
          const mins = (Date.now() - last.getTime()) / 60000;
          if (mins >= interval) {
            jobQueue.push({ userId, term, type: 'scan_new' });
            console.log('Scheduling scan_new for "' + term + '" (user ' + userId + ') (last ' + Math.round(mins) + 'm ago)');
          }
        } else {
          jobQueue.push({ userId, term, type: 'scan_new' });
          console.log('Scheduling first scan_new for "' + term + '" (user ' + userId + ')');
        }
      }
    }
  }
  processQueue();
  for (const [userId] of sessions) {
    broadcastUpdate(userId);
  }
}, 60000);

// ─── Membership sync ──────────────────────────────────────────────────
setInterval(() => {
  syncAllMemberships().catch(err => console.error('Membership sync error:', err));
}, MEMBERSHIP_SYNC_INTERVAL);

// ─── Start server ──────────────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  console.log('\n' + '='.repeat(60));
  console.log('Vinted Price Monitor Server (with Push & Responsive UI)');
  console.log('='.repeat(60));
  console.log('HTTP: http://localhost:' + PORT);
  console.log('WebSocket: ws://localhost:' + PORT);
  console.log('Data directory: ' + DATA_DIR);
  console.log('Persistent storage: ' + (process.env.RENDER_PERSISTENT_DISK ? 'enabled' : 'disabled'));
  console.log('Membership sync interval: ' + MEMBERSHIP_SYNC_INTERVAL / 1000 + 's');
  console.log('Referral route: /referral?referral=YOUR_CODE');
  console.log('VAPID Public Key: ' + VAPID_PUBLIC_KEY.substring(0, 30) + '...');
  console.log('='.repeat(60) + '\n');
});
