/**
 * Torrent Microservice
 *
 * Standalone Node.js HTTP server for WebTorrent management.
 * Run separately from the main Bun application:
 *   cd torrent-service && pnpm install && node index.mjs
 *
 * REST API:
 *   POST   /downloads              Add a download (magnet link or file:// URL)
 *   GET    /downloads/active       List active (non-paused) downloads [?path=]
 *   GET    /downloads/waiting      List paused/waiting downloads [?offset=&num=&path=]
 *   GET    /downloads/completed    List finished downloads kept as history [?path=]
 *   DELETE /downloads/completed    Dismiss the whole completed history
 *   GET    /downloads/:gid         Get status of a specific download
 *   PATCH  /downloads/:gid         Pause or resume { action: 'pause'|'resume' }
 *   DELETE /downloads/:gid         Remove a download
 *   GET    /tracker/test           Test tracker connectivity [?url=]
 *
 * A finished download is kept in a bounded history (see COMPLETED_HISTORY) rather
 * than forgotten: the clients that care about a completion are not necessarily
 * connected at the moment it happens. Dismissing a completed entry never touches
 * the files it produced — removing an UNFINISHED download does delete its partial
 * data, which is useless once the torrent is gone.
 *
 * WebSocket:
 *   ws://host:PORT/events          Push events to connected clients
 *   Event format: { type, payload }
 *   Types: download-progress, download-complete, download-paused, download-resumed,
 *          download-removed, downloads-status (state snapshot sent on connect)
 *
 * Environment:
 *   TORRENT_SERVICE_PORT     Port to listen on (default: 9669)
 *   TORRENT_STATE_FILE       Path to persist download state (default: ./torrent-downloads.json)
 *   TORRENT_COMPLETED_HISTORY  How many finished downloads to keep (default: 100)
 */

import http from 'http';
import https from 'https';
import { readFile, writeFile, rm } from 'fs/promises';
import { existsSync } from 'fs';
import { resolve, sep } from 'path';
import { WebSocketServer } from 'ws';
import WebTorrent from 'webtorrent';

const PORT = parseInt(process.env.TORRENT_SERVICE_PORT || '9669', 10);
const STATE_FILE = process.env.TORRENT_STATE_FILE || resolve(process.cwd(), './torrent-downloads.json');
const COMPLETED_HISTORY = parseInt(process.env.TORRENT_COMPLETED_HISTORY || '100', 10);

// --- Logger ---
const log = {
  info:  (msg, meta) => console.log(`[torrent-service] INFO  ${msg}${meta ? ' ' + JSON.stringify(meta) : ''}`),
  warn:  (msg, meta) => console.warn(`[torrent-service] WARN  ${msg}${meta ? ' ' + JSON.stringify(meta) : ''}`),
  error: (msg, meta) => console.error(`[torrent-service] ERROR ${msg}${meta ? ' ' + JSON.stringify(meta) : ''}`),
  debug: (msg, meta) => console.log(`[torrent-service] DEBUG ${msg}${meta ? ' ' + JSON.stringify(meta) : ''}`),
};

// --- WebSocket event broadcast ---
const wss = new WebSocketServer({ noServer: true });

function broadcast(event) {
  const data = JSON.stringify(event);
  wss.clients.forEach((ws) => {
    if (ws.readyState === ws.OPEN) ws.send(data);
  });
}

// --- State ---
let client = null;
const pausedDownloads = new Map();    // gid → torrentInfo (for downloads removed from WebTorrent)
const progressIntervals = new Map();  // gid → intervalId
const downloadMeta = new Map();       // gid → { url, dir, relativePath, addedAt }
const completedDownloads = new Map(); // gid → completed record, oldest first (see rememberCompleted)

// --- WebTorrent client ---
function ensureClient() {
  if (client) return client;

  const randomId = () => {
    const b = Buffer.alloc(12);
    for (let i = 0; i < 12; i++) b[i] = Math.floor(Math.random() * 256);
    return Buffer.concat([Buffer.from('-qB4600-'), b]);
  };

  client = new WebTorrent({
    dht: true,
    pex: true,
    tracker: { userAgent: 'qBittorrent/4.6.0' },
    maxConns: 100,
    maxPeers: 100,
    ports: [6881, 6882],
    nodeId: randomId(),
    peerId: randomId(),
  });

  log.info('WebTorrent client initialized');
  return client;
}

// --- Helpers ---
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

function formatTorrentInfo(torrent) {
  const gid = torrent.infoHash;
  return {
    gid,
    name: torrent.name || 'Unknown',
    status: torrent.paused ? 'paused' : 'active',
    progress: torrent.progress ? Math.round(torrent.progress * 100) : 0,
    downloadSpeed: formatBytes(torrent.downloadSpeed || 0) + '/s',
    downloaded: formatBytes(torrent.downloaded || 0),
    totalSize: formatBytes(torrent.length || 0),
    error: null,
    errorCode: null,
    isTorrent: true,
    infoHash: torrent.infoHash,
    uploadSpeed: formatBytes(torrent.uploadSpeed || 0) + '/s',
    seeders: torrent.numPeers || 0,
    peers: torrent.numPeers || 0,
    path: downloadMeta.get(gid)?.relativePath || '',
  };
}

/**
 * Snapshot a finished torrent for the history.
 *
 * Taken while the torrent is still in the client: once it is removed its stats
 * are gone, and the completion event itself only carries a gid.
 */
function toCompletedRecord(info, relativePath) {
  return {
    ...info,
    status: 'complete',
    progress: 100,
    downloadSpeed: '0 B/s',
    uploadSpeed: '0 B/s',
    seeders: 0,
    peers: 0,
    path: relativePath,
    completedAt: new Date().toISOString(),
  };
}

/** Record a completion, evicting the oldest once the history is full. */
function rememberCompleted(record) {
  completedDownloads.delete(record.gid); // re-insert so the map stays oldest-first
  completedDownloads.set(record.gid, record);
  while (completedDownloads.size > COMPLETED_HISTORY) {
    completedDownloads.delete(completedDownloads.keys().next().value);
  }
}

function startProgressInterval(torrent, gid) {
  let lastLoggedPeers = -1;
  const interval = setInterval(() => {
    if (torrent.destroyed) return;
    const info = formatTorrentInfo(torrent);
    if (torrent.numPeers !== lastLoggedPeers && (torrent.numPeers > 0 || lastLoggedPeers === -1)) {
      log.debug('Peers update', { gid, peers: torrent.numPeers, progress: info.progress });
      lastLoggedPeers = torrent.numPeers;
    }
    broadcast({ type: 'download-progress', payload: info });
  }, 1000);
  progressIntervals.set(gid, interval);
  return interval;
}

async function saveDownloadState() {
  try {
    const active = client
      ? client.torrents.map((t) => ({
          gid: t.infoHash,
          url: downloadMeta.get(t.infoHash)?.url,
          dir: downloadMeta.get(t.infoHash)?.dir,
          relativePath: downloadMeta.get(t.infoHash)?.relativePath,
          name: t.name,
          addedAt: downloadMeta.get(t.infoHash)?.addedAt,
          status: t.paused ? 'paused' : 'active',
        }))
      : [];

    const paused = Array.from(pausedDownloads.entries()).map(([gid, info]) => ({
      gid,
      url: downloadMeta.get(gid)?.url,
      dir: downloadMeta.get(gid)?.dir,
      relativePath: downloadMeta.get(gid)?.relativePath,
      name: info.name,
      addedAt: downloadMeta.get(gid)?.addedAt,
      status: 'paused',
    }));

    // Completed entries are stored whole: there is no torrent left to rebuild
    // them from, so the record itself is the only copy.
    const completed = Array.from(completedDownloads.values());

    await writeFile(STATE_FILE, JSON.stringify([...active, ...paused, ...completed], null, 2));
  } catch (err) {
    log.error('Failed to save download state', { error: err.message });
  }
}

async function restoreDownloads() {
  try {
    if (!existsSync(STATE_FILE)) return;
    const saved = JSON.parse(await readFile(STATE_FILE, 'utf-8'));
    for (const entry of saved) {
      // History rows: restored as-is, never handed back to WebTorrent.
      if (entry.status === 'complete') {
        if (entry.gid) rememberCompleted(entry);
        continue;
      }
      if (entry.status !== 'active' && entry.status !== 'paused') continue;
      log.info('Restoring torrent', { name: entry.name, gid: entry.gid, status: entry.status });
      await addDownload(entry.url, {
        dir: entry.dir,
        relativePath: entry.relativePath,
        paused: entry.status === 'paused',
        skipSave: true,
      }).catch((err) => log.warn('Failed to restore torrent', { gid: entry.gid, error: err.message }));
    }
    // Save once after all restores instead of N individual saves
    await saveDownloadState();
  } catch (err) {
    log.error('Failed to restore downloads', { error: err.message });
  }
}

function setupTorrentListeners(torrent, gid) {
  torrent.on('peer', () => {
    log.debug('Peer connected', { gid, numPeers: torrent.numPeers });
  });

  torrent.on('noPeers', (announceType) => {
    log.warn('No peers available', { gid, announceType, totalPeers: torrent.numPeers });
  });

  torrent.on('trackerAnnounce', () => {
    log.info('Tracker announce', { gid, numPeers: torrent.numPeers });
  });

  torrent.on('trackerError', (err) => {
    log.error('Tracker error', { gid, error: err.message, tracker: err.announce || 'unknown' });
  });

  torrent.on('trackerWarning', (warning) => {
    log.warn('Tracker warning', { gid, warning: warning.message || warning });
  });

  torrent.on('done', async () => {
    log.info('Download completed', { gid, name: torrent.name });
    const interval = progressIntervals.get(gid);
    if (interval) { clearInterval(interval); progressIntervals.delete(gid); }

    const completedRelativePath = downloadMeta.get(gid)?.relativePath || '';
    const record = toCompletedRecord(formatTorrentInfo(torrent), completedRelativePath);
    rememberCompleted(record);
    downloadMeta.delete(gid);
    await saveDownloadState();

    // The record travels with the event so a client that never saw the transfer
    // can still name it; targetPath stays for consumers that only read that.
    broadcast({ type: 'download-complete', payload: { ...record, targetPath: completedRelativePath } });

    torrent.removeAllListeners();
    // destroyStore: false — the files are the whole point of the download.
    if (!torrent.destroyed) client.remove(torrent.infoHash, { destroyStore: false });
  });

  torrent.on('error', async (err) => {
    log.error('Torrent error', { gid, error: err.message });
    const interval = progressIntervals.get(gid);
    if (interval) { clearInterval(interval); progressIntervals.delete(gid); }
    downloadMeta.delete(gid);
    await saveDownloadState();
    torrent.removeAllListeners();
  });
}

// --- Core functions ---
async function addDownload(url, options = {}) {
  const c = ensureClient();
  let torrent;

  if (url.startsWith('magnet:')) {
    log.info('Adding magnet link', { magnet: url.substring(0, 60) });
    torrent = await new Promise((resolve, reject) => {
      const t = c.add(url, { path: options.dir }, (tor) => resolve(tor));
      t.on('error', reject);
      const timeout = setTimeout(
        () => reject(new Error('Magnet link metadata timeout — no peers available')),
        120_000,
      );
      t.on('metadata', () => clearTimeout(timeout));
    });
  } else if (url.startsWith('file://')) {
    const filePath = url.slice(7);
    log.info('Adding torrent file', { path: filePath });
    const buffer = await readFile(filePath);
    torrent = await new Promise((resolve, reject) => {
      const t = c.add(buffer, { path: options.dir }, (tor) => resolve(tor));
      t.on('error', reject);
    });
  } else {
    throw new Error(
      url.startsWith('http')
        ? 'HTTP/HTTPS downloads are not supported — use a magnet link or .torrent file'
        : `Unsupported URL format: ${url.substring(0, 40)}`,
    );
  }

  const gid = torrent.infoHash;

  if (torrent.private) {
    log.info('Private torrent — disabling DHT', { gid });
    if (torrent.discovery?.dht) {
      torrent.discovery.dht.destroy();
      torrent.discovery.dht = null;
    }
    setTimeout(() => {
      if (!torrent.destroyed && torrent.numPeers === 0) {
        log.warn('Private torrent has 0 peers after 15 s', { gid });
      }
    }, 15_000);
  }

  if (options.paused) {
    torrent.pause();
    log.info('Restored torrent in paused state', { gid });
  }

  startProgressInterval(torrent, gid);

  downloadMeta.set(gid, {
    url,
    dir: options.dir,
    relativePath: options.relativePath || '',
    addedAt: new Date().toISOString(),
  });

  setupTorrentListeners(torrent, gid);

  if (!options.skipSave) await saveDownloadState();

  log.info('Download added', { gid, name: torrent.name });
  return gid;
}

function getActiveDownloads(filterPath = null) {
  const c = ensureClient();
  return c.torrents
    .filter((t) => {
      if (t.paused) return false;
      if (filterPath != null) return (downloadMeta.get(t.infoHash)?.relativePath || '') === filterPath;
      return true;
    })
    .map(formatTorrentInfo);
}

function getWaitingDownloads(offset = 0, num = 100, filterPath = null) {
  const c = ensureClient();
  const webTorrentPaused = c.torrents
    .filter((t) => {
      if (!t.paused) return false;
      if (filterPath != null) return (downloadMeta.get(t.infoHash)?.relativePath || '') === filterPath;
      return true;
    })
    .map(formatTorrentInfo);

  let pausedEntries = Array.from(pausedDownloads.values());
  if (filterPath != null) pausedEntries = pausedEntries.filter((d) => (d.path || '') === filterPath);

  return [...webTorrentPaused, ...pausedEntries].slice(offset, offset + num);
}

function getCompletedDownloads(filterPath = null) {
  const entries = Array.from(completedDownloads.values());
  if (filterPath == null) return entries;
  return entries.filter((d) => (d.path || '') === filterPath);
}

function getDownloadStatus(gid) {
  const c = ensureClient();
  const torrent = c.torrents.find((t) => t.infoHash === gid);
  if (torrent) return formatTorrentInfo(torrent);
  return pausedDownloads.get(gid) || completedDownloads.get(gid) || null;
}

async function pauseDownload(gid) {
  const c = ensureClient();
  const torrent = c.torrents.find((t) => t.infoHash === gid);
  if (!torrent) throw new Error(`Download not found: ${gid}`);

  const downloadInfo = formatTorrentInfo(torrent);

  const interval = progressIntervals.get(gid);
  if (interval) { clearInterval(interval); progressIntervals.delete(gid); }

  if (!torrent.destroyed) torrent.removeAllListeners();
  if (!torrent.destroyed) client.remove(torrent.infoHash, { destroyStore: false });

  downloadInfo.status = 'paused';
  pausedDownloads.set(gid, downloadInfo);

  broadcast({ type: 'download-paused', payload: downloadInfo });
  await saveDownloadState();
  log.info('Download paused', { gid });
  return true;
}

async function resumeDownload(gid) {
  const c = ensureClient();
  let torrent = c.torrents.find((t) => t.infoHash === gid);

  if (!torrent) {
    const meta = downloadMeta.get(gid);
    if (!meta?.url) throw new Error(`Cannot resume: no metadata for ${gid}`);

    let addTarget = meta.url;
    if (meta.url.startsWith('file://')) addTarget = await readFile(meta.url.slice(7));

    torrent = await new Promise((resolve, reject) => {
      const timeoutId = setTimeout(() => reject(new Error('Timeout re-adding torrent')), 10_000);
      try {
        c.add(addTarget, { path: meta.dir }, (t) => {
          clearTimeout(timeoutId);
          if (!t || t.destroyed) reject(new Error('c.add() returned invalid torrent'));
          else resolve(t);
        });
      } catch (err) {
        clearTimeout(timeoutId);
        reject(err);
      }
    });

    await new Promise((r) => setTimeout(r, 500));
  }

  pausedDownloads.delete(gid);

  if (!progressIntervals.has(gid)) {
    startProgressInterval(torrent, gid);
    setupTorrentListeners(torrent, gid);
  }

  broadcast({ type: 'download-resumed', payload: formatTorrentInfo(torrent) });
  await saveDownloadState();
  log.info('Download resumed', { gid });
  return true;
}

/**
 * Delete the partial data an unfinished download left behind.
 *
 * WebTorrent writes to `${dir}/${name}`, but a torrent name comes from the
 * torrent itself, so the resolved path is checked to be strictly inside `dir`
 * before anything is unlinked — a crafted name must not reach out of the
 * download folder.
 */
async function deletePartialData(gid, name) {
  const dir = downloadMeta.get(gid)?.dir;
  if (!dir || !name) return false;

  const base = resolve(dir);
  const target = resolve(base, name);
  if (target === base || !target.startsWith(base + sep)) {
    log.warn('Refusing to delete outside the download folder', { gid, name });
    return false;
  }

  try {
    await rm(target, { recursive: true, force: true });
    log.info('Deleted partial download data', { gid, target });
    return true;
  } catch (err) {
    log.warn('Failed to delete partial download data', { gid, target, error: err.message });
    return false;
  }
}

/**
 * Remove a download.
 *
 * A finished download is only a history row, so dismissing it leaves the files
 * it produced alone. An unfinished one is deleted along with its partial data:
 * a half-downloaded torrent store is unusable once the torrent is gone.
 */
async function removeDownload(gid) {
  if (completedDownloads.has(gid)) {
    completedDownloads.delete(gid);
    downloadMeta.delete(gid);
    await saveDownloadState();
    log.info('Completed download dismissed, files kept', { gid });
    return { success: true, filesDeleted: false };
  }

  const c = ensureClient();
  const torrent = c.torrents.find((t) => t.infoHash === gid);

  // A paused download has already left the client, so its partial data has to be
  // unlinked by path rather than through the torrent's own store.
  if (!torrent) {
    if (!pausedDownloads.has(gid)) throw new Error(`Download not found: ${gid}`);
    const filesDeleted = await deletePartialData(gid, pausedDownloads.get(gid)?.name);
    pausedDownloads.delete(gid);
    downloadMeta.delete(gid);
    await saveDownloadState();
    log.info('Paused download removed', { gid, filesDeleted });
    return { success: true, filesDeleted };
  }

  const interval = progressIntervals.get(gid);
  if (interval) { clearInterval(interval); progressIntervals.delete(gid); }

  if (!torrent.destroyed) torrent.removeAllListeners();
  c.remove(gid, { destroyStore: true });
  downloadMeta.delete(gid);
  await saveDownloadState();

  log.info('Download removed', { gid, filesDeleted: true });
  return { success: true, filesDeleted: true };
}

/** Dismiss the whole history. Files are never touched — see removeDownload. */
async function clearCompletedDownloads() {
  const gids = Array.from(completedDownloads.keys());
  completedDownloads.clear();
  for (const gid of gids) downloadMeta.delete(gid);
  await saveDownloadState();

  // Reuses the per-download event clients already handle, so a cleared history
  // disappears everywhere without a bespoke message type.
  for (const gid of gids) broadcast({ type: 'download-removed', payload: { gid } });

  log.info('Cleared completed downloads', { count: gids.length });
  return gids.length;
}

async function testTrackerConnectivity(trackerUrl) {
  return new Promise((resolve) => {
    try {
      const url = new URL(trackerUrl);
      const protocol = url.protocol === 'https:' ? https : http;
      const request = protocol.get(
        trackerUrl,
        { headers: { 'User-Agent': 'qBittorrent/4.6.0' }, timeout: 10_000 },
        (response) => {
          let body = '';
          response.on('data', (chunk) => { body += chunk; });
          response.on('end', () => {
            resolve({
              success: response.statusCode === 200,
              statusCode: response.statusCode,
              statusMessage: response.statusMessage,
              body,
            });
          });
        },
      );
      request.on('error', (err) => resolve({ success: false, error: err.message, code: err.code }));
      request.on('timeout', () => { request.destroy(); resolve({ success: false, error: 'Connection timeout' }); });
    } catch (err) {
      resolve({ success: false, error: err.message });
    }
  });
}

// --- HTTP helpers ---
function sendJson(res, status, body) {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}

async function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => { data += chunk; });
    req.on('end', () => {
      try { resolve(JSON.parse(data)); } catch { resolve({}); }
    });
    req.on('error', reject);
  });
}

// --- HTTP server ---
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  const { pathname } = url;
  const { method } = req;

  try {
    // POST /downloads
    if (method === 'POST' && pathname === '/downloads') {
      const body = await readBody(req);
      if (!body.url) return sendJson(res, 400, { error: 'Missing url' });
      const gid = await addDownload(body.url, {
        dir: body.dir,
        relativePath: body.relativePath,
        paused: body.paused,
      });
      return sendJson(res, 200, { gid });
    }

    // GET /downloads/active
    if (method === 'GET' && pathname === '/downloads/active') {
      const filterPath = url.searchParams.has('path') ? url.searchParams.get('path') : null;
      return sendJson(res, 200, getActiveDownloads(filterPath));
    }

    // GET /downloads/waiting
    if (method === 'GET' && pathname === '/downloads/waiting') {
      const offset = parseInt(url.searchParams.get('offset') || '0', 10);
      const num = parseInt(url.searchParams.get('num') || '100', 10);
      const filterPath = url.searchParams.has('path') ? url.searchParams.get('path') : null;
      return sendJson(res, 200, getWaitingDownloads(offset, num, filterPath));
    }

    // GET /downloads/completed — must precede the /downloads/:gid match below,
    // which would otherwise treat "completed" as a gid.
    if (method === 'GET' && pathname === '/downloads/completed') {
      const filterPath = url.searchParams.has('path') ? url.searchParams.get('path') : null;
      return sendJson(res, 200, getCompletedDownloads(filterPath));
    }

    // DELETE /downloads/completed — same ordering constraint as the GET above.
    if (method === 'DELETE' && pathname === '/downloads/completed') {
      const cleared = await clearCompletedDownloads();
      return sendJson(res, 200, { success: true, cleared });
    }

    // GET /tracker/test
    if (method === 'GET' && pathname === '/tracker/test') {
      const trackerUrl = url.searchParams.get('url');
      if (!trackerUrl) return sendJson(res, 400, { error: 'Missing url query param' });
      return sendJson(res, 200, await testTrackerConnectivity(trackerUrl));
    }

    // Routes with :gid
    const gidMatch = pathname.match(/^\/downloads\/([^/]+)$/);
    if (gidMatch) {
      const gid = decodeURIComponent(gidMatch[1]);

      if (method === 'GET') {
        const status = getDownloadStatus(gid);
        return status
          ? sendJson(res, 200, status)
          : sendJson(res, 404, { error: 'Not found' });
      }

      if (method === 'PATCH') {
        const body = await readBody(req);
        if (body.action === 'pause') { await pauseDownload(gid); return sendJson(res, 200, { success: true }); }
        if (body.action === 'resume') { await resumeDownload(gid); return sendJson(res, 200, { success: true }); }
        return sendJson(res, 400, { error: 'action must be "pause" or "resume"' });
      }

      if (method === 'DELETE') {
        return sendJson(res, 200, await removeDownload(gid));
      }
    }

    sendJson(res, 404, { error: 'Not found' });
  } catch (err) {
    log.error('Request error', { path: pathname, error: err.message });
    sendJson(res, 500, { error: err.message });
  }
});

// WebSocket upgrade — only /events path
server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname !== '/events') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
    log.info('WebSocket client connected');
  });
});

wss.on('connection', (ws) => {
  // This service is the only holder of download state and otherwise says nothing
  // until the next progress tick, so a client that connects — including the main
  // app's bridge after a restart or a reconnect — is handed the current picture
  // straight away instead of inferring it from whatever happens next.
  try {
    ws.send(JSON.stringify({
      type: 'downloads-status',
      payload: {
        downloads: [...getActiveDownloads(), ...getWaitingDownloads(), ...getCompletedDownloads()],
        timestamp: new Date().toISOString(),
      },
    }));
  } catch (err) {
    log.warn('Failed to send state snapshot to new WebSocket client', { error: err.message });
  }

  ws.on('close', () => log.info('WebSocket client disconnected'));
});

// Graceful shutdown
process.on('SIGTERM', () => { if (client) client.destroy(); process.exit(0); });
process.on('SIGINT',  () => { if (client) client.destroy(); process.exit(0); });

// Start
server.listen(PORT, () => {
  log.info(`Torrent service listening`, { port: PORT });
  ensureClient();
  restoreDownloads().catch((err) => log.error('Failed to restore downloads on startup', { error: err.message }));
});
