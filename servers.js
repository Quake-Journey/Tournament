// servers.js
require('dotenv').config();
const https = require('https');

const SERVERS_API = (process.env.SERVERS_API || 'http://localhost:3001').replace(/\/$/, '');

/* ── GameTracker proxy helpers ── */
const gtCache = new Map();
const GT_TTL = 5 * 60 * 1000;

function gtFetch(url) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9',
      }
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    });
    req.on('error', reject);
    req.setTimeout(12000, () => { req.destroy(); reject(new Error('timeout')); });
  });
}

function gtParse(html, server) {
  const strip = s => s
    .replace(/<[^>]+>/g, ' ')
    .replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"')
    .replace(/&nbsp;/g, ' ').replace(/&#(\d+);/g, (_, n) => String.fromCharCode(+n))
    .replace(/\s+/g, ' ').trim();

  const result = { server, url: 'https://www.gametracker.com/server_info/' + server + '/' };

  // Server name — class="blocknewheadertitle"
  let m = html.match(/class="blocknewheadertitle">\s*([\s\S]*?)\s*<\/span>/i);
  result.name = m ? strip(m[1]) : server;

  // Status — class="item_color_success"
  m = html.match(/class="item_color_success">\s*([\s\S]*?)\s*<\/span>/i);
  result.status = m ? strip(m[1]) : '';

  // Rank
  m = html.match(/Game Server Rank:<\/span>\s*([\s\S]*?)<br/i);
  result.rank = m ? strip(m[1]) : '';
  m = html.match(/Highest \(past month\):<\/span>\s*([^\s<&]+)/i);
  if (m) result.rankHigh = m[1].trim();
  m = html.match(/Lowest \(past month\):<\/span>\s*([^\s<&]+)/i);
  if (m) result.rankLow = m[1].trim();

  // Current map — id="HTML_curr_map" and id="HTML_map_ss_img"
  m = html.match(/id="HTML_curr_map">\s*([\s\S]*?)\s*<\/div>/i);
  result.currentMap = m ? strip(m[1]) : '';
  m = html.match(/id="HTML_map_ss_img">\s*<img[^>]+src="([^"]+)"/i);
  if (m) result.mapImg = 'https:' + m[1].replace(/^https?:/, '');

  // Top 10 players — find "TOP 10 PLAYERS" section, then parse only that table
  const top10Idx = html.indexOf('TOP 10 PLAYERS');
  const players = [];
  if (top10Idx >= 0) {
    const tblStart = html.indexOf('<table', top10Idx);
    const tblEnd   = html.indexOf('</table>', tblStart) + 8;
    if (tblStart >= 0 && tblEnd > tblStart) {
      const tbl = html.slice(tblStart, tblEnd);
      const rowRe = /<tr[^>]*>([\s\S]*?)<\/tr>/gi;
      let row;
      while ((row = rowRe.exec(tbl)) !== null && players.length < 10) {
        const cells = [];
        const cellRe = /<td[^>]*>([\s\S]*?)<\/td>/gi;
        let cell;
        while ((cell = cellRe.exec(row[1])) !== null) cells.push(strip(cell[1]));
        if (cells.length >= 3 && /^\d+\.?$/.test(cells[0].trim())) {
          players.push({ rank: cells[0].replace('.',''), name: cells[1], score: cells[2], time: cells[3] || '' });
        }
      }
    }
  }
  result.players = players;

  // Charts via GSID
  m = html.match(/GSID=(\d+)/);
  if (m) {
    const g = m[1];
    result.charts = [
      { label: 'Игроки (24ч / 7д / 30д)',    url: 'https://cache.gametracker.com/images/graphs/server_players.php?GSID=' + g + '&start=-1d' },
      { label: 'Любимые карты (за неделю)',   url: 'https://cache.gametracker.com/images/graphs/server_maps.php?GSID=' + g },
      { label: 'Рейтинг сервера (30 дней)',   url: 'https://cache.gametracker.com/images/graphs/server_rank.php?GSID=' + g },
    ];
  } else {
    result.charts = [];
  }

  return result;
}

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function buildServersPage() {
  return `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Серверы</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html { overflow-x: clip; }

body {
  background: linear-gradient(140deg, #080d1a 0%, #0f1629 40%, #0a1020 70%, #111827 100%);
  color: #e2e8f0;
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  min-height: 100vh;
  overflow-x: hidden;
  width: 100%;
}

/* ── Header ── */
.srv-header {
  position: sticky;
  top: 0;
  z-index: 100;
  background: rgba(10, 16, 32, 0.9);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border-bottom: 1px solid rgba(99, 130, 255, 0.2);
  padding: 10px 24px;
  width: 100%;
  overflow: hidden;
}
.header-inner {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 1600px;
  width: 100%;
  margin: 0 auto;
}
.header-left {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-shrink: 0;
}
.back-link {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 32px; height: 32px;
  border-radius: 8px;
  background: rgba(99, 130, 255, 0.15);
  color: #93c5fd;
  text-decoration: none;
  font-size: 18px;
  transition: background 0.15s;
}
.back-link:hover { background: rgba(99, 130, 255, 0.3); }
.page-title {
  font-size: 17px;
  font-weight: 700;
  color: #e2e8f0;
  letter-spacing: 0.01em;
}
.page-badge {
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #64748b;
  background: rgba(99, 130, 255, 0.1);
  border: 1px solid rgba(99, 130, 255, 0.2);
  border-radius: 6px;
  padding: 2px 8px;
}

/* ── Controls ── */
.controls {
  display: flex;
  flex-direction: column;
  gap: 6px;
}
.ctrl-row {
  display: flex;
  align-items: center;
  gap: 14px;
  flex-wrap: wrap;
}
.ctrl-group {
  display: flex;
  align-items: center;
  gap: 7px;
}
.ctrl-label {
  font-size: 13px;
  color: #94a3b8;
  white-space: nowrap;
  cursor: pointer;
}
.ctrl-select {
  background: rgba(15, 23, 42, 0.9);
  border: 1px solid rgba(99, 130, 255, 0.3);
  border-radius: 8px;
  color: #cbd5e1;
  padding: 5px 10px;
  font-size: 13px;
  cursor: pointer;
  outline: none;
  transition: border-color 0.15s;
}
.ctrl-select:focus { border-color: rgba(99, 130, 255, 0.7); }
.ctrl-input {
  background: rgba(15, 23, 42, 0.9);
  border: 1px solid rgba(99, 130, 255, 0.3);
  border-radius: 8px;
  color: #cbd5e1;
  padding: 5px 10px;
  font-size: 13px;
  outline: none;
  transition: border-color 0.15s;
  width: 150px;
}
.ctrl-input::placeholder { color: #475569; }
.ctrl-input:focus { border-color: rgba(99, 130, 255, 0.7); }
.ctrl-checkbox {
  appearance: none;
  -webkit-appearance: none;
  width: 16px; height: 16px;
  border: 1px solid rgba(99, 130, 255, 0.4);
  border-radius: 4px;
  background: rgba(15, 23, 42, 0.9);
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
  flex-shrink: 0;
  transition: border-color 0.15s, background 0.15s;
}
.ctrl-checkbox:checked {
  background: rgba(99, 130, 255, 0.3);
  border-color: rgba(99, 130, 255, 0.8);
}
.ctrl-checkbox:checked::after {
  content: '✓';
  position: absolute;
  color: #93c5fd;
  font-size: 11px;
  font-weight: 700;
  line-height: 1;
}
.ctrl-btn {
  background: rgba(99, 130, 255, 0.15);
  border: 1px solid rgba(99, 130, 255, 0.3);
  border-radius: 8px;
  color: #93c5fd;
  padding: 5px 14px;
  font-size: 13px;
  cursor: pointer;
  transition: background 0.15s, border-color 0.15s;
  white-space: nowrap;
}
.ctrl-btn:hover {
  background: rgba(99, 130, 255, 0.28);
  border-color: rgba(99, 130, 255, 0.6);
}
.ctrl-btn-save {
  background: rgba(34, 197, 94, 0.12);
  border: 1px solid rgba(34, 197, 94, 0.3);
  color: #86efac;
}
.ctrl-btn-save:hover {
  background: rgba(34, 197, 94, 0.25);
  border-color: rgba(34, 197, 94, 0.6);
}
.ctrl-sep {
  width: 1px;
  height: 20px;
  background: rgba(99, 130, 255, 0.15);
  flex-shrink: 0;
}

/* ── Status bar ── */
.status-bar {
  font-size: 12px;
  color: #64748b;
  white-space: nowrap;
}
.status-dot {
  display: inline-block;
  width: 7px; height: 7px;
  border-radius: 50%;
  background: #64748b;
  margin-right: 5px;
  vertical-align: middle;
  transition: background 0.3s;
}
.status-dot.ok { background: #22c55e; }
.status-dot.err { background: #ef4444; }
.status-dot.loading { background: #f59e0b; animation: pulse 1s infinite; }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

/* ── Main ── */
.srv-main {
  padding: 24px 20px 48px;
  max-width: 1400px;
  margin: 0 auto;
  transition: max-width 0.2s;
}
.srv-main.full-width {
  max-width: none;
}

.no-servers {
  text-align: center;
  padding: 60px 20px;
  color: #475569;
  font-size: 15px;
}
.no-servers span { font-size: 40px; display: block; margin-bottom: 12px; }

.error-box {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: 10px;
  padding: 16px 20px;
  color: #fca5a5;
  font-size: 14px;
  margin-bottom: 16px;
}

/* ── Server grid ── */
.servers-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
  gap: 16px;
}

/* ── Server card ── */
.srv-card {
  background: rgba(15, 23, 42, 0.7);
  border: 1px solid rgba(99, 130, 255, 0.15);
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 4px 20px rgba(0,0,0,0.35);
  transition: border-color 0.15s, box-shadow 0.15s;
}
.srv-card:hover {
  border-color: rgba(99, 130, 255, 0.35);
  box-shadow: 0 6px 28px rgba(0,0,0,0.45);
}

.srv-card-head {
  padding: 12px 16px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  background: rgba(99, 130, 255, 0.06);
}
.srv-card-title {
  font-size: 14px;
  font-weight: 600;
  color: #e2e8f0;
  word-break: break-all;
  margin-bottom: 4px;
}
.srv-card-meta {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}
.srv-badge {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  border-radius: 5px;
  padding: 2px 7px;
  white-space: nowrap;
}
.badge-q2 {
  background: rgba(59, 130, 246, 0.2);
  color: #93c5fd;
  border: 1px solid rgba(59, 130, 246, 0.35);
}
.badge-qw {
  background: rgba(168, 85, 247, 0.2);
  color: #d8b4fe;
  border: 1px solid rgba(168, 85, 247, 0.35);
}
.srv-addr {
  font-size: 12px;
  color: #64748b;
  font-family: monospace;
}
.srv-addr-sep {
  font-size: 11px;
  color: #334155;
  margin: 0 2px;
}
.srv-map {
  font-size: 12px;
  color: #94a3b8;
}
.srv-map-icon { margin-right: 3px; }

.srv-card-body { padding: 10px 16px 14px; }

.players-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 8px;
}
.players-title {
  font-size: 12px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #64748b;
}
.players-count {
  font-size: 12px;
  font-weight: 700;
  color: #93c5fd;
  background: rgba(99, 130, 255, 0.1);
  border: 1px solid rgba(99, 130, 255, 0.2);
  border-radius: 10px;
  padding: 1px 8px;
}

.players-table {
  width: 100%;
  border-collapse: collapse;
}
.players-table th {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  color: #475569;
  padding: 3px 6px;
  text-align: left;
  border-bottom: 1px solid rgba(255,255,255,0.05);
}
.players-table th.right, .players-table td.right { text-align: right; }
.players-table td {
  font-size: 13px;
  color: #cbd5e1;
  padding: 4px 6px;
  border-bottom: 1px solid rgba(255,255,255,0.03);
}
.players-table tr:last-child td { border-bottom: none; }
.players-table tr:hover td { background: rgba(99, 130, 255, 0.05); }
.player-name { word-break: break-all; }
.ping-val { color: #94a3b8; font-family: monospace; }
.ping-low { color: #22c55e; }
.ping-mid { color: #f59e0b; }
.ping-high { color: #ef4444; }
.score-val { color: #93c5fd; font-family: monospace; font-weight: 600; }
.score-trend-col { width: 1px; white-space: nowrap; padding: 0 6px 0 4px !important; }
.score-trend { font-size: 11px; line-height: 1; display: inline-flex; align-items: center; gap: 4px; }
.score-trend-arrow { font-size: 10px; opacity: 0.95; }
.score-trend-delta { font-family: monospace; font-weight: 700; font-size: 11px; }
.score-trend.up   .score-trend-arrow { color: #4ade80; }
.score-trend.up   .score-trend-delta { color: #bbf7d0; }
.score-trend.down .score-trend-arrow { color: #f87171; }
.score-trend.down .score-trend-delta { color: #fecaca; }

.no-players {
  font-size: 13px;
  color: #475569;
  font-style: italic;
  padding: 4px 0;
}

/* ── Server mod & details ── */
.srv-mod {
  font-size: 12px;
  color: #a78bfa;
  margin-top: 3px;
}
.srv-mod-icon { margin-right: 3px; }
.srv-details {
  margin-top: 8px;
  border: 1px solid rgba(99, 130, 255, 0.12);
  border-radius: 8px;
  overflow: hidden;
}
.srv-details summary {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #64748b;
  padding: 5px 10px;
  cursor: pointer;
  user-select: none;
  background: rgba(99, 130, 255, 0.04);
  list-style: none;
  display: flex;
  align-items: center;
  gap: 5px;
}
.srv-details summary::-webkit-details-marker { display: none; }
.srv-details summary::before {
  content: '+';
  font-size: 13px;
  color: #93c5fd;
  width: 14px;
  text-align: center;
  flex-shrink: 0;
}
.srv-details[open] summary::before { content: '−'; }
.srv-details summary:hover { background: rgba(99, 130, 255, 0.1); color: #94a3b8; }
.srv-details-body {
  padding: 8px 10px;
  display: grid;
  grid-template-columns: auto 1fr;
  gap: 2px 10px;
}
.srv-kv-key {
  font-size: 11px;
  color: #64748b;
  white-space: nowrap;
  font-family: monospace;
  padding: 1px 0;
}
.srv-kv-val {
  font-size: 11px;
  color: #94a3b8;
  word-break: break-all;
  font-family: monospace;
  padding: 1px 0;
}

/* ── Q2TV toggle button in controls ── */
.ctrl-btn-q2tv-off, .ctrl-btn-q2tv-on {
  padding: 3px 10px;
  border-radius: 5px;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  border: 1px solid;
  transition: background 0.15s, border-color 0.15s;
  white-space: nowrap;
}
.ctrl-btn-q2tv-off {
  background: rgba(139, 92, 246, 0.1);
  border-color: rgba(139, 92, 246, 0.3);
  color: #a78bfa;
}
.ctrl-btn-q2tv-off:hover {
  background: rgba(139, 92, 246, 0.2);
  border-color: rgba(139, 92, 246, 0.5);
}
.ctrl-btn-q2tv-on {
  background: rgba(139, 92, 246, 0.3);
  border-color: rgba(139, 92, 246, 0.7);
  color: #ddd6fe;
}
.ctrl-btn-q2tv-on:hover {
  background: rgba(139, 92, 246, 0.15);
  border-color: rgba(139, 92, 246, 0.4);
  color: #a78bfa;
}

/* ── Q2TV button in server card (скрыта по умолчанию) ── */
.btn-q2tv {
  display: none;
}
.q2tv-enabled .btn-q2tv {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  margin-top: 10px;
  padding: 5px 12px;
  background: rgba(139, 92, 246, 0.15);
  border: 1px solid rgba(139, 92, 246, 0.35);
  border-radius: 6px;
  color: #c4b5fd;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  cursor: pointer;
  transition: background 0.15s, border-color 0.15s;
}
.btn-q2tv:hover {
  background: rgba(139, 92, 246, 0.28);
  border-color: rgba(139, 92, 246, 0.6);
  color: #ddd6fe;
}

/* ── Local client button in controls (desktop only) ── */
.ctrl-btn-local {
  padding: 3px 10px;
  border-radius: 5px;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  border: 1px solid rgba(52, 211, 153, 0.3);
  background: rgba(52, 211, 153, 0.08);
  color: #6ee7b7;
  transition: background 0.15s, border-color 0.15s;
  white-space: nowrap;
}
.ctrl-btn-local:hover {
  background: rgba(52, 211, 153, 0.18);
  border-color: rgba(52, 211, 153, 0.5);
}
.ctrl-btn-local.configured {
  background: rgba(52, 211, 153, 0.2);
  border-color: rgba(52, 211, 153, 0.55);
  color: #a7f3d0;
}
@media (max-width: 767px) {
  .ctrl-group-local { display: none !important; }
  .btn-local { display: none !important; }
}

/* ── Local connect button in server card ── */
.btn-local {
  display: none;
}
.local-enabled .btn-local {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  margin-top: 10px;
  margin-left: 6px;
  padding: 5px 12px;
  background: rgba(52, 211, 153, 0.1);
  border: 1px solid rgba(52, 211, 153, 0.28);
  border-radius: 6px;
  color: #6ee7b7;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  cursor: pointer;
  transition: background 0.15s, border-color 0.15s;
}
.local-enabled .btn-local:hover {
  background: rgba(52, 211, 153, 0.22);
  border-color: rgba(52, 211, 153, 0.55);
  color: #a7f3d0;
}

/* ── Theme selector control ── */
.ctrl-theme-select {
  background: rgba(15, 23, 42, 0.9);
  border: 1px solid rgba(99, 130, 255, 0.3);
  border-radius: 8px;
  color: #cbd5e1;
  padding: 5px 10px;
  font-size: 13px;
  cursor: pointer;
  outline: none;
  transition: border-color 0.15s;
}
.ctrl-theme-select:focus { border-color: rgba(99, 130, 255, 0.7); }

/* ── Light theme ── */
body.theme-light {
  background: linear-gradient(140deg, #e8edf5 0%, #dce4f0 40%, #e4eaf5 70%, #edf1f8 100%);
  color: #0f172a;
}
body.theme-light .srv-header {
  background: rgba(255, 255, 255, 0.92);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border-bottom: 1px solid #c7d2e8;
}
body.theme-light .page-title { color: #0f172a; }
body.theme-light .page-badge { color: #334155; background: #e0e7ff; border-color: #a5b4fc; font-weight: 700; }
body.theme-light .back-link { color: #fff; background: #3b82f6; }
body.theme-light .back-link:hover { background: #2563eb; }
body.theme-light .ctrl-label { color: #334155; font-weight: 500; }
body.theme-light .ctrl-select,
body.theme-light .ctrl-input,
body.theme-light .ctrl-theme-select {
  background: #fff;
  border: 1px solid #94a3b8;
  color: #0f172a;
}
body.theme-light .ctrl-select:focus,
body.theme-light .ctrl-input:focus,
body.theme-light .ctrl-theme-select:focus { border-color: #3b82f6; }
body.theme-light .ctrl-input::placeholder { color: #94a3b8; }
body.theme-light .ctrl-checkbox { background: #fff; border: 1px solid #94a3b8; }
body.theme-light .ctrl-checkbox:checked { background: #3b82f6; border-color: #2563eb; }
body.theme-light .ctrl-checkbox:checked::after { color: #fff; }
body.theme-light .ctrl-btn { background: #e0e7ff; border: 1px solid #a5b4fc; color: #1d4ed8; font-weight: 600; }
body.theme-light .ctrl-btn:hover { background: #c7d2fe; border-color: #818cf8; }
body.theme-light .ctrl-btn-save { background: #dcfce7; border: 1px solid #86efac; color: #15803d; font-weight: 600; }
body.theme-light .ctrl-btn-save:hover { background: #bbf7d0; border-color: #4ade80; }
body.theme-light .ctrl-sep { background: #c7d2e8; }
body.theme-light .status-bar { color: #475569; }
body.theme-light .status-dot { background: #94a3b8; }
body.theme-light .status-dot.ok { background: #16a34a; }
body.theme-light .status-dot.err { background: #dc2626; }
body.theme-light .status-dot.loading { background: #d97706; }
body.theme-light .srv-card {
  background: #ffffff;
  border: 1px solid #cbd5e1;
  box-shadow: 0 2px 8px rgba(15,23,42,0.08), 0 1px 2px rgba(15,23,42,0.06);
}
body.theme-light .srv-card:hover {
  border-color: #93c5fd;
  box-shadow: 0 6px 20px rgba(59,130,246,0.15), 0 2px 6px rgba(15,23,42,0.08);
}
body.theme-light .srv-card-head { background: #f1f5f9; border-bottom: 1px solid #e2e8f0; }
body.theme-light .srv-card-title { color: #0f172a; font-weight: 700; }
body.theme-light .srv-addr { color: #475569; }
body.theme-light .badge-q2 { background: #dbeafe; color: #1d4ed8; border: 1px solid #93c5fd; font-weight: 700; }
body.theme-light .badge-qw { background: #ede9fe; color: #6d28d9; border: 1px solid #c4b5fd; font-weight: 700; }
body.theme-light .no-servers { color: #64748b; }
body.theme-light .error-box { background: #fef2f2; border: 1px solid #fca5a5; color: #b91c1c; }
body.theme-light .srv-footer { border-top: 1px solid #cbd5e1; color: #475569; }
body.theme-light .srv-footer a { color: #2563eb; }
/* Панель статистики */
body.theme-light .srv-stats { background: #f1f5f9; border-color: #cbd5e1; }
body.theme-light .stat-val { color: #1d4ed8; }
body.theme-light .stat-key { color: #64748b; }
body.theme-light .stats-details { border-top-color: #e2e8f0; }
body.theme-light .stat-detail-item { color: #475569; }
body.theme-light .stat-detail-item b { color: #0f172a; }
body.theme-light .stat-detail-label { color: #64748b; }
body.theme-light .hint-icon { background: #e0e7ff; color: #1d4ed8; border: 1px solid #a5b4fc; }
body.theme-light .ctrl-btn-local { background: #d1fae5; border: 1px solid #6ee7b7; color: #065f46; font-weight: 600; }
body.theme-light .ctrl-btn-local:hover { background: #a7f3d0; border-color: #34d399; }
body.theme-light.local-enabled .btn-local { background: #059669; border: 1px solid #047857; color: #fff; font-weight: 700; }
body.theme-light.local-enabled .btn-local:hover { background: #047857; border-color: #065f46; }
/* Мод / карта */
body.theme-light .srv-mod { color: #7c3aed; }
body.theme-light .srv-addr { color: #334155; }
/* Секция НАСТРОЙКИ СЕРВЕРА */
body.theme-light .srv-details { border-color: #cbd5e1; }
body.theme-light .srv-details summary { background: #f1f5f9; color: #334155; }
body.theme-light .srv-details summary::before { color: #2563eb; }
body.theme-light .srv-details summary:hover { background: #e2e8f0; color: #0f172a; }
body.theme-light .srv-kv-key { color: #475569; }
body.theme-light .srv-kv-val { color: #0f172a; }
/* GTi кнопка */
body.theme-light .btn-gti { background: #fef3c7; border-color: #fbbf24; color: #92400e; }
body.theme-light .btn-gti:hover { background: #fde68a; border-color: #f59e0b; }
body.theme-light .btn-gti .gti-i { color: #1d4ed8; }
/* Q2TV кнопки в контролах */
body.theme-light .ctrl-btn-q2tv-off { background: #ede9fe; border-color: #c4b5fd; color: #5b21b6; }
body.theme-light .ctrl-btn-q2tv-off:hover { background: #ddd6fe; border-color: #a78bfa; }
body.theme-light .ctrl-btn-q2tv-on { background: #7c3aed; border-color: #6d28d9; color: #fff; }
body.theme-light .ctrl-btn-q2tv-on:hover { background: #6d28d9; border-color: #5b21b6; color: #fff; }
/* Q2TV кнопка в карточке сервера */
body.theme-light.q2tv-enabled .btn-q2tv { background: #7c3aed; border: 1px solid #6d28d9; color: #fff; font-weight: 700; }
body.theme-light .btn-q2tv:hover { background: #6d28d9; border-color: #5b21b6; color: #fff; }
/* GTi диалог */
body.theme-light #gt-box { background: #ffffff; border-color: #fbbf24; box-shadow: 0 20px 60px rgba(0,0,0,0.2); }
body.theme-light #gt-header { background: #fefce8; border-bottom-color: #fde68a; }
body.theme-light #gt-title { color: #92400e; }
body.theme-light #gt-expand, body.theme-light #gt-close { color: #64748b; }
body.theme-light #gt-expand:hover { color: #b45309; background: #fef3c7; }
body.theme-light #gt-close:hover { color: #dc2626; background: #fee2e2; }
body.theme-light #gt-body { background: #fff; color: #0f172a; }
body.theme-light .gt-link { color: #1d4ed8; }
body.theme-light .gt-section-title { color: #92400e; border-bottom-color: #fde68a; }
body.theme-light .gt-info-key { color: #475569; }
body.theme-light .gt-info-val { color: #0f172a; }
body.theme-light .gt-status-alive { color: #15803d; }
body.theme-light .gt-status-dead  { color: #dc2626; }
body.theme-light .gt-rank-val { color: #92400e; }
body.theme-light .gt-players-table th { color: #334155; border-bottom-color: #e2e8f0; }
body.theme-light .gt-players-table td { color: #0f172a; border-bottom-color: #f1f5f9; }
body.theme-light .gt-players-table tr:hover td { background: #f8fafc; }
body.theme-light .gt-rank-cell  { color: #64748b; }
body.theme-light .gt-score-cell { color: #b45309; }
body.theme-light .gt-time-cell  { color: #1d4ed8; }
body.theme-light .gt-chart-wrap { background: #f8fafc; border-color: #e2e8f0; }
body.theme-light .gt-chart-label { color: #475569; }
body.theme-light .gt-map-img { border-color: #e2e8f0; }
body.theme-light .gt-map-name { color: #0f172a; }
body.theme-light .gt-loading { color: #94a3b8; }
body.theme-light .gt-error   { color: #dc2626; }
/* Q2TV предупреждение (диалог "Включить Q2TV") */
body.theme-light #q2tv-warn-box { background: #ffffff; border-color: #c4b5fd; color: #0f172a; box-shadow: 0 16px 60px rgba(0,0,0,0.2); }
body.theme-light #q2tv-warn-title { color: #5b21b6; }
body.theme-light .q2tv-warn-section { color: #5b21b6; }
body.theme-light #q2tv-warn-body code { background: #f1f5f9; color: #1e293b; }
body.theme-light #q2tv-warn-yes { background: #7c3aed; border-color: #6d28d9; color: #fff; }
body.theme-light #q2tv-warn-yes:hover { background: #6d28d9; }
body.theme-light #q2tv-warn-no { background: #f1f5f9; border-color: #cbd5e1; color: #334155; }
body.theme-light #q2tv-warn-no:hover { background: #e2e8f0; }
/* Диалог "Локальный Q2 клиент" */
body.theme-light #local-modal-box { background: #ffffff; border-color: #6ee7b7; color: #0f172a; box-shadow: 0 16px 60px rgba(0,0,0,0.2); }
body.theme-light #local-modal-title { color: #065f46; }
body.theme-light #local-modal-close { color: #64748b; }
body.theme-light #local-modal-close:hover { color: #dc2626; }
body.theme-light .local-modal-label { color: #334155; }
body.theme-light #local-modal-path { background: #f8fafc; border-color: #94a3b8; color: #0f172a; }
body.theme-light #local-modal-path:focus { border-color: #34d399; }
body.theme-light .local-modal-hint { color: #475569; }
body.theme-light .local-modal-hint b { color: #0f172a; }
body.theme-light #local-modal-clear { border-color: #cbd5e1; color: #64748b; }
body.theme-light #local-modal-clear:hover { color: #dc2626; border-color: #fca5a5; }
body.theme-light #local-modal-dl { background: #d1fae5; border-color: #6ee7b7; color: #065f46; }
body.theme-light #local-modal-dl:hover { background: #a7f3d0; }
body.theme-light #local-modal-save { background: #059669; border-color: #047857; color: #fff; }
body.theme-light #local-modal-save:hover { background: #047857; }
body.theme-light .players-title { color: #334155; }
body.theme-light .players-count { color: #1d4ed8; background: #dbeafe; border-color: #93c5fd; }
body.theme-light .players-table th { color: #334155; border-bottom: 1px solid #e2e8f0; }
body.theme-light .players-table td { color: #0f172a; border-bottom: 1px solid #f1f5f9; }
body.theme-light .players-table tr:hover td { background: #f0f7ff; }
body.theme-light .score-val { color: #1d4ed8; }
body.theme-light .score-trend.up   .score-trend-arrow { color: #16a34a; }
body.theme-light .score-trend.up   .score-trend-delta { color: #166534; }
body.theme-light .score-trend.down .score-trend-arrow { color: #dc2626; }
body.theme-light .score-trend.down .score-trend-delta { color: #991b1b; }
body.theme-light .ping-val { color: #475569; }
body.theme-light .ping-low { color: #15803d; }
body.theme-light .ping-mid { color: #b45309; }
body.theme-light .ping-high { color: #dc2626; }
body.theme-light .no-players { color: #64748b; }
body.theme-light .srv-card-body { color: #0f172a; }

/* ── Q2CSS theme (ретро Quake-стиль) ── */
body.theme-q2css {
  background: #FEF1DE;
  color: #000;
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
}
body.theme-q2css .srv-header { background: #FEF1DE; border-bottom: 2px solid #A22C21; }
body.theme-q2css .page-title { color: #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight: 700; }
body.theme-q2css .page-badge { color: #fff; background: #A22C21; border-color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .back-link { color: #A22C21; background: #FEF1DE; border: 1px solid #000; border-radius: 0; }
body.theme-q2css .back-link:hover { background: #FAD3BC; }
body.theme-q2css .ctrl-label { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .ctrl-select,
body.theme-q2css .ctrl-input,
body.theme-q2css .ctrl-theme-select {
  background: #FEF1DE;
  border: 1px solid #000;
  color: #000;
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  border-radius: 0;
}
body.theme-q2css option { background: #FEF1DE; color: #000; }
body.theme-q2css .ctrl-checkbox { background: #FEF1DE; border-color: #000; border-radius: 0; }
body.theme-q2css .ctrl-checkbox:checked { background: #A22C21; border-color: #A22C21; }
body.theme-q2css .ctrl-checkbox:checked::after { color: #fff; }
body.theme-q2css .ctrl-btn { background: #FEF1DE; border: 1px solid #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .ctrl-btn:hover { background: #FAD3BC; border-color: #A22C21; }
body.theme-q2css .ctrl-btn-save { background: #FEF1DE; border-color: #000; color: #A22C21; border-radius: 0; }
body.theme-q2css .ctrl-btn-save:hover { background: #FAD3BC; }
body.theme-q2css .ctrl-sep { background: #A22C21; opacity: 0.35; }
body.theme-q2css .status-bar { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .status-dot { background: #A22C21; }
body.theme-q2css .status-dot.ok { background: #006600; }
body.theme-q2css .status-dot.err { background: #A22C21; }
body.theme-q2css .status-dot.loading { background: #A22C21; }
body.theme-q2css .srv-card { background: #FEF1DE; border: 1px solid #000; border-radius: 0; box-shadow: none; }
body.theme-q2css .srv-card:hover { border-color: #A22C21; box-shadow: none; }
body.theme-q2css .srv-card-head { background: #F5C4B0; border-bottom: 1px solid #A22C21; }
body.theme-q2css .srv-card-title { color: #5C1A12; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 12px; font-weight: 700; }
body.theme-q2css .srv-addr { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .badge-q2 { background: #FAD3BC; color: #000; border-color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .badge-qw { background: #FEECD3; color: #A22C21; border-color: #A22C21; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .no-servers { color: #3D3D3D; }
body.theme-q2css .error-box { background: #FEECD3; border-color: #A22C21; color: #A22C21; border-radius: 0; }
body.theme-q2css .srv-footer { border-top: 1px solid #000; color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .srv-footer a { color: #A22C21; }
/* Панель статистики */
body.theme-q2css .srv-stats { background: #FEECD3; border: 1px solid #000; border-radius: 0; }
body.theme-q2css .stat-val { color: #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight: 700; }
body.theme-q2css .stat-key { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .stats-details { border-top-color: #000; }
body.theme-q2css .stat-detail-item { color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .stat-detail-item b { color: #A22C21; }
body.theme-q2css .stat-detail-label { color: #3D3D3D; }
body.theme-q2css .hint-icon { background: #FAD3BC; color: #000; border-color: #000; border-radius: 0; }
body.theme-q2css .ctrl-btn-local { background: #FEF1DE; border: 1px solid #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight: 700; }
body.theme-q2css .ctrl-btn-local:hover { background: #FAD3BC; border-color: #A22C21; color: #A22C21; }
body.theme-q2css.local-enabled .btn-local { background: #FAD3BC; border: 1px solid #A22C21; color: #A22C21; border-radius: 0; font-weight: 700; }
/* Мод / карта */
body.theme-q2css .srv-mod { color: #A22C21; }
/* Секция НАСТРОЙКИ СЕРВЕРА */
body.theme-q2css .srv-details { border: 1px solid #000; border-radius: 0; }
body.theme-q2css .srv-details summary { background: #FEECD3; color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .srv-details summary::before { color: #A22C21; }
body.theme-q2css .srv-details summary:hover { background: #FAD3BC; color: #000; }
body.theme-q2css .srv-kv-key { color: #3D3D3D; font-size: 11px; }
body.theme-q2css .srv-kv-val { color: #000; font-size: 11px; }
/* Q2TV кнопки в контролах */
body.theme-q2css .ctrl-btn-q2tv-off { background: #FEF1DE; border: 1px solid #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .ctrl-btn-q2tv-off:hover { background: #FAD3BC; border-color: #A22C21; }
body.theme-q2css .ctrl-btn-q2tv-on { background: #FAD3BC; border: 1px solid #A22C21; color: #A22C21; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .ctrl-btn-q2tv-on:hover { background: #FEECD3; }
/* Q2TV кнопка в карточке */
body.theme-q2css.q2tv-enabled .btn-q2tv { background: #FAD3BC; border: 1px solid #A22C21; color: #A22C21; border-radius: 0; font-weight: 700; }
body.theme-q2css .btn-q2tv:hover { background: #FAD3BC; border-color: #A22C21; }
/* GTi кнопка */
body.theme-q2css .btn-gti { background: #FEECD3; border-color: #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .btn-gti:hover { background: #FAD3BC; border-color: #A22C21; }
body.theme-q2css .btn-gti .gti-i { color: #A22C21; font-style: normal; }
/* GTi диалог */
body.theme-q2css #gt-box { background: #FEF1DE; border: 1px solid #A22C21; border-radius: 0; box-shadow: none; }
body.theme-q2css #gt-header { background: #F5C4B0; border-bottom: 1px solid #A22C21; }
body.theme-q2css #gt-title { color: #5C1A12; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #gt-expand, body.theme-q2css #gt-close { color: #5C1A12; }
body.theme-q2css #gt-expand:hover { color: #A22C21; background: rgba(162,44,33,0.1); }
body.theme-q2css #gt-close:hover { color: #A22C21; background: rgba(162,44,33,0.1); }
body.theme-q2css #gt-body { background: #FEF1DE; color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .gt-link { color: #A22C21; }
body.theme-q2css .gt-section-title { color: #A22C21; border-bottom: 1px solid #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .gt-info-key { color: #3D3D3D; }
body.theme-q2css .gt-info-val { color: #000; }
body.theme-q2css .gt-status-alive { color: #006600; }
body.theme-q2css .gt-status-dead  { color: #A22C21; }
body.theme-q2css .gt-rank-val { color: #000; }
body.theme-q2css .gt-players-table th { color: #5C1A12; background: #F5C4B0; border-bottom: 1px solid #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .gt-players-table td { color: #000; border-bottom: 1px solid #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .gt-players-table tr:hover td { background: #FAD3BC; }
body.theme-q2css .gt-rank-cell  { color: #3D3D3D; }
body.theme-q2css .gt-score-cell { color: #A22C21; font-weight: 700; }
body.theme-q2css .gt-time-cell  { color: #3D3D3D; }
body.theme-q2css .gt-chart-wrap { background: #FEECD3; border: 1px solid #000; border-radius: 0; }
body.theme-q2css .gt-chart-label { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .gt-map-img { border: 1px solid #000; border-radius: 0; }
body.theme-q2css .gt-map-name { color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .gt-loading { color: #3D3D3D; }
body.theme-q2css .gt-error   { color: #A22C21; }
/* Q2TV предупреждение (диалог "Включить Q2TV") */
body.theme-q2css #q2tv-warn-box { background: #FEF1DE; border: 1px solid #000; border-radius: 0; color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; box-shadow: none; }
body.theme-q2css #q2tv-warn-title { color: #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .q2tv-warn-section { color: #A22C21; }
body.theme-q2css #q2tv-warn-body code { background: #FEECD3; color: #000; border-radius: 0; }
body.theme-q2css #q2tv-warn-yes { background: #C0392B; border: 1px solid #A22C21; color: #fff; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #q2tv-warn-yes:hover { background: #A22C21; }
body.theme-q2css #q2tv-warn-no { background: #FEF1DE; border: 1px solid #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #q2tv-warn-no:hover { background: #FAD3BC; border-color: #A22C21; }
/* Диалог "Локальный Q2 клиент" */
body.theme-q2css #local-modal-box { background: #FEF1DE; border: 1px solid #000; border-radius: 0; color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; box-shadow: none; }
body.theme-q2css #local-modal-title { color: #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #local-modal-close { color: #3D3D3D; }
body.theme-q2css #local-modal-close:hover { color: #A22C21; }
body.theme-q2css .local-modal-label { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css #local-modal-path { background: #fff; border: 1px solid #000; border-radius: 0; color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #local-modal-path:focus { border-color: #A22C21; }
body.theme-q2css .local-modal-hint { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
body.theme-q2css .local-modal-hint b { color: #000; }
body.theme-q2css #local-modal-clear { background: #FEF1DE; border: 1px solid #000; color: #3D3D3D; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #local-modal-clear:hover { color: #A22C21; border-color: #A22C21; }
body.theme-q2css #local-modal-dl { background: #FEECD3; border: 1px solid #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #local-modal-dl:hover { background: #FAD3BC; border-color: #A22C21; }
body.theme-q2css #local-modal-save { background: #C0392B; border: 1px solid #A22C21; color: #fff; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight: 600; }
body.theme-q2css #local-modal-save:hover { background: #A22C21; }

/* ── Darkness theme (тёмная с тёплыми красно-оранжевыми акцентами) ── */
body.theme-darkness {
  background: linear-gradient(140deg, #0e0e0e 0%, #161616 40%, #111111 70%, #1a1a1a 100%);
  color: #d8d8d8;
}
body.theme-darkness .srv-header {
  background: rgba(18, 18, 18, 0.9);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border-bottom: 1px solid #3a3a3a;
}
body.theme-darkness .page-title { color: #e0e0e0; }
body.theme-darkness .page-badge { color: #f0d8d2; background: linear-gradient(to bottom, #8a3328, #5a1f18); border-color: #5a1f18; font-weight: 700; }
body.theme-darkness .back-link { color: #ff8a65; background: rgba(255, 90, 60, 0.18); }
body.theme-darkness .back-link:hover { background: rgba(255, 90, 60, 0.32); color: #ffaa85; }
body.theme-darkness .ctrl-label { color: #bdbdbd; font-weight: 500; }
body.theme-darkness .ctrl-select,
body.theme-darkness .ctrl-input,
body.theme-darkness .ctrl-theme-select {
  background: #1e1e1e;
  border: 1px solid #3a3a3a;
  color: #e0e0e0;
}
body.theme-darkness .ctrl-select:focus,
body.theme-darkness .ctrl-input:focus,
body.theme-darkness .ctrl-theme-select:focus { border-color: #ff5a3c; }
body.theme-darkness .ctrl-input::placeholder { color: #6a6a6a; }
body.theme-darkness option { background: #1b1b1b; color: #e0e0e0; }
body.theme-darkness .ctrl-checkbox { background: #1e1e1e; border: 1px solid #3a3a3a; }
body.theme-darkness .ctrl-checkbox:checked { background: #c84b2a; border-color: #ff5a3c; }
body.theme-darkness .ctrl-checkbox:checked::after { color: #fff; }
body.theme-darkness .ctrl-btn { background: #1e1e1e; border: 1px solid #3a3a3a; color: #ff7a45; font-weight: 600; }
body.theme-darkness .ctrl-btn:hover { background: #2a2a2a; border-color: #ff5a3c; color: #ff8a65; }
body.theme-darkness .ctrl-btn-save { background: #1f3a2a; border: 1px solid #2d5a3f; color: #6fff9a; font-weight: 600; }
body.theme-darkness .ctrl-btn-save:hover { background: #2a4a35; border-color: #6fff9a; }
body.theme-darkness .ctrl-sep { background: #3a3a3a; }
body.theme-darkness .status-bar { color: #aaa; }
body.theme-darkness .status-dot { background: #6a6a6a; }
body.theme-darkness .status-dot.ok { background: #6fbf73; }
body.theme-darkness .status-dot.err { background: #ff5a3c; }
body.theme-darkness .status-dot.loading { background: #ff8c3a; }
body.theme-darkness .srv-card {
  background: #1b1b1b;
  border: 1px solid #2f2f2f;
  box-shadow: 0 2px 8px rgba(0,0,0,0.5), 0 1px 2px rgba(0,0,0,0.4);
}
body.theme-darkness .srv-card:hover {
  border-color: #5a2a1f;
  box-shadow: 0 6px 20px rgba(255, 90, 60, 0.18), 0 2px 6px rgba(0,0,0,0.4);
}
body.theme-darkness .srv-card-head {
  background: linear-gradient(to bottom, #2c2c2c, #1c1c1c);
  border-bottom: 1px solid #333;
}
body.theme-darkness .srv-card-title { color: #e0e0e0; font-weight: 700; }
body.theme-darkness .srv-addr { color: #aaa; }
body.theme-darkness .badge-q2 { background: rgba(255,90,60,0.15); color: #ff7a45; border: 1px solid rgba(255,90,60,0.4); font-weight: 700; }
body.theme-darkness .badge-qw { background: rgba(215,140,255,0.15); color: #d78cff; border: 1px solid rgba(215,140,255,0.4); font-weight: 700; }
body.theme-darkness .no-servers { color: #888; }
body.theme-darkness .error-box { background: rgba(255,90,60,0.1); border: 1px solid rgba(255,90,60,0.45); color: #ff8a65; }
body.theme-darkness .srv-footer { border-top: 1px solid #2f2f2f; color: #aaa; }
body.theme-darkness .srv-footer a { color: #ff5a3c; }
/* Панель статистики */
body.theme-darkness .srv-stats { background: #1a1a1a; border-color: #2f2f2f; }
body.theme-darkness .stat-val { color: #ff7a45; }
body.theme-darkness .stat-key { color: #aaa; }
body.theme-darkness .stats-details { border-top-color: #2f2f2f; }
body.theme-darkness .stat-detail-item { color: #d0d0d0; }
body.theme-darkness .stat-detail-item b { color: #ff8a65; }
body.theme-darkness .stat-detail-label { color: #aaa; }
body.theme-darkness .hint-icon { background: rgba(255,90,60,0.15); color: #ff7a45; border: 1px solid rgba(255,90,60,0.4); }
body.theme-darkness .ctrl-btn-local { background: #1f3a2a; border: 1px solid #2d5a3f; color: #6fff9a; font-weight: 600; }
body.theme-darkness .ctrl-btn-local:hover { background: #2a4a35; border-color: #6fff9a; }
body.theme-darkness.local-enabled .btn-local { background: #34d399; border: 1px solid #6fff9a; color: #0a1a12; font-weight: 700; }
body.theme-darkness.local-enabled .btn-local:hover { background: #6fff9a; border-color: #a7f3d0; }
/* Мод / карта */
body.theme-darkness .srv-mod { color: #d78cff; }
/* Секция НАСТРОЙКИ СЕРВЕРА */
body.theme-darkness .srv-details { border-color: #2f2f2f; }
body.theme-darkness .srv-details summary { background: #1f1f1f; color: #bdbdbd; }
body.theme-darkness .srv-details summary::before { color: #ff7a45; }
body.theme-darkness .srv-details summary:hover { background: #2a2a2a; color: #ff8a65; }
body.theme-darkness .srv-kv-key { color: #aaa; }
body.theme-darkness .srv-kv-val { color: #e0e0e0; }
/* GTi кнопка */
body.theme-darkness .btn-gti { background: rgba(216,160,96,0.12); border-color: rgba(216,160,96,0.4); color: #d8a060; }
body.theme-darkness .btn-gti:hover { background: rgba(216,160,96,0.22); border-color: #d8a060; }
body.theme-darkness .btn-gti .gti-i { color: #ff7a45; }
/* Q2TV кнопки в контролах */
body.theme-darkness .ctrl-btn-q2tv-off { background: rgba(215,140,255,0.12); border-color: rgba(215,140,255,0.35); color: #d78cff; }
body.theme-darkness .ctrl-btn-q2tv-off:hover { background: rgba(215,140,255,0.22); border-color: #d78cff; }
body.theme-darkness .ctrl-btn-q2tv-on { background: #2f1f3a; border-color: #d78cff; color: #d78cff; }
body.theme-darkness .ctrl-btn-q2tv-on:hover { background: #3f2a4a; border-color: #e9b3ff; color: #e9b3ff; }
/* Q2TV кнопка в карточке сервера */
body.theme-darkness.q2tv-enabled .btn-q2tv { background: #2f1f3a; border: 1px solid #d78cff; color: #d78cff; font-weight: 700; }
body.theme-darkness .btn-q2tv:hover { background: #3f2a4a; border-color: #e9b3ff; color: #e9b3ff; }
/* GTi диалог */
body.theme-darkness #gt-box { background: #181818; border-color: #5a2a1f; box-shadow: 0 20px 60px rgba(0,0,0,0.8); }
body.theme-darkness #gt-header { background: linear-gradient(to bottom, #2c2c2c, #181818); border-bottom-color: #333; }
body.theme-darkness #gt-title { color: #ff7a45; }
body.theme-darkness #gt-expand, body.theme-darkness #gt-close { color: #aaa; }
body.theme-darkness #gt-expand:hover { color: #ff8a65; background: rgba(255,90,60,0.12); }
body.theme-darkness #gt-close:hover { color: #ff5a3c; background: rgba(255,90,60,0.16); }
body.theme-darkness #gt-body { background: #181818; color: #d8d8d8; }
body.theme-darkness .gt-link { color: #ff5a3c; }
body.theme-darkness .gt-section-title { color: #ff7a45; border-bottom-color: #333; }
body.theme-darkness .gt-info-key { color: #aaa; }
body.theme-darkness .gt-info-val { color: #e0e0e0; }
body.theme-darkness .gt-status-alive { color: #6fbf73; }
body.theme-darkness .gt-status-dead  { color: #ff5a3c; }
body.theme-darkness .gt-rank-val { color: #ff8c3a; }
body.theme-darkness .gt-players-table th { color: #ff7a45; border-bottom-color: #333; }
body.theme-darkness .gt-players-table td { color: #d8d8d8; border-bottom-color: #252525; }
body.theme-darkness .gt-players-table tr:hover td { background: #202020; }
body.theme-darkness .gt-rank-cell  { color: #888; }
body.theme-darkness .gt-score-cell { color: #ff8c3a; }
body.theme-darkness .gt-time-cell  { color: #aaa; }
body.theme-darkness .gt-chart-wrap { background: #1e1e1e; border-color: #2f2f2f; }
body.theme-darkness .gt-chart-label { color: #aaa; }
body.theme-darkness .gt-map-img { border-color: #3a3a3a; }
body.theme-darkness .gt-map-name { color: #e0e0e0; }
body.theme-darkness .gt-loading { color: #888; }
body.theme-darkness .gt-error   { color: #ff5a3c; }
/* Q2TV предупреждение (диалог "Включить Q2TV") */
body.theme-darkness #q2tv-warn-box { background: #181818; border-color: #2f1f3a; color: #d8d8d8; box-shadow: 0 16px 60px rgba(0,0,0,0.8); }
body.theme-darkness #q2tv-warn-title { color: #d78cff; }
body.theme-darkness .q2tv-warn-section { color: #d78cff; }
body.theme-darkness #q2tv-warn-body code { background: #2a2a2a; color: #ff8a65; }
body.theme-darkness #q2tv-warn-yes { background: #2f1f3a; border-color: #d78cff; color: #d78cff; }
body.theme-darkness #q2tv-warn-yes:hover { background: #3f2a4a; }
body.theme-darkness #q2tv-warn-no { background: #1e1e1e; border-color: #3a3a3a; color: #aaa; }
body.theme-darkness #q2tv-warn-no:hover { background: #2a2a2a; color: #d8d8d8; }
/* Диалог "Локальный Q2 клиент" */
body.theme-darkness #local-modal-box { background: #181818; border-color: #2d5a3f; color: #d8d8d8; box-shadow: 0 16px 60px rgba(0,0,0,0.8); }
body.theme-darkness #local-modal-title { color: #6fff9a; }
body.theme-darkness #local-modal-close { color: #aaa; }
body.theme-darkness #local-modal-close:hover { color: #ff5a3c; }
body.theme-darkness .local-modal-label { color: #aaa; }
body.theme-darkness #local-modal-path { background: #1e1e1e; border-color: #3a3a3a; color: #e0e0e0; }
body.theme-darkness #local-modal-path:focus { border-color: #6fff9a; }
body.theme-darkness .local-modal-hint { color: #888; }
body.theme-darkness .local-modal-hint b { color: #d8d8d8; }
body.theme-darkness #local-modal-clear { border-color: #3a3a3a; color: #888; }
body.theme-darkness #local-modal-clear:hover { color: #ff5a3c; border-color: rgba(255,90,60,0.4); }
body.theme-darkness #local-modal-dl { background: #1f3a2a; border-color: #2d5a3f; color: #6fff9a; }
body.theme-darkness #local-modal-dl:hover { background: #2a4a35; }
body.theme-darkness #local-modal-save { background: #34d399; border-color: #6fff9a; color: #0a1a12; font-weight: 700; }
body.theme-darkness #local-modal-save:hover { background: #6fff9a; }

/* ── Local client config modal ── */
#local-modal {
  display: none;
  position: fixed;
  inset: 0;
  z-index: 10006;
  align-items: center;
  justify-content: center;
}
#local-modal.active { display: flex; }
#local-modal-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0,0,0,0.6);
  backdrop-filter: blur(3px);
}
#local-modal-box {
  position: relative;
  z-index: 1;
  background: #1e1e2e;
  border: 1px solid rgba(52,211,153,0.35);
  border-radius: 10px;
  padding: 20px 24px 16px;
  max-width: 460px;
  width: 90vw;
  color: #cdd6f4;
  box-shadow: 0 16px 60px rgba(0,0,0,0.7);
}
#local-modal-title {
  font-size: 14px;
  font-weight: 600;
  color: #6ee7b7;
  margin-bottom: 14px;
}
#local-modal-close {
  position: absolute;
  top: 10px;
  right: 12px;
  background: none;
  border: none;
  color: #aaa;
  font-size: 18px;
  cursor: pointer;
  padding: 2px 6px;
}
#local-modal-close:hover { color: #f87171; }
.local-modal-label {
  font-size: 12px;
  color: #94a3b8;
  margin-bottom: 5px;
}
#local-modal-path {
  width: 100%;
  box-sizing: border-box;
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.15);
  border-radius: 6px;
  color: #cdd6f4;
  font-size: 12px;
  padding: 7px 10px;
  font-family: monospace;
  outline: none;
}
#local-modal-path:focus { border-color: rgba(52,211,153,0.5); }
.local-modal-hint {
  font-size: 11px;
  color: #64748b;
  margin-top: 10px;
  line-height: 1.6;
}
.local-modal-hint b { color: #94a3b8; }
#local-modal-btns {
  display: flex;
  gap: 8px;
  margin-top: 16px;
  align-items: center;
}
#local-modal-clear {
  background: transparent;
  border: 1px solid rgba(255,255,255,0.12);
  color: #64748b;
  padding: 6px 12px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
  margin-right: auto;
}
#local-modal-clear:hover { color: #f87171; border-color: rgba(248,113,113,0.3); }
#local-modal-dl {
  background: rgba(52,211,153,0.12);
  border: 1px solid rgba(52,211,153,0.35);
  color: #6ee7b7;
  padding: 6px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
}
#local-modal-dl:hover { background: rgba(52,211,153,0.25); }
#local-modal-save {
  background: rgba(52,211,153,0.22);
  border: 1px solid rgba(52,211,153,0.55);
  color: #a7f3d0;
  padding: 6px 18px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 12px;
  font-weight: 600;
}
#local-modal-save:hover { background: rgba(52,211,153,0.35); }

/* ── GTi button ── */
.btn-gti {
  display: inline-block;
  margin-left: 7px;
  padding: 1px 5px;
  font-size: 10px;
  font-weight: 700;
  background: rgba(251,191,36,0.12);
  border: 1px solid rgba(251,191,36,0.35);
  border-radius: 4px;
  color: #fbbf24;
  cursor: pointer;
  vertical-align: middle;
  transition: background 0.15s;
  white-space: nowrap;
  line-height: 1.4;
  letter-spacing: 0.03em;
}
.btn-gti:hover { background: rgba(251,191,36,0.28); border-color: rgba(251,191,36,0.6); }
.btn-gti .gti-i { color: #60a5fa; font-style: italic; }

/* ── GT modal ── */
#gt-modal { display: none; position: fixed; inset: 0; z-index: 10003; align-items: center; justify-content: center; }
#gt-modal.active { display: flex; }
#gt-backdrop { position: absolute; inset: 0; background: rgba(0,0,0,0.65); backdrop-filter: blur(3px); }
#gt-box {
  position: relative; z-index: 1;
  background: #1a1a2e;
  border: 1px solid rgba(251,191,36,0.3);
  border-radius: 12px;
  width: min(960px, 96vw);
  max-width: 100vw;
  max-height: 90vh;
  display: flex; flex-direction: column;
  overflow: hidden;
  box-shadow: 0 20px 80px rgba(0,0,0,0.8);
}
#gt-header {
  display: flex; align-items: center;
  padding: 11px 16px;
  background: rgba(251,191,36,0.07);
  border-bottom: 1px solid rgba(251,191,36,0.2);
  flex-shrink: 0;
}
#gt-title { flex: 1; font-size: 13px; font-weight: 600; color: #fbbf24; }
#gt-expand, #gt-close { background: none; border: none; color: #888; font-size: 18px; cursor: pointer; padding: 2px 6px; border-radius: 4px; }
#gt-expand:hover { color: #fbbf24; background: rgba(251,191,36,0.1); }
#gt-close:hover  { color: #f87171; background: rgba(248,113,113,0.1); }
#gt-box.gt-expanded { width: 100vw !important; height: 100vh !important; max-height: 100vh !important; border-radius: 0 !important; }
#gt-box.gt-expanded .gt-map-img { width: 100%; height: auto; max-height: 55vh; object-fit: contain; }
#gt-box.gt-expanded .gt-chart-wrap { flex: 0 1 calc(50% - 5px); max-width: calc(50% - 5px); }
#gt-body { overflow-y: auto; flex: 1; padding: 14px 16px; color: #cdd6f4; font-size: 13px; }
.gt-link { display: inline-block; margin-bottom: 12px; font-size: 11px; color: #60a5fa; text-decoration: none; }
.gt-link:hover { text-decoration: underline; }
.gt-section { margin-bottom: 14px; }
.gt-section-title { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #fbbf24; margin-bottom: 7px; border-bottom: 1px solid rgba(251,191,36,0.2); padding-bottom: 3px; }
.gt-info-grid { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; font-size: 12px; }
.gt-info-key { color: #94a3b8; white-space: nowrap; }
.gt-info-val { color: #e2e8f0; }
.gt-status-alive { color: #4ade80; font-weight: 600; }
.gt-status-dead  { color: #f87171; font-weight: 600; }
.gt-rank-val { color: #fbbf24; font-weight: 600; }
.gt-players-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.gt-players-table th { text-align: left; padding: 4px 8px; color: #94a3b8; font-weight: 600; font-size: 11px; border-bottom: 1px solid rgba(255,255,255,0.1); }
.gt-players-table td { padding: 3px 8px; border-bottom: 1px solid rgba(255,255,255,0.05); color: #e2e8f0; }
.gt-players-table tr:hover td { background: rgba(255,255,255,0.04); }
.gt-rank-cell  { color: #94a3b8; }
.gt-score-cell { color: #fbbf24; font-family: monospace; }
.gt-time-cell  { color: #60a5fa; }
.gt-charts { display: flex; flex-wrap: wrap; gap: 10px; }
.gt-chart-wrap { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: 8px; padding: 8px; flex: 0 1 calc(50% - 5px); min-width: 220px; max-width: calc(50% - 5px); }
.gt-chart-label { font-size: 11px; color: #94a3b8; margin-bottom: 5px; font-weight: 600; }
.gt-chart-wrap img { width: 100%; height: auto; border-radius: 4px; display: block; }
.gt-map-wrap { display: flex; align-items: flex-start; gap: 12px; margin-bottom: 4px; }
.gt-map-img { width: 360px; max-width: 100%; height: auto; aspect-ratio: 4/3; object-fit: cover; border-radius: 6px; border: 1px solid rgba(255,255,255,0.1); flex-shrink: 0; }
.gt-map-name { font-size: 16px; font-weight: 600; color: #e2e8f0; }
.gt-details { border: none; }
.gt-summary { cursor: pointer; list-style: none; user-select: none; }
.gt-summary::-webkit-details-marker { display: none; }
.gt-summary::before { content: '▶ '; font-size: 9px; vertical-align: middle; }
details.gt-details[open] .gt-summary::before { content: '▼ '; }
.gt-details[open] .gt-charts { margin-top: 8px; }
.gt-loading { text-align: center; padding: 40px; color: #64748b; }
.gt-error   { text-align: center; padding: 24px; color: #f87171; }

/* ── Q2TV modal ── */
#q2tv-modal {
  display: none;
  position: fixed;
  inset: 0;
  z-index: 9999;
  align-items: center;
  justify-content: center;
}
#q2tv-modal.active { display: flex; }
#q2tv-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0,0,0,0.75);
  backdrop-filter: blur(4px);
}
#q2tv-box {
  position: relative;
  z-index: 1;
  display: flex;
  flex-direction: column;
  width: min(1024px, 96vw);
  height: min(640px, 90vh);
  background: #080d1a;
  border: 1px solid rgba(99, 130, 255, 0.25);
  border-radius: 10px;
  overflow: hidden;
  box-shadow: 0 24px 80px rgba(0,0,0,0.7);
}
#q2tv-box.fake-fullscreen {
  position: fixed !important;
  inset: 0 !important;
  width: 100% !important;
  height: 100% !important;
  max-width: none !important;
  max-height: none !important;
  border-radius: 0 !important;
  margin: 0 !important;
  transform: none !important;
  z-index: 9999;
}
/* ── Q2TV resize handles ── */
.q2tv-rh {
  position: absolute;
  z-index: 20;
}
.q2tv-rh-n  { top: 0;    left: 8px;  right: 8px; height: 5px; cursor: n-resize; }
.q2tv-rh-s  { bottom: 0; left: 8px;  right: 8px; height: 5px; cursor: s-resize; }
.q2tv-rh-e  { right: 0;  top: 8px; bottom: 8px;  width: 5px;  cursor: e-resize; }
.q2tv-rh-w  { left: 0;   top: 8px; bottom: 8px;  width: 5px;  cursor: w-resize; }
.q2tv-rh-nw { top: 0; left: 0;   width: 14px; height: 14px; cursor: nw-resize; }
.q2tv-rh-ne { top: 0; right: 0;  width: 14px; height: 14px; cursor: ne-resize; }
.q2tv-rh-sw { bottom: 0; left: 0;  width: 14px; height: 14px; cursor: sw-resize; }
.q2tv-rh-se { bottom: 0; right: 0; width: 14px; height: 14px; cursor: se-resize; }

#q2tv-header {
  display: flex;
  align-items: center;
  padding: 8px 12px;
  background: rgba(10,16,32,0.95);
  border-bottom: 1px solid rgba(99,130,255,0.15);
  gap: 8px;
  flex-shrink: 0;
  cursor: grab;
  user-select: none;
}
#q2tv-header.dragging { cursor: grabbing; }
#q2tv-title {
  flex: 1;
  font-size: 13px;
  font-weight: 600;
  color: #93c5fd;
  letter-spacing: 0.05em;
}
#q2tv-clearcache, #q2tv-fullscreen, #q2tv-close {
  background: none;
  border: none;
  color: #64748b;
  font-size: 16px;
  cursor: pointer;
  padding: 2px 6px;
  border-radius: 4px;
  line-height: 1;
  transition: color 0.15s, background 0.15s;
}
#q2tv-clearcache:hover { color: #fcd34d; background: rgba(252,211,77,0.1); }
#q2tv-fullscreen:hover { color: #93c5fd; background: rgba(99,130,255,0.1); }
#q2tv-close:hover { color: #f87171; background: rgba(248,113,113,0.1); }
#q2tv-iframe {
  flex: 1;
  border: none;
  width: 100%;
  display: block;
}

/* ── Q2TV loading overlay ── */
#q2tv-loading {
  display: none;
  position: absolute;
  inset: 0;
  background: rgba(8,13,26,0.97);
  flex-direction: column;
  align-items: center;
  justify-content: center;
  z-index: 10000;
  gap: 16px;
  border-radius: 10px;
}
@keyframes q2tv-spin { to { transform: rotate(360deg); } }
#q2tv-load-spinner {
  width: 40px; height: 40px;
  border: 3px solid rgba(99,130,255,0.2);
  border-top-color: #6382ff;
  border-radius: 50%;
  animation: q2tv-spin 0.8s linear infinite;
}
#q2tv-load-text {
  color: #8899bb;
  font-size: 13px;
  font-family: monospace;
}
#q2tv-load-bar-wrap {
  width: 260px;
  height: 4px;
  background: rgba(255,255,255,0.08);
  border-radius: 2px;
  overflow: hidden;
}
#q2tv-load-bar {
  height: 100%;
  background: #6382ff;
  border-radius: 2px;
  width: 0%;
  transition: width 0.4s;
}

/* ── Q2TV modal: на мобилке на весь экран ── */
@media (max-width: 767px) {
  #q2tv-modal {
    align-items: flex-start;
    justify-content: flex-start;
  }
  #q2tv-box {
    width: 100vw;
    height: 100vh; /* fallback, точная высота ставится через JS */
    border-radius: 0;
    border: none;
  }
  #q2tv-loading {
    border-radius: 0;
  }
  /* warn-modal: компактнее на мобилке */
  #q2tv-warn-box {
    width: 96vw;
    max-height: 90dvh;
    padding: 12px 14px 10px;
    font-size: 11px;
  }
  #q2tv-warn-title {
    font-size: 12px;
    margin-bottom: 7px;
  }
  #q2tv-warn-body {
    font-size: 11px;
    line-height: 1.4;
  }
  #q2tv-warn-body code {
    font-size: 10px;
  }
  .q2tv-warn-section {
    font-size: 11px;
  }
  #q2tv-warn-yes, #q2tv-warn-no {
    font-size: 12px;
    padding: 6px 14px;
  }
}

/* ── Q2TV warning modal ── */
#q2tv-warn-modal {
  display: none;
  position: fixed;
  inset: 0;
  z-index: 10006;
  align-items: center;
  justify-content: center;
}
#q2tv-warn-modal.active { display: flex; }
#q2tv-warn-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0,0,0,0.6);
  backdrop-filter: blur(3px);
}
#q2tv-warn-box {
  position: relative;
  z-index: 1;
  background: #1e1e2e;
  border: 1px solid rgba(139,92,246,0.4);
  border-radius: 10px;
  padding: 16px 20px 14px;
  max-width: 480px;
  width: 90vw;
  max-height: 80vh;
  display: flex;
  flex-direction: column;
  color: #cdd6f4;
  font-size: 12px;
  line-height: 1.45;
  box-shadow: 0 16px 60px rgba(0,0,0,0.7);
}
#q2tv-warn-title {
  font-size: 13px;
  font-weight: 600;
  color: #a78bfa;
  margin-bottom: 10px;
  flex-shrink: 0;
}
#q2tv-warn-body {
  overflow-y: auto;
  flex: 1;
  min-height: 0;
  padding-right: 4px;
}
#q2tv-warn-body p { margin: 0 0 6px; }
#q2tv-warn-body ul { margin: 3px 0 8px 0; padding-left: 16px; }
#q2tv-warn-body li { margin-bottom: 2px; }
.q2tv-warn-section {
  margin: 8px 0 3px;
  font-weight: 600;
  color: #a78bfa;
  font-size: 12px;
}
#q2tv-warn-body code {
  background: rgba(255,255,255,0.08);
  padding: 1px 5px;
  border-radius: 4px;
  font-family: monospace;
  font-size: 11px;
}
#q2tv-warn-btns {
  display: flex;
  gap: 8px;
  margin-top: 12px;
  justify-content: flex-end;
  flex-shrink: 0;
}
#q2tv-warn-yes {
  background: rgba(139,92,246,0.3);
  border: 1px solid rgba(139,92,246,0.6);
  color: #ddd6fe;
  padding: 7px 20px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 13px;
}
#q2tv-warn-yes:hover { background: rgba(139,92,246,0.5); }
#q2tv-warn-no {
  background: rgba(255,255,255,0.05);
  border: 1px solid rgba(255,255,255,0.15);
  color: #aaa;
  padding: 7px 20px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 13px;
}
#q2tv-warn-no:hover { background: rgba(255,255,255,0.1); }

/* ── Footer ── */
.srv-footer {
  text-align: center;
  padding: 20px;
  font-size: 12px;
  color: #475569;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  flex-wrap: wrap;
}
.srv-footer a {
  color: #64748b;
  text-decoration: none;
}
.srv-footer a:hover { color: #93c5fd; }
.srv-footer-sep { color: #334155; }

/* ── Hint tooltip ── */
.hint-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 15px; height: 15px;
  border-radius: 50%;
  background: rgba(99, 130, 255, 0.15);
  border: 1px solid rgba(99, 130, 255, 0.35);
  color: #93c5fd;
  font-size: 9px;
  font-weight: 700;
  cursor: help;
  position: relative;
  flex-shrink: 0;
  line-height: 1;
}
/* hint-icon tooltip is rendered via JS into body — see initHintTooltip() */
.hint-tooltip {
  position: fixed;
  background: rgba(8, 13, 26, 0.97);
  border: 1px solid rgba(99, 130, 255, 0.3);
  border-radius: 8px;
  padding: 9px 12px;
  font-size: 12px;
  color: #cbd5e1;
  white-space: pre-line;
  width: 270px;
  text-align: left;
  pointer-events: none;
  opacity: 0;
  transition: opacity 0.15s;
  z-index: 99999;
  font-weight: 400;
  line-height: 1.55;
}

/* ── Stats ── */
.srv-stats {
  margin-top: 28px;
  padding: 16px 20px;
  background: rgba(15, 23, 42, 0.5);
  border: 1px solid rgba(99, 130, 255, 0.12);
  border-radius: 12px;
}
.stats-nums {
  display: flex;
  gap: 28px;
  flex-wrap: wrap;
  align-items: flex-end;
  margin-bottom: 14px;
}
.stat-num {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2px;
}
.stat-val {
  font-size: 26px;
  font-weight: 700;
  color: #93c5fd;
  line-height: 1;
}
.stat-key {
  font-size: 11px;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  white-space: nowrap;
}
.stats-details {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 20px;
  border-top: 1px solid rgba(99, 130, 255, 0.08);
  padding-top: 10px;
}
.stat-detail-item {
  font-size: 12px;
  color: #94a3b8;
  display: flex;
  align-items: center;
  gap: 4px;
}
.stat-detail-item b { color: #cbd5e1; font-weight: 600; }
.stat-detail-label { color: #64748b; }

/* ── Top row: no wrap on desktop ── */
.ctrl-row-top { flex-wrap: nowrap; }

/* ── Filter row: визуально ближе к контенту ── */
.ctrl-row-filters {
  border-top: 1px solid rgba(99, 130, 255, 0.1);
  padding-top: 8px;
  flex-wrap: wrap;
}

/* ── Settings button ── */
.ctrl-btn-settings {
  background: rgba(99, 130, 255, 0.12);
  border: 1px solid rgba(99, 130, 255, 0.3);
  color: #93c5fd;
  white-space: nowrap;
}
.ctrl-btn-settings:hover {
  background: rgba(99, 130, 255, 0.25);
  border-color: rgba(99, 130, 255, 0.55);
}

/* ── Settings modal ── */
#settings-modal {
  display: none;
  position: fixed;
  inset: 0;
  z-index: 10004;
}
#settings-modal.active { display: block; }
#settings-backdrop {
  position: absolute;
  inset: 0;
  background: rgba(0,0,0,0.2);
}
#settings-box {
  position: absolute;
}
#settings-box {
  z-index: 1;
  background: #1a1f35;
  border: 1px solid rgba(99,130,255,0.3);
  border-radius: 12px;
  width: 280px;
  box-shadow: 0 8px 40px rgba(0,0,0,0.6);
  overflow: hidden;
}
#settings-header {
  display: flex;
  align-items: center;
  padding: 12px 16px;
  background: rgba(99,130,255,0.08);
  border-bottom: 1px solid rgba(99,130,255,0.15);
}
#settings-title {
  flex: 1;
  font-size: 13px;
  font-weight: 600;
  color: #93c5fd;
}
#settings-close {
  background: none;
  border: none;
  color: #64748b;
  font-size: 16px;
  cursor: pointer;
  padding: 2px 4px;
  border-radius: 4px;
  line-height: 1;
}
#settings-close:hover { color: #f87171; }
#settings-body { padding: 4px 0 8px; }
.settings-section { padding: 8px 16px; }
.settings-section-label {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: #475569;
  margin-bottom: 8px;
}
.settings-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;
}
.settings-row:last-child { margin-bottom: 0; }
.settings-label {
  font-size: 13px;
  color: #94a3b8;
  cursor: pointer;
  white-space: nowrap;
}
.settings-ctrl { flex: 1; }
.settings-divider {
  height: 1px;
  background: rgba(99,130,255,0.1);
  margin: 0 0;
}
.settings-row-save {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px 4px;
}

/* ── Settings modal — Light theme ── */
body.theme-light #settings-box { background: #ffffff; border-color: #cbd5e1; box-shadow: 0 16px 60px rgba(0,0,0,0.15); }
body.theme-light #settings-header { background: #f1f5f9; border-bottom-color: #e2e8f0; }
body.theme-light #settings-title { color: #1d4ed8; }
body.theme-light #settings-close { color: #64748b; }
body.theme-light #settings-close:hover { color: #dc2626; }
body.theme-light .settings-section-label { color: #94a3b8; }
body.theme-light .settings-label { color: #334155; }
body.theme-light .settings-divider { background: #e2e8f0; }
body.theme-light .ctrl-btn-settings { background: #e0e7ff; border-color: #a5b4fc; color: #1d4ed8; font-weight: 600; }
body.theme-light .ctrl-btn-settings:hover { background: #c7d2fe; border-color: #818cf8; }
body.theme-light .ctrl-row-filters { border-top-color: #e2e8f0; }

/* ── Settings modal — Q2CSS theme ── */
body.theme-q2css #settings-box { background: #FEF1DE; border: 1px solid #A22C21; border-radius: 0; box-shadow: none; }
body.theme-q2css #settings-header { background: #F5C4B0; border-bottom: 1px solid #A22C21; }
body.theme-q2css #settings-title { color: #5C1A12; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css #settings-close { color: #5C1A12; }
body.theme-q2css #settings-close:hover { color: #A22C21; }
body.theme-q2css .settings-section-label { color: #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight: 700; font-size: 11px; }
body.theme-q2css .settings-label { color: #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 12px; }
body.theme-q2css .settings-divider { background: #A22C21; opacity: 0.3; }
body.theme-q2css .ctrl-btn-settings { background: #FEF1DE; border: 1px solid #000; color: #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .ctrl-btn-settings:hover { background: #FAD3BC; border-color: #A22C21; }
body.theme-q2css .ctrl-row-filters { border-top: 1px solid #A22C21; }
/* Таблица игроков */
body.theme-q2css .players-title { color: #A22C21; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-weight: 700; }
body.theme-q2css .players-count { color: #5C1A12; background: #F5C4B0; border-color: #A22C21; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .players-table th { color: #3D3D3D; border-bottom: 1px solid #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 10px; }
body.theme-q2css .players-table td { color: #000; border-bottom: 1px solid rgba(0,0,0,0.15); font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 12px; }
body.theme-q2css .players-table tr:last-child td { border-bottom: none; }
body.theme-q2css .players-table tr:hover td { background: #FAD3BC; }
body.theme-q2css .score-val { color: #A22C21; font-weight: 700; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .score-trend.up   .score-trend-arrow { color: #006600; }
body.theme-q2css .score-trend.up   .score-trend-delta { color: #004400; }
body.theme-q2css .score-trend.down .score-trend-arrow { color: #A22C21; }
body.theme-q2css .score-trend.down .score-trend-delta { color: #6B0000; }
body.theme-q2css .ping-val  { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }
body.theme-q2css .ping-low  { color: #006600; }
body.theme-q2css .ping-mid  { color: #8a5c00; }
body.theme-q2css .ping-high { color: #A22C21; }
body.theme-q2css .no-players { color: #3D3D3D; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; }

/* ── Settings modal — Darkness theme ── */
body.theme-darkness #settings-box { background: #181818; border-color: #5a2a1f; box-shadow: 0 16px 60px rgba(0,0,0,0.8); }
body.theme-darkness #settings-header { background: linear-gradient(to bottom, #2c2c2c, #181818); border-bottom-color: #333; }
body.theme-darkness #settings-title { color: #ff7a45; }
body.theme-darkness #settings-close { color: #aaa; }
body.theme-darkness #settings-close:hover { color: #ff5a3c; }
body.theme-darkness .settings-section-label { color: #ff7a45; font-weight: 700; }
body.theme-darkness .settings-label { color: #d8d8d8; }
body.theme-darkness .settings-divider { background: #2f2f2f; }
body.theme-darkness .ctrl-btn-settings { background: #1e1e1e; border: 1px solid #3a3a3a; color: #ff7a45; font-weight: 600; }
body.theme-darkness .ctrl-btn-settings:hover { background: #2a2a2a; border-color: #ff5a3c; color: #ff8a65; }
body.theme-darkness .ctrl-row-filters { border-top-color: #2f2f2f; }
/* Таблица игроков — Darkness */
body.theme-darkness .players-title { color: #bdbdbd; }
body.theme-darkness .players-count { color: #ff7a45; background: rgba(255,90,60,0.15); border-color: rgba(255,90,60,0.4); }
body.theme-darkness .players-table th { color: #bdbdbd; border-bottom: 1px solid #2f2f2f; }
body.theme-darkness .players-table td { color: #d8d8d8; border-bottom: 1px solid #252525; }
body.theme-darkness .players-table tr:last-child td { border-bottom: none; }
body.theme-darkness .players-table tr:hover td { background: #202020; }
body.theme-darkness .score-val { color: #ff7a45; font-weight: 700; }
body.theme-darkness .score-trend.up   .score-trend-arrow { color: #6fbf73; }
body.theme-darkness .score-trend.up   .score-trend-delta { color: #4caf50; }
body.theme-darkness .score-trend.down .score-trend-arrow { color: #ff5a3c; }
body.theme-darkness .score-trend.down .score-trend-delta { color: #e24a2b; }
body.theme-darkness .ping-val  { color: #aaa; }
body.theme-darkness .ping-low  { color: #6fbf73; }
body.theme-darkness .ping-mid  { color: #ff8c3a; }
body.theme-darkness .ping-high { color: #ff5a3c; }
body.theme-darkness .no-players { color: #888; }

/* ── Responsive ── */
@media (max-width: 900px) {
  .ctrl-row-top { flex-wrap: wrap; gap: 8px; }
  .ctrl-sep { display: none; }
}

@media (max-width: 600px) {
  .srv-header { padding: 8px 12px; }
  .controls { gap: 8px; }
  .ctrl-row { gap: 8px; flex-wrap: wrap; }
  .ctrl-row-top { gap: 6px; }
  .ctrl-group { gap: 5px; }
  .srv-main { padding: 16px 12px 40px; }
  .servers-grid { grid-template-columns: 1fr; }
  .ctrl-input { width: 120px; }
  .header-left { width: 100%; }
  .status-bar { width: 100%; }
}

@media (max-width: 420px) {
  .ctrl-input { width: calc(100vw - 110px); max-width: 200px; }
  .ctrl-select { width: 100%; }
  .ctrl-group { width: 100%; }
  .ctrl-btn { font-size: 12px; padding: 5px 10px; }
  .ctrl-label { font-size: 12px; }
}
</style>
</head>
<body>

<header class="srv-header">
  <div class="header-inner">
    <div class="controls">

      <!-- Строка 1: навигация + обновление + статус + настройки -->
      <div class="ctrl-row ctrl-row-top">
        <div class="header-left">
          <a href="/" class="back-link" title="На главную" id="back-home-link">&#8592;</a>
          <span class="page-title">Серверы</span>
          <span class="page-badge">Live</span>
        </div>

        <div class="ctrl-sep"></div>

        <button class="ctrl-btn" id="btn-refresh" title="Обновить сейчас">&#8635; Обновить</button>

        <div class="ctrl-group">
          <label class="ctrl-label" for="sel-refresh">Авто:</label>
          <select id="sel-refresh" class="ctrl-select">
            <option value="0" selected>Выкл</option>
            <option value="5">5 сек</option>
            <option value="10">10 сек</option>
            <option value="30">30 сек</option>
            <option value="60">1 мин</option>
            <option value="300">5 мин</option>
          </select>
        </div>

        <div class="status-bar">
          <span class="status-dot" id="status-dot"></span>
          <span id="status-text">—</span>
        </div>

        <div class="ctrl-sep"></div>

        <button class="ctrl-btn ctrl-btn-settings" id="btn-settings" title="Настройки">&#9881; Настройки</button>
      </div>

      <!-- Строка 2: поиск / фильтры — визуально ближе к карточкам -->
      <div class="ctrl-row ctrl-row-filters">
        <div class="ctrl-group">
          <label class="ctrl-label" for="txt-players">Игроки:</label>
          <input type="text" id="txt-players" class="ctrl-input" placeholder="ник1, ник2...">
          <input type="checkbox" id="chk-players-sub" class="ctrl-checkbox" checked>
          <label class="ctrl-label" for="chk-players-sub">Не точное</label>
          <span class="hint-icon" tabindex="0" data-tip="☑ Не точное — поиск по вхождению: достаточно написать часть ника. Например, «urr» найдёт «purri» и «[purri]». Запятая разделяет несколько ников: «ly, david» покажет серверы, где есть хотя бы один из них. Регистр не важен.&#10;&#10;☐ Точное — ник должен совпадать полностью (кроме регистра). «purri» не найдёт «[purri]».">?</span>
        </div>

        <div class="ctrl-sep"></div>

        <div class="ctrl-group">
          <label class="ctrl-label" for="txt-servers">Серверa:</label>
          <input type="text" id="txt-servers" class="ctrl-input" placeholder="название или ip:port">
        </div>

        <div class="ctrl-sep"></div>

        <div class="ctrl-group">
          <label class="ctrl-label" for="txt-mod">Мод:</label>
          <input type="text" id="txt-mod" class="ctrl-input" placeholder="OpenFFA, CTF...">
        </div>

        <div class="ctrl-sep"></div>

        <div class="ctrl-group">
          <label class="ctrl-label" for="txt-map">Карта:</label>
          <input type="text" id="txt-map" class="ctrl-input" placeholder="q2dm1, match1...">
        </div>

        <div class="ctrl-sep"></div>

        <div class="ctrl-group">
          <label class="ctrl-label" for="sel-sort">Сортировка:</label>
          <select id="sel-sort" class="ctrl-select">
            <option value="default">По умолчанию</option>
            <option value="players">По игрокам</option>
            <option value="mod">По моду</option>
            <option value="map">По карте</option>
            <option value="score">По счёту</option>
          </select>
        </div>
      </div>

    </div>
  </div>
</header>

<main class="srv-main" id="main-content">
  <div class="no-servers"><span>&#8987;</span>Загрузка...</div>
</main>

<footer class="srv-footer">
  <span>Работает на данных <a href="https://t.me/Quake2InfoBot" target="_blank" rel="noopener">QuakeInfoBot</a></span>
  <span class="srv-footer-sep">·</span>
  <span>developed by ly, ${new Date().getFullYear()}</span>
  <span class="srv-footer-sep">·</span>
  <a href="https://github.com/orgs/Quake-Journey/repositories" target="_blank" rel="noopener">github</a>
</footer>

<div id="q2tv-modal">
  <div id="q2tv-backdrop"></div>
  <div id="q2tv-box">
    <div class="q2tv-rh q2tv-rh-n"  data-dir="n"></div>
    <div class="q2tv-rh q2tv-rh-s"  data-dir="s"></div>
    <div class="q2tv-rh q2tv-rh-e"  data-dir="e"></div>
    <div class="q2tv-rh q2tv-rh-w"  data-dir="w"></div>
    <div class="q2tv-rh q2tv-rh-nw" data-dir="nw"></div>
    <div class="q2tv-rh q2tv-rh-ne" data-dir="ne"></div>
    <div class="q2tv-rh q2tv-rh-sw" data-dir="sw"></div>
    <div class="q2tv-rh q2tv-rh-se" data-dir="se"></div>
    <div id="q2tv-header">
      <span id="q2tv-title">&#127909; Q2TV (альфа)</span>
      <button id="q2tv-clearcache" title="Сбросить кэш и перезагрузить">&#x21BA;</button>
      <button id="q2tv-fullscreen" title="Во весь экран">&#x26F6;</button>
      <button id="q2tv-close" title="Закрыть">&#x2715;</button>
    </div>
    <iframe id="q2tv-iframe" src="" allowfullscreen></iframe>
    <div id="q2tv-loading">
      <div id="q2tv-load-spinner"></div>
      <div id="q2tv-load-text">&#x417;&#x430;&#x433;&#x440;&#x443;&#x437;&#x43A;&#x430;...</div>
      <div id="q2tv-load-bar-wrap"><div id="q2tv-load-bar"></div></div>
    </div>
  </div>
</div>

<div id="q2tv-warn-modal">
  <div id="q2tv-warn-backdrop"></div>
  <div id="q2tv-warn-box">
    <div id="q2tv-warn-title">&#127909; Q2TV (&#x430;&#x43B;&#x44C;&#x444;&#x430;-&#x432;&#x435;&#x440;&#x441;&#x438;&#x44F;)</div>
    <div id="q2tv-warn-body">
      <p>&#127760; &#x41F;&#x440;&#x43E;&#x441;&#x43C;&#x43E;&#x442;&#x440; &#x438;&#x433;&#x440;&#x43E;&#x432;&#x44B;&#x445; &#x441;&#x435;&#x440;&#x432;&#x435;&#x440;&#x43E;&#x432; Quake II &#x43F;&#x440;&#x44F;&#x43C;&#x43E; &#x432; &#x431;&#x440;&#x430;&#x443;&#x437;&#x435;&#x440;&#x435;.</p>
      <p>&#9888; <b>&#x421;&#x442;&#x430;&#x442;&#x443;&#x441;:</b> &#x444;&#x443;&#x43D;&#x43A;&#x446;&#x438;&#x44F; &#x43D;&#x430;&#x445;&#x43E;&#x434;&#x438;&#x442;&#x441;&#x44F; &#x432; &#x440;&#x430;&#x437;&#x440;&#x430;&#x431;&#x43E;&#x442;&#x43A;&#x435; &#x438; &#x43C;&#x43E;&#x436;&#x435;&#x442; &#x440;&#x430;&#x431;&#x43E;&#x442;&#x430;&#x442;&#x44C; &#x43D;&#x435;&#x441;&#x442;&#x430;&#x431;&#x438;&#x43B;&#x44C;&#x43D;&#x43E;.</p>
      <div class="q2tv-warn-section">&#x1F5A5; &#x41F;&#x43E;&#x434;&#x434;&#x435;&#x440;&#x436;&#x43A;&#x430; &#x443;&#x441;&#x442;&#x440;&#x43E;&#x439;&#x441;&#x442;&#x432;:</div>
      <ul>
        <li>&#x434;&#x435;&#x441;&#x43A;&#x442;&#x43E;&#x43F; &#x438; &#x43C;&#x43E;&#x431;&#x438;&#x43B;&#x44C;&#x43D;&#x44B;&#x435; &#x443;&#x441;&#x442;&#x440;&#x43E;&#x439;&#x441;&#x442;&#x432;&#x430; (Android, iOS)</li>
      </ul>
      <div class="q2tv-warn-section">&#128230; &#x41F;&#x440;&#x435;&#x434;&#x43A;&#x435;&#x448;&#x438;&#x440;&#x43E;&#x432;&#x430;&#x43D;&#x43D;&#x44B;&#x435; &#x440;&#x435;&#x441;&#x443;&#x440;&#x441;&#x44B;:</div>
      <ul>
        <li>&#x441;&#x442;&#x430;&#x43D;&#x434;&#x430;&#x440;&#x442;&#x43D;&#x44B;&#x435; &#x438; &#x43D;&#x435;&#x43A;&#x43E;&#x442;&#x43E;&#x440;&#x44B;&#x435; &#x43F;&#x43E;&#x43F;&#x443;&#x43B;&#x44F;&#x440;&#x43D;&#x44B;&#x435; &#x43A;&#x430;&#x440;&#x442;&#x44B;</li>
        <li>&#x43D;&#x435;&#x43A;&#x43E;&#x442;&#x43E;&#x440;&#x44B;&#x435; &#x43C;&#x43E;&#x434;&#x435;&#x43B;&#x438; &#x438;&#x433;&#x440;&#x43E;&#x43A;&#x43E;&#x432; &#x438; &#x434;&#x440;&#x443;&#x433;&#x438;&#x435; &#x440;&#x435;&#x441;&#x443;&#x440;&#x441;&#x44B;</li>
      </ul>
      <div class="q2tv-warn-section">&#128295; &#x41D;&#x430;&#x441;&#x442;&#x440;&#x43E;&#x439;&#x43A;&#x438; &#x438;&#x43D;&#x442;&#x435;&#x440;&#x444;&#x435;&#x439;&#x441;&#x430; <span style="font-weight:normal;opacity:0.6">(&#x442;&#x43E;&#x43B;&#x44C;&#x43A;&#x43E; &#x434;&#x43B;&#x44F; &#x41F;&#x41A;)</span>:</div>
      <p>&#x420;&#x430;&#x437;&#x43C;&#x435;&#x440; &#x448;&#x440;&#x438;&#x444;&#x442;&#x43E;&#x432; &#x438; &#x44D;&#x43B;&#x435;&#x43C;&#x435;&#x43D;&#x442;&#x43E;&#x432; &#x438;&#x43D;&#x442;&#x435;&#x440;&#x444;&#x435;&#x439;&#x441;&#x430; &#x43C;&#x43E;&#x436;&#x43D;&#x43E; &#x438;&#x437;&#x43C;&#x435;&#x43D;&#x438;&#x442;&#x44C; &#x432; &#x43A;&#x43E;&#x43D;&#x441;&#x43E;&#x43B;&#x438; &#x43A;&#x43E;&#x43C;&#x430;&#x43D;&#x434;&#x43E;&#x439;:</p>
      <p><code>/set con_fontscale</code></p>
      <div class="q2tv-warn-section">&#128683; &#x41E;&#x433;&#x440;&#x430;&#x43D;&#x438;&#x447;&#x435;&#x43D;&#x438;&#x44F; &#x442;&#x435;&#x43A;&#x443;&#x449;&#x435;&#x439; &#x432;&#x435;&#x440;&#x441;&#x438;&#x438;:</div>
      <ul>
        <li>&#x437;&#x430;&#x433;&#x440;&#x443;&#x437;&#x43A;&#x430; &#x434;&#x43E; &#x441;&#x442;&#x430;&#x440;&#x442;&#x430; &#x43F;&#x43E;&#x434;&#x43A;&#x43B;&#x44E;&#x447;&#x435;&#x43D;&#x438;&#x44F; &#x43A; &#x43A;&#x430;&#x440;&#x442;&#x435; &#x43C;&#x43E;&#x436;&#x435;&#x442; &#x437;&#x430;&#x43D;&#x438;&#x43C;&#x430;&#x442;&#x44C; &#x434;&#x435;&#x441;&#x44F;&#x442;&#x43A;&#x438; &#x441;&#x435;&#x43A;&#x443;&#x43D;&#x434; (&#x436;&#x434;&#x438;&#x442;&#x435;)</li>
        <li>&#x432; &#x43A;&#x430;&#x447;&#x435;&#x441;&#x442;&#x432;&#x435; &#x430;&#x43D;&#x442;&#x438;-&#x441;&#x43F;&#x430;&#x43C;&#x430; &#x437;&#x430;&#x431;&#x43B;&#x43E;&#x43A;&#x438;&#x440;&#x43E;&#x432;&#x430;&#x43D;&#x44B; &#x43A;&#x43E;&#x43C;&#x430;&#x43D;&#x434;&#x44B; &#x442;&#x435;&#x43A;&#x441;&#x442;&#x43E;&#x432;&#x44B;&#x445; &#x441;&#x43E;&#x43E;&#x431;&#x449;&#x435;&#x43D;&#x438;&#x439;</li>
      </ul>
    </div>
    <div id="q2tv-warn-btns">
      <button id="q2tv-warn-yes">&#x414;&#x430;, &#x432;&#x43A;&#x43B;&#x44E;&#x447;&#x438;&#x442;&#x44C;</button>
      <button id="q2tv-warn-no">&#x41D;&#x435;&#x442;</button>
    </div>
  </div>
</div>

<div id="settings-modal">
  <div id="settings-backdrop"></div>
  <div id="settings-box">
    <div id="settings-header">
      <span id="settings-title">&#9881; Настройки</span>
      <button id="settings-close">&#x2715;</button>
    </div>
    <div id="settings-body">

      <div class="settings-section">
        <div class="settings-section-label">Данные</div>
        <div class="settings-row">
          <label class="settings-label" for="sel-game">Игра:</label>
          <select id="sel-game" class="ctrl-select settings-ctrl">
            <option value="q2" selected>Quake II</option>
            <option value="qw">QuakeWorld</option>
            <option value="all">Все</option>
          </select>
        </div>
        <div class="settings-row">
          <input type="checkbox" id="chk-notempty" class="ctrl-checkbox" checked>
          <label class="settings-label" for="chk-notempty">Только с игроками</label>
        </div>
        <div class="settings-row">
          <input type="checkbox" id="chk-ignorebots" class="ctrl-checkbox" checked>
          <label class="settings-label" for="chk-ignorebots">Игнорировать ботов</label>
        </div>
      </div>

      <div class="settings-divider"></div>

      <div class="settings-section">
        <div class="settings-section-label">Функции</div>
        <div class="settings-row">
          <button id="btn-q2tv-toggle" class="ctrl-btn-q2tv-off">&#127909; Включить Q2TV (альфа)</button>
        </div>
        <div class="settings-row ctrl-group-local">
          <button id="btn-local-client" class="ctrl-btn-local">&#128421; Клиент</button>
        </div>
      </div>

      <div class="settings-divider"></div>

      <div class="settings-section">
        <div class="settings-section-label">Отображение</div>
        <div class="settings-row">
          <input type="checkbox" id="chk-fullwidth" class="ctrl-checkbox">
          <label class="settings-label" for="chk-fullwidth">На всю ширину</label>
        </div>
        <div class="settings-row">
          <input type="checkbox" id="chk-dynamic-scores" class="ctrl-checkbox">
          <label class="settings-label" for="chk-dynamic-scores">Динамические показатели</label>
        </div>
        <div class="settings-row">
          <label class="settings-label" for="sel-theme">Тема:</label>
          <select id="sel-theme" class="ctrl-theme-select settings-ctrl">
            <option value="auto">🌗 Авто</option>
            <option value="light">☀️ Светлая</option>
            <option value="dark">🌙 Тёмная</option>
            <option value="darkness">🩸 Darkness</option>
            <option value="q2css">🎮 Альтернативная</option>
          </select>
        </div>
      </div>

      <div class="settings-divider"></div>

      <div class="settings-section">
        <div class="settings-row">
          <button class="ctrl-btn ctrl-btn-save" id="btn-save" title="Сохранить настройки в браузере">&#128190; Сохранить настройки</button>
        </div>
        <div class="settings-row">
          <input type="checkbox" id="chk-autosave" class="ctrl-checkbox">
          <label class="settings-label" for="chk-autosave">Автосохранение</label>
        </div>
      </div>

    </div>
  </div>
</div>

<div id="local-modal">
  <div id="local-modal-backdrop"></div>
  <div id="local-modal-box">
    <div id="local-modal-title">&#128421; Локальный Q2 клиент</div>
    <button id="local-modal-close">&#x2715;</button>
    <div class="local-modal-label">Путь к quake2.exe:</div>
    <input type="text" id="local-modal-path" placeholder="C:\Games\YamagiQ2\quake2.exe" spellcheck="false">
    <div class="local-modal-hint">
      <b>Как настроить (один раз):</b><br>
      1. Введите полный путь к exe вашего Q2 клиента<br>
      2. Нажмите <b>Скачать .reg</b> и запустите скачанный файл<br>
      3. Подтвердите добавление в реестр Windows<br>
      4. Нажмите <b>Сохранить</b> — в карточках серверов появится кнопка <b>&#9654; Запустить</b>
    </div>
    <div id="local-modal-btns">
      <button id="local-modal-clear">Сбросить</button>
      <button id="local-modal-dl" style="display:none">&#11015; Скачать .reg</button>
      <button id="local-modal-save" style="display:none">Сохранить</button>
    </div>
  </div>
</div>

<div id="gt-modal">
  <div id="gt-backdrop"></div>
  <div id="gt-box">
    <div id="gt-header">
      <span id="gt-title">GameTracker</span>
      <button id="gt-expand" title="Развернуть">&#x26F6;</button>
      <button id="gt-close" title="Закрыть">&#x2715;</button>
    </div>
    <div id="gt-body"><div class="gt-loading">&#8987; Загрузка...</div></div>
  </div>
</div>

<script>
/* ── Скрываем кнопку "На главную" если страница открыта в iframe ── */
if (window !== window.top) {
  var _bl = document.getElementById('back-home-link');
  if (_bl) _bl.style.display = 'none';
}

let refreshTimer = null;

/* ── Dynamic Scores (session-only, не сохраняется между сессиями) ── */
var dynamicScoresEnabled = false;
// { [serverKey]: { map: string, players: { [name]: number } } }
var _scoreHistory = {};
// Тренды текущего рендера: { [serverKey]: { [playerName]: 'up'|'down' } }
var _currentTrends = {};

function updateScoreHistory(serverKey, mapname, players) {
  var prev = _scoreHistory[serverKey];

  // Числовые счета текущего снимка
  var newScores = {};
  var newTotal = 0;
  players.forEach(function(p) {
    var sc = typeof p.score === 'number' ? p.score : parseFloat(p.score);
    if (!isNaN(sc) && p.name != null) {
      newScores[p.name] = sc;
      newTotal += sc;
    }
  });

  // Нужно ли сбросить историю?
  var shouldReset = false;
  if (!prev) {
    shouldReset = true;
  } else if (prev.map !== mapname) {
    shouldReset = true; // сменилась карта
  } else {
    // Резкое падение суммарного счёта → вероятно новый матч
    var prevTotal = Object.keys(prev.players).reduce(function(s, k) { return s + prev.players[k]; }, 0);
    if (prevTotal > 10 && newTotal < prevTotal * 0.35) {
      shouldReset = true;
    }
  }

  if (shouldReset) {
    _scoreHistory[serverKey] = { map: mapname, players: newScores };
    return {}; // первый снимок — трендов нет
  }

  var trends = {};
  Object.keys(newScores).forEach(function(name) {
    if (Object.prototype.hasOwnProperty.call(prev.players, name)) {
      var diff = newScores[name] - prev.players[name];
      if (diff > 0) trends[name] = { dir: 'up',   delta: diff };
      else if (diff < 0) trends[name] = { dir: 'down', delta: diff };
    }
  });

  _scoreHistory[serverKey] = { map: mapname, players: newScores };
  return trends;
}

/* ── URL sync ── */
const URL_DEFAULTS = {
  game: 'q2', not_empty: '1', ignore_bots: '1',
  players: '', players_sub: '1',
  servers_q: '',
  mod_q: '',
  map_q: '',
  sort: 'default',
  full_width: '0', refresh: '0',
  theme: 'auto',
};

function getState() {
  return {
    game:        document.getElementById('sel-game').value,
    not_empty:   document.getElementById('chk-notempty').checked    ? '1' : '0',
    ignore_bots: document.getElementById('chk-ignorebots').checked  ? '1' : '0',
    players:     document.getElementById('txt-players').value,
    players_sub: document.getElementById('chk-players-sub').checked ? '1' : '0',
    servers_q:   document.getElementById('txt-servers').value,
    mod_q:       document.getElementById('txt-mod').value,
    map_q:       document.getElementById('txt-map').value,
    sort:        document.getElementById('sel-sort').value,
    full_width:  document.getElementById('chk-fullwidth').checked   ? '1' : '0',
    refresh:     document.getElementById('sel-refresh').value,
    theme:       document.getElementById('sel-theme').value,
  };
}

function syncToUrl() {
  const state = getState();
  const p = new URLSearchParams();
  for (const k of Object.keys(state)) {
    if (state[k] !== URL_DEFAULTS[k]) p.set(k, state[k]);
  }
  const qs = p.toString();
  history.replaceState(null, '', '/servers' + (qs ? '?' + qs : ''));
}

function loadFromUrl() {
  const p = new URLSearchParams(location.search);
  if (!p.toString()) return; // нет параметров — не перезаписываем настройки из кук
  if (p.has('game'))        document.getElementById('sel-game').value               = p.get('game');
  if (p.has('not_empty'))   document.getElementById('chk-notempty').checked         = p.get('not_empty') !== '0';
  if (p.has('ignore_bots')) document.getElementById('chk-ignorebots').checked       = p.get('ignore_bots') !== '0';
  if (p.has('players'))     document.getElementById('txt-players').value            = p.get('players');
  if (p.has('players_sub')) document.getElementById('chk-players-sub').checked      = p.get('players_sub') !== '0';
  if (p.has('servers_q'))   document.getElementById('txt-servers').value            = p.get('servers_q');
  if (p.has('mod_q'))       document.getElementById('txt-mod').value                = p.get('mod_q');
  if (p.has('map_q'))       document.getElementById('txt-map').value                = p.get('map_q');
  if (p.has('sort'))        document.getElementById('sel-sort').value               = p.get('sort');
  if (p.has('full_width'))  { document.getElementById('chk-fullwidth').checked      = p.get('full_width') !== '0'; applyFullWidth(); }
  if (p.has('refresh'))     document.getElementById('sel-refresh').value            = p.get('refresh');
  if (p.has('theme'))       { document.getElementById('sel-theme').value            = p.get('theme'); applyTheme(); }
}

/* ── Cookie helpers ── */
const COOKIE_NAME = 'srv_settings';
const COOKIE_DAYS = 365;

function saveSettings() {
  const s = {
    game: document.getElementById('sel-game').value,
    not_empty: document.getElementById('chk-notempty').checked,
    ignore_bots: document.getElementById('chk-ignorebots').checked,
    refresh: document.getElementById('sel-refresh').value,
    players: document.getElementById('txt-players').value,
    players_sub: document.getElementById('chk-players-sub').checked,
    servers: document.getElementById('txt-servers').value,
    mod: document.getElementById('txt-mod').value,
    map: document.getElementById('txt-map').value,
    sort: document.getElementById('sel-sort').value,
    full_width: document.getElementById('chk-fullwidth').checked,
    dynamic_scores: document.getElementById('chk-dynamic-scores').checked,
    autosave: document.getElementById('chk-autosave').checked,
    theme: document.getElementById('sel-theme').value,
  };
  const exp = new Date(Date.now() + COOKIE_DAYS * 86400000).toUTCString();
  document.cookie = COOKIE_NAME + '=' + encodeURIComponent(JSON.stringify(s))
    + '; expires=' + exp + '; path=/; SameSite=Lax';
}

function loadSettings() {
  const match = document.cookie.match(new RegExp('(?:^|; )' + COOKIE_NAME + '=([^;]*)'));
  if (!match) return;
  try {
    const s = JSON.parse(decodeURIComponent(match[1]));
    if (s.game != null)        document.getElementById('sel-game').value               = s.game;
    if (s.not_empty != null)   document.getElementById('chk-notempty').checked         = s.not_empty;
    if (s.ignore_bots != null) document.getElementById('chk-ignorebots').checked       = s.ignore_bots;
    if (s.refresh != null)     document.getElementById('sel-refresh').value            = s.refresh;
    if (s.players != null)     document.getElementById('txt-players').value            = s.players;
    if (s.players_sub != null) document.getElementById('chk-players-sub').checked      = s.players_sub;
    if (s.servers != null)     document.getElementById('txt-servers').value            = s.servers;
    if (s.mod != null)         document.getElementById('txt-mod').value                = s.mod;
    if (s.map != null)         document.getElementById('txt-map').value                = s.map;
    if (s.sort != null)        document.getElementById('sel-sort').value               = s.sort;
    if (s.full_width != null)     document.getElementById('chk-fullwidth').checked       = s.full_width;
    if (s.dynamic_scores != null) document.getElementById('chk-dynamic-scores').checked = s.dynamic_scores;
    if (s.autosave != null)       document.getElementById('chk-autosave').checked        = s.autosave;
    if (s.theme != null)          document.getElementById('sel-theme').value             = s.theme;
    applyFullWidth();
    applyDynamicScores();
    applyTheme();
  } catch(e) {}
}

function maybeAutosave() {
  if (document.getElementById('chk-autosave').checked) saveSettings();
}

/* ── Dynamic Scores toggle ── */
function applyDynamicScores() {
  var enabled = document.getElementById('chk-dynamic-scores').checked;
  if (!enabled && dynamicScoresEnabled) {
    // Сбрасываем историю при выключении
    _scoreHistory = {};
    _currentTrends = {};
  }
  dynamicScoresEnabled = enabled;
}

/* ── Full-width toggle ── */
function applyFullWidth() {
  const fw = document.getElementById('chk-fullwidth').checked;
  const main = document.getElementById('main-content');
  if (fw) main.classList.add('full-width');
  else main.classList.remove('full-width');
}

/* ── Theme ── */
var _themeMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
function applyTheme() {
  var theme = document.getElementById('sel-theme').value;
  document.body.classList.remove('theme-light', 'theme-q2css', 'theme-darkness');
  if (theme === 'light') {
    document.body.classList.add('theme-light');
  } else if (theme === 'q2css') {
    document.body.classList.add('theme-q2css');
  } else if (theme === 'darkness') {
    document.body.classList.add('theme-darkness');
  } else if (theme === 'auto') {
    // авто: светлая если system preference = light (тёмная — по умолчанию).
    // darkness — отдельная опциональная тема, в авто НЕ попадает.
    if (!_themeMediaQuery.matches) document.body.classList.add('theme-light');
  }
  // dark — без класса (тема по умолчанию)
}
_themeMediaQuery.addEventListener('change', function() {
  if (document.getElementById('sel-theme').value === 'auto') applyTheme();
});

/* ── API URL builder ── */
function buildApiUrl() {
  const game = document.getElementById('sel-game').value;
  const notEmpty = document.getElementById('chk-notempty').checked ? '1' : '0';
  const ignoreBots = document.getElementById('chk-ignorebots').checked ? '1' : '0';
  return '/api/servers?game=' + encodeURIComponent(game)
    + '&not_empty=' + notEmpty
    + '&ignore_bots=' + ignoreBots;
}

/* ── Client-side filters (players / servers search) ── */
function applyClientFilters(servers) {
  const playerQuery = document.getElementById('txt-players').value.trim();
  const playerSub = document.getElementById('chk-players-sub').checked;
  const serverQuery = document.getElementById('txt-servers').value.trim();
  const modQuery = document.getElementById('txt-mod').value.trim();
  const mapQuery = document.getElementById('txt-map').value.trim();

  let result = servers;

  if (modQuery) {
    const terms = modQuery.split(',').map(t => t.trim().toLowerCase()).filter(Boolean);
    result = result.filter(function(srv) {
      const gamename = String((srv.serverInfo || {}).gamename || '').toLowerCase();
      return terms.some(function(term) { return gamename.includes(term); });
    });
  }

  if (mapQuery) {
    const terms = mapQuery.split(',').map(t => t.trim().toLowerCase()).filter(Boolean);
    result = result.filter(function(srv) {
      const mapname = String(((srv.serverInfo || {}).mapname || (srv.serverInfo || {}).map) || '').toLowerCase();
      return terms.some(function(term) { return mapname.includes(term); });
    });
  }

  if (playerQuery) {
    const terms = playerQuery.split(',').map(t => t.trim().toLowerCase()).filter(Boolean);
    result = result.filter(function(srv) {
      const players = Array.isArray(srv.serverPlayers) ? srv.serverPlayers : [];
      return terms.some(function(term) {
        return players.some(function(p) {
          const name = String(p.name || '').toLowerCase();
          return playerSub ? name.includes(term) : name === term;
        });
      });
    });
  }

  if (serverQuery) {
    const terms = serverQuery.split(',').map(t => t.trim().toLowerCase()).filter(Boolean);
    result = result.filter(function(srv) {
      const info = srv.serverInfo || {};
      const hostname = String(info.hostname || info.sv_hostname || '').toLowerCase();
      const addrs = Array.isArray(srv.allAddrs) && srv.allAddrs.length > 0
        ? srv.allAddrs
        : [srv.server || ((srv.ip || '') + ':' + (srv.port || ''))];
      return terms.some(function(term) {
        return hostname.includes(term) || addrs.some(function(a) { return a.toLowerCase().includes(term); });
      });
    });
  }

  return result;
}

/* ── Rendering ── */
function pingClass(ping) {
  if (!ping && ping !== 0) return '';
  if (ping < 60) return 'ping-low';
  if (ping < 150) return 'ping-mid';
  return 'ping-high';
}

function renderPlayers(players, trends) {
  if (!players || players.length === 0) {
    return '<div class="no-players">Нет игроков</div>';
  }
  // score может прийти как число или как строка ("12", "Spectator" и т.п.)
  function numericScore(p) {
    if (typeof p.score === 'number') return p.score;
    const n = parseFloat(p.score);
    return isNaN(n) ? null : n;
  }
  const sorted = players.slice().sort(function(a, b) {
    const sa = numericScore(a), sb = numericScore(b);
    const aSpec = sa === null, bSpec = sb === null;
    if (aSpec !== bSpec) return aSpec ? 1 : -1;        // сначала игроки, потом спектаторы
    if (aSpec) return String(a.name || '').localeCompare(String(b.name || '')); // спектаторы — по имени
    return sb - sa;                                     // игроки — по счёту убыванием
  });
  const showTrend = trends !== null && trends !== undefined;
  const rows = sorted.map(function(p) {
    const name = p.name != null ? p.name : '—';
    const score = p.score != null ? p.score : '—';
    const ping = p.ping != null ? p.ping : '—';
    const pc = typeof p.ping === 'number' ? pingClass(p.ping) : '';
    var trendCell = '';
    if (showTrend) {
      var t = trends[p.name];
      if (t) {
        var arrow = t.dir === 'up' ? '▲' : '▼';
        var absDelta = Math.abs(t.delta);
        trendCell = '<td class="score-trend-col"><span class="score-trend ' + t.dir + '">'
          + '<span class="score-trend-arrow">' + arrow + '</span>'
          + '<span class="score-trend-delta">' + absDelta + '</span>'
          + '</span></td>';
      } else {
        trendCell = '<td class="score-trend-col"></td>';
      }
    }
    return '<tr>'
      + '<td class="player-name">' + escHtml(name) + '</td>'
      + '<td class="right score-val">' + escHtml(String(score)) + '</td>'
      + trendCell
      + '<td class="right ping-val ' + pc + '">' + escHtml(String(ping)) + '</td>'
      + '</tr>';
  }).join('');
  const trendTh = showTrend ? '<th class="score-trend-col"></th>' : '';
  return '<table class="players-table">'
    + '<thead><tr><th>Игрок</th><th class="right">Счёт</th>' + trendTh + '<th class="right">Пинг</th></tr></thead>'
    + '<tbody>' + rows + '</tbody>'
    + '</table>';
}

function escHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Ключи serverInfo, которые уже показаны в шапке — не дублируем в деталях
const SKIP_INFO_KEYS = new Set(['hostname', 'sv_hostname', 'mapname', 'map', 'maxclients', 'max_clients', 'gamename', 'serverIp', 'serverPort']);

function renderServerDetails(info) {
  const entries = Object.entries(info).filter(function(kv) { return !SKIP_INFO_KEYS.has(kv[0]); });
  if (entries.length === 0) return '';
  const rows = entries.map(function(kv) {
    return '<span class="srv-kv-key">' + escHtml(kv[0]) + '</span>'
         + '<span class="srv-kv-val">' + escHtml(String(kv[1])) + '</span>';
  }).join('');
  return '<details class="srv-details">'
    + '<summary>Настройки сервера</summary>'
    + '<div class="srv-details-body">' + rows + '</div>'
    + '</details>';
}

function renderCard(srv) {
  const info = srv.serverInfo || {};
  const hostname = info.hostname || info.sv_hostname || srv.server || ((srv.ip || '') + ':' + (srv.port || ''));
  const mapname = info.mapname || info.map || '—';
  const gamename = info.gamename || '';
  const game = srv.game || 'q2';
  const _srvKey = srv.server || ((srv.ip || '') + ':' + (srv.port || ''));
  const _trends = dynamicScoresEnabled ? (_currentTrends[_srvKey] || null) : null;
  const badgeCls = game === 'qw' ? 'badge-qw' : 'badge-q2';
  const badgeTxt = game === 'qw' ? 'QW' : 'Q2';
  const addr = escHtml(srv.server || ((srv.ip || '') + ':' + (srv.port || '')));
  const playersCount = Array.isArray(srv.serverPlayers) ? srv.serverPlayers.length : 0;
  const maxPlayers = info.maxclients || info.max_clients || '?';


  // Все адреса сервера (IP + домен, если есть) — allAddrs приходит из API
  const allAddrs = (Array.isArray(srv.allAddrs) && srv.allAddrs.length > 0)
    ? srv.allAddrs
    : [srv.server || ((srv.ip || '') + ':' + (srv.port || ''))];
  const addrsHtml = allAddrs.map(function(a) {
    return '<span class="srv-addr">' + escHtml(a) + '</span>';
  }).join('<span class="srv-addr-sep">·</span>');

  return '<div class="srv-card">'
    + '<div class="srv-card-head">'
      + '<div class="srv-card-title">' + escHtml(hostname)
        + (srv.ip ? '<button class="btn-gti" data-gt="' + escHtml(srv.ip + ':' + (srv.port || '')) + '">GT<em class="gti-i">i</em></button>' : '')
      + '</div>'
      + '<div class="srv-card-meta">'
        + '<span class="srv-badge ' + badgeCls + '">' + badgeTxt + '</span>'
        + addrsHtml
      + '</div>'
      + '<div class="srv-mod">'
        + (gamename ? '<span class="srv-mod-icon">&#127918;</span>Мод: ' + escHtml(gamename) + '&emsp;' : '')
        + '<span class="srv-map"><span class="srv-map-icon">&#128506;</span>' + escHtml(mapname) + '</span>'
      + '</div>'
    + '</div>'
    + '<div class="srv-card-body">'
      + '<div class="players-header">'
        + '<span class="players-title">Игроки</span>'
        + '<span class="players-count">' + playersCount + ' / ' + escHtml(String(maxPlayers)) + '</span>'
      + '</div>'
      + renderPlayers(srv.serverPlayers, _trends)
      + renderServerDetails(info)
      + (game !== 'qw'
          ? '<button class="btn-q2tv" data-s="' + btoa(unescape(encodeURIComponent(srv.server || ((srv.ip || '') + ':' + (srv.port || ''))))) + '">&#127909; Q2TV (альфа)</button>'
            + '<button class="btn-local" data-addr="' + escHtml((srv.ip || '') + ':' + (srv.port || '')) + '">&#9654; Запустить</button>'
          : '')
    + '</div>'
    + '</div>';
}

/* ── Statistics ── */
function renderStats(servers) {
  if (servers.length === 0) return '';

  function numericScore(p) {
    if (typeof p.score === 'number') return p.score;
    const n = parseFloat(p.score);
    return isNaN(n) ? null : n;
  }

  let totalPlayers = 0, totalSpecs = 0, pingSum = 0, pingCount = 0;
  const mapPlayers = {}, modServers = {};
  let topPlayer = null, topScore = -Infinity;
  let topServer = null, topServerLoad = -1;

  for (const srv of servers) {
    const players = Array.isArray(srv.serverPlayers) ? srv.serverPlayers : [];
    const info = srv.serverInfo || {};
    const mapname = info.mapname || info.map || '';
    const gamename = info.gamename || '';
    const maxclients = parseInt(info.maxclients || info.max_clients || 0, 10);
    let realCount = 0;

    for (const p of players) {
      const sc = numericScore(p);
      if (sc === null) {
        totalSpecs++;
      } else {
        totalPlayers++;
        realCount++;
        if (typeof p.ping === 'number' && p.ping > 0) { pingSum += p.ping; pingCount++; }
        if (topPlayer === null || sc > topScore) {
          topScore = sc;
          topPlayer = { name: p.name, score: sc, server: info.hostname || srv.server || '' };
        }
      }
    }

    if (mapname) mapPlayers[mapname] = (mapPlayers[mapname] || 0) + realCount;
    if (gamename) modServers[gamename] = (modServers[gamename] || 0) + 1;

    if (maxclients > 0) {
      const load = realCount / maxclients;
      if (load > topServerLoad) {
        topServerLoad = load;
        topServer = { name: info.hostname || srv.server || '', players: realCount, max: maxclients };
      }
    }
  }

  const avgPing = pingCount > 0 ? Math.round(pingSum / pingCount) : null;
  const topMapEntry = Object.entries(mapPlayers).sort((a, b) => b[1] - a[1])[0];
  const topModEntry = Object.entries(modServers).sort((a, b) => b[1] - a[1])[0];

  const nums = [
    { val: servers.length, label: 'серверов' },
    { val: totalPlayers,   label: 'игрок' + (totalPlayers % 10 === 1 && totalPlayers % 100 !== 11 ? '' : totalPlayers % 10 >= 2 && totalPlayers % 10 <= 4 && (totalPlayers % 100 < 10 || totalPlayers % 100 >= 20) ? 'а' : 'ов') },
    totalSpecs > 0 ? { val: totalSpecs, label: 'спектатор' + (totalSpecs === 1 ? '' : totalSpecs >= 2 && totalSpecs <= 4 ? 'а' : 'ов') } : null,
    avgPing !== null ? { val: avgPing + ' мс', label: 'средний пинг' } : null,
  ].filter(Boolean);

  const numsHtml = nums.map(function(x) {
    return '<div class="stat-num">'
      + '<span class="stat-val">' + escHtml(String(x.val)) + '</span>'
      + '<span class="stat-key">' + escHtml(x.label) + '</span>'
      + '</div>';
  }).join('');

  const details = [];
  if (topMapEntry) details.push(
    '<span class="stat-detail-item">&#x1F5FA; <span class="stat-detail-label">Карта:</span> <b>' + escHtml(topMapEntry[0]) + '</b>&nbsp;· ' + topMapEntry[1] + ' игр.</span>'
  );
  if (topModEntry) details.push(
    '<span class="stat-detail-item">&#x1F3AE; <span class="stat-detail-label">Мод:</span> <b>' + escHtml(topModEntry[0]) + '</b>&nbsp;· ' + topModEntry[1] + ' серв.</span>'
  );
  if (topPlayer && topScore > -Infinity && topScore > 0) details.push(
    '<span class="stat-detail-item">&#x1F3C6; <span class="stat-detail-label">Рекордсмен:</span> <b>' + escHtml(topPlayer.name) + '</b>&nbsp;· ' + topScore + ' очков</span>'
  );
  if (topServer && topServer.players > 0) details.push(
    '<span class="stat-detail-item">&#x1F525; <span class="stat-detail-label">Самый загруженный:</span> <b>' + escHtml(topServer.name) + '</b>&nbsp;· ' + topServer.players + '/' + topServer.max + '</span>'
  );

  return '<div class="srv-stats">'
    + '<div class="stats-nums">' + numsHtml + '</div>'
    + (details.length > 0 ? '<div class="stats-details">' + details.join('') + '</div>' : '')
    + '</div>';
}

function setStatus(state, text) {
  const dot = document.getElementById('status-dot');
  const txt = document.getElementById('status-text');
  dot.className = 'status-dot ' + state;
  txt.textContent = text;
}

/* ── Sorting ── */
function getSrvPlayerCount(srv) {
  return Array.isArray(srv.serverPlayers) ? srv.serverPlayers.length : 0;
}
function getSrvTotalScore(srv) {
  if (!Array.isArray(srv.serverPlayers)) return 0;
  return srv.serverPlayers.reduce(function(sum, p) {
    var s = typeof p.score === 'number' ? p.score : parseFloat(p.score);
    return sum + (isNaN(s) || s < 0 ? 0 : s);
  }, 0);
}
function applySortOrder(servers) {
  var mode = document.getElementById('sel-sort').value;
  if (mode === 'default') return servers;
  var sorted = servers.slice();
  if (mode === 'players') {
    sorted.sort(function(a, b) { return getSrvPlayerCount(b) - getSrvPlayerCount(a); });
  } else if (mode === 'mod') {
    sorted.sort(function(a, b) {
      var ma = String((a.serverInfo || {}).gamename || '').toLowerCase();
      var mb = String((b.serverInfo || {}).gamename || '').toLowerCase();
      if (ma !== mb) return ma < mb ? -1 : 1;
      return getSrvPlayerCount(b) - getSrvPlayerCount(a);
    });
  } else if (mode === 'map') {
    sorted.sort(function(a, b) {
      var ma = String((a.serverInfo || {}).mapname || '').toLowerCase();
      var mb = String((b.serverInfo || {}).mapname || '').toLowerCase();
      if (ma !== mb) return ma < mb ? -1 : 1;
      return getSrvPlayerCount(b) - getSrvPlayerCount(a);
    });
  } else if (mode === 'score') {
    sorted.sort(function(a, b) { return getSrvTotalScore(b) - getSrvTotalScore(a); });
  }
  return sorted;
}

async function fetchAndRender() {
  setStatus('loading', 'Загрузка...');
  const main = document.getElementById('main-content');
  try {
    const resp = await fetch(buildApiUrl());
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();

    const raw = data.servers || [];

    // Дедупликация по ip:port (страховка, бот уже дедуплицирует по hostname:port)
    const seenIpPort = new Set();
    let servers = raw.filter(function(s) {
      const key = (s.ip || '') + ':' + (s.port || '');
      if (seenIpPort.has(key)) return false;
      seenIpPort.add(key);
      return true;
    });
    servers = applyClientFilters(servers);
    servers = applySortOrder(servers);

    // Обновляем историю счёта и вычисляем тренды (если настройка включена)
    if (dynamicScoresEnabled) {
      var newTrends = {};
      servers.forEach(function(srv) {
        var key = srv.server || ((srv.ip || '') + ':' + (srv.port || ''));
        var map = (srv.serverInfo || {}).mapname || (srv.serverInfo || {}).map || '';
        newTrends[key] = updateScoreHistory(key, map, Array.isArray(srv.serverPlayers) ? srv.serverPlayers : []);
      });
      _currentTrends = newTrends;
    } else {
      _currentTrends = {};
    }

    if (servers.length === 0) {
      main.innerHTML = '<div class="no-servers"><span>&#127918;</span>Серверов не найдено по заданным фильтрам</div>';
    } else {
      main.innerHTML = '<div class="servers-grid">' + servers.map(renderCard).join('') + '</div>'
        + renderStats(servers);
    }

    const now = new Date().toLocaleTimeString('ru-RU');
    setStatus('ok', 'Обновлено в ' + now + ' (' + servers.length + ' серв.)');
  } catch (e) {
    main.innerHTML = '<div class="error-box">Ошибка загрузки данных: ' + escHtml(String(e.message)) + '</div>'
      + '<div class="no-servers"><span>&#10060;</span>Проверьте доступность API</div>';
    setStatus('err', 'Ошибка');
  }
}

function resetTimer() {
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null; }
  const secs = parseInt(document.getElementById('sel-refresh').value, 10);
  if (secs > 0) refreshTimer = setInterval(fetchAndRender, secs * 1000);
}

/* ── Hint tooltip (JS, attached to body to avoid clipping) ── */
(function initHintTooltip() {
  var tip = document.createElement('div');
  tip.className = 'hint-tooltip';
  document.body.appendChild(tip);

  var hideTimer;
  function show(el) {
    clearTimeout(hideTimer);
    tip.textContent = el.getAttribute('data-tip');
    tip.style.opacity = '0';
    tip.style.display = 'block';
    var r = el.getBoundingClientRect();
    var left = r.left;
    if (left + 270 > window.innerWidth - 8) left = window.innerWidth - 278;
    if (left < 8) left = 8;
    tip.style.left = left + 'px';
    tip.style.top  = (r.bottom + 7) + 'px';
    tip.style.opacity = '1';
  }
  function hide() {
    hideTimer = setTimeout(function() { tip.style.opacity = '0'; }, 100);
  }

  document.querySelectorAll('.hint-icon').forEach(function(el) {
    el.addEventListener('mouseenter', function() { show(el); });
    el.addEventListener('mouseleave', hide);
    el.addEventListener('focus',      function() { show(el); });
    el.addEventListener('blur',       hide);
  });
})();

/* ── Event wiring ── */
document.getElementById('btn-refresh').addEventListener('click', fetchAndRender);

/* ── Settings modal ── */
function openSettings() {
  var btn = document.getElementById('btn-settings');
  var rect = btn.getBoundingClientRect();
  var box = document.getElementById('settings-box');
  var boxW = 280;
  var left = Math.min(rect.right - boxW, window.innerWidth - boxW - 8);
  if (left < 8) left = 8;
  box.style.top  = (rect.bottom + 6) + 'px';
  box.style.left = left + 'px';
  document.getElementById('settings-modal').classList.add('active');
}
function closeSettings() {
  document.getElementById('settings-modal').classList.remove('active');
}
document.getElementById('btn-settings').addEventListener('click', function(e) {
  e.stopPropagation();
  if (document.getElementById('settings-modal').classList.contains('active')) {
    closeSettings();
  } else {
    openSettings();
  }
});
document.getElementById('settings-close').addEventListener('click', closeSettings);
document.getElementById('settings-backdrop').addEventListener('click', function(e) {
  if (e.target === this) closeSettings();
});
document.getElementById('btn-save').addEventListener('click', function() {
  saveSettings();
  this.textContent = '✓ Сохранено';
  const btn = this;
  setTimeout(function() { btn.textContent = '\u{1F4BE} Сохранить настройки'; }, 1500);
});

// API-уровень: изменение вызывает перезапрос
['sel-game', 'chk-notempty', 'chk-ignorebots'].forEach(function(id) {
  document.getElementById(id).addEventListener('change', function() {
    syncToUrl(); maybeAutosave(); fetchAndRender();
  });
});

document.getElementById('sel-sort').addEventListener('change', function() {
  syncToUrl(); maybeAutosave(); fetchAndRender();
});

// Текстовые фильтры с дебаунсом
['txt-players', 'txt-servers', 'txt-mod', 'txt-map'].forEach(function(id) {
  let debounce = null;
  document.getElementById(id).addEventListener('input', function() {
    clearTimeout(debounce);
    debounce = setTimeout(function() { syncToUrl(); maybeAutosave(); fetchAndRender(); }, 350);
  });
});

document.getElementById('chk-players-sub').addEventListener('change', function() {
  syncToUrl(); maybeAutosave(); fetchAndRender();
});

document.getElementById('chk-fullwidth').addEventListener('change', function() {
  applyFullWidth(); syncToUrl(); maybeAutosave();
});

document.getElementById('chk-dynamic-scores').addEventListener('change', function() {
  applyDynamicScores(); maybeAutosave(); fetchAndRender();
});

document.getElementById('sel-theme').addEventListener('change', function() {
  applyTheme(); syncToUrl(); maybeAutosave();
});

document.getElementById('chk-autosave').addEventListener('change', function() {
  if (this.checked) saveSettings();
});

document.getElementById('sel-refresh').addEventListener('change', function() {
  syncToUrl(); maybeAutosave(); resetTimer(); fetchAndRender();
});

// Инициализация: если в URL есть параметры — используем только их (шаринг ссылок),
// иначе загружаем сохранённые настройки из кук
if (new URLSearchParams(location.search).toString()) {
  loadFromUrl();
} else {
  loadSettings();
}
applyFullWidth();
applyDynamicScores();
applyTheme();
syncToUrl(); // нормализуем URL с учётом загруженных настроек
resetTimer();
fetchAndRender();

/* ── Q2TV toggle ── */
var Q2TV_STORAGE_KEY = 'q2tv_enabled';
var q2tvEnabled = localStorage.getItem(Q2TV_STORAGE_KEY) === '1';

function applyQ2tvState() {
  var btn = document.getElementById('btn-q2tv-toggle');
  if (q2tvEnabled) {
    document.body.classList.add('q2tv-enabled');
    btn.textContent = '\uD83C\uDFA5 Q2TV \u0432\u043A\u043B\u044E\u0447\u0451\u043D (\u0430\u043B\u044C\u0444\u0430)';
    btn.className = 'ctrl-btn-q2tv-on';
  } else {
    document.body.classList.remove('q2tv-enabled');
    btn.textContent = '\uD83D\uDCF9 \u0412\u043A\u043B\u044E\u0447\u0438\u0442\u044C Q2TV (\u0430\u043B\u044C\u0444\u0430)';
    btn.className = 'ctrl-btn-q2tv-off';
  }
}

/* ── Позиционирование суб-модалей в iframe ──
   В iframe position:fixed покрывает весь iframe-документ, а не видимую
   область родительской страницы. Если iframe высокий (много серверов),
   центр flex-контейнера уходит за пределы экрана. Решение: когда открываем
   Q2TV / Клиент внутри iframe — позиционируем бокс рядом с settings-box,
   который гарантированно виден (он сам открыт кнопкой на экране). */
var IN_IFRAME = (window.self !== window.top);

function positionSubModalBox(boxEl) {
  var settingsBox = document.getElementById('settings-box');
  if (!settingsBox) return;
  var sr = settingsBox.getBoundingClientRect();
  var bw = boxEl.offsetWidth  || 460;
  var bh = boxEl.offsetHeight || 320;
  var cx = sr.left + sr.width  / 2;
  var cy = sr.top  + sr.height / 2;
  var left = Math.max(8, Math.min(cx - bw / 2, window.innerWidth  - bw - 8));
  var top  = Math.max(8, Math.min(cy - bh / 2, window.innerHeight - bh - 8));
  boxEl.style.position = 'fixed';
  boxEl.style.left     = left + 'px';
  boxEl.style.top      = top  + 'px';
  boxEl.style.zIndex   = '10010';
}

function resetSubModalBoxPosition(boxEl) {
  boxEl.style.position = '';
  boxEl.style.left     = '';
  boxEl.style.top      = '';
  boxEl.style.zIndex   = '';
}

document.getElementById('btn-q2tv-toggle').addEventListener('click', function() {
  if (!q2tvEnabled) {
    document.getElementById('q2tv-warn-modal').classList.add('active');
    if (IN_IFRAME) {
      requestAnimationFrame(function() {
        positionSubModalBox(document.getElementById('q2tv-warn-box'));
      });
    }
    return;
  }
  q2tvEnabled = false;
  localStorage.setItem(Q2TV_STORAGE_KEY, '0');
  applyQ2tvState();
});

document.getElementById('q2tv-warn-yes').addEventListener('click', function() {
  document.getElementById('q2tv-warn-modal').classList.remove('active');
  if (IN_IFRAME) resetSubModalBoxPosition(document.getElementById('q2tv-warn-box'));
  q2tvEnabled = true;
  localStorage.setItem(Q2TV_STORAGE_KEY, '1');
  applyQ2tvState();
});

document.getElementById('q2tv-warn-no').addEventListener('click', function() {
  document.getElementById('q2tv-warn-modal').classList.remove('active');
  if (IN_IFRAME) resetSubModalBoxPosition(document.getElementById('q2tv-warn-box'));
});

document.getElementById('q2tv-warn-backdrop').addEventListener('click', function() {
  document.getElementById('q2tv-warn-modal').classList.remove('active');
  if (IN_IFRAME) resetSubModalBoxPosition(document.getElementById('q2tv-warn-box'));
});

applyQ2tvState();

/* ── Local client ── */
var LOCAL_CLIENT_KEY = 'q2_local_client_path';

/* ── Local client: скрыть кнопку на не-Windows или мобилке ── */
(function() {
  var isWindows = /Windows/.test(navigator.userAgent) || /Win/.test(navigator.platform || '');
  if (!isWindows) {
    document.querySelectorAll('.ctrl-group-local').forEach(function(el) { el.style.display = 'none'; });
  }
})();

function isValidExePath(p) {
  return /\.(exe|com|bat|cmd)$/i.test(p.trim());
}

function updateLocalModalBtns() {
  var valid = isValidExePath(document.getElementById('local-modal-path').value);
  document.getElementById('local-modal-dl').style.display   = valid ? '' : 'none';
  document.getElementById('local-modal-save').style.display = valid ? '' : 'none';
}

function applyLocalClientState() {
  var path = localStorage.getItem(LOCAL_CLIENT_KEY);
  var btn = document.getElementById('btn-local-client');
  if (path) {
    document.body.classList.add('local-enabled');
    btn.classList.add('configured');
    btn.textContent = '\uD83D\uDDA5 \u041A\u043B\u0438\u0435\u043D\u0442 \u2713';
  } else {
    document.body.classList.remove('local-enabled');
    btn.classList.remove('configured');
    btn.textContent = '\uD83D\uDDA5 \u041A\u043B\u0438\u0435\u043D\u0442';
  }
}

function openLocalModal() {
  document.getElementById('local-modal-path').value = localStorage.getItem(LOCAL_CLIENT_KEY) || '';
  updateLocalModalBtns();
  document.getElementById('local-modal').classList.add('active');
  if (IN_IFRAME) {
    requestAnimationFrame(function() {
      positionSubModalBox(document.getElementById('local-modal-box'));
    });
  }
}

function closeLocalModal() {
  document.getElementById('local-modal').classList.remove('active');
  if (IN_IFRAME) resetSubModalBoxPosition(document.getElementById('local-modal-box'));
}

function downloadRegFile() {
  var p = document.getElementById('local-modal-path').value.trim();
  if (!p) { alert('\u0423\u043A\u0430\u0436\u0438\u0442\u0435 \u043F\u0443\u0442\u044C \u043A quake2.exe'); return; }
  window.location.href = '/q2-gen-reg?exe=' + encodeURIComponent(p);
}

document.getElementById('btn-local-client').addEventListener('click', openLocalModal);
document.getElementById('local-modal-close').addEventListener('click', closeLocalModal);
document.getElementById('local-modal-backdrop').addEventListener('click', closeLocalModal);

document.getElementById('local-modal-save').addEventListener('click', function() {
  var path = document.getElementById('local-modal-path').value.trim();
  if (!path) { alert('\u0423\u043A\u0430\u0436\u0438\u0442\u0435 \u043F\u0443\u0442\u044C \u043A quake2.exe'); return; }
  localStorage.setItem(LOCAL_CLIENT_KEY, path);
  applyLocalClientState();
  closeLocalModal();
});

document.getElementById('local-modal-path').addEventListener('input', updateLocalModalBtns);
document.getElementById('local-modal-dl').addEventListener('click', downloadRegFile);

document.getElementById('local-modal-clear').addEventListener('click', function() {
  localStorage.removeItem(LOCAL_CLIENT_KEY);
  applyLocalClientState();
  closeLocalModal();
});

document.addEventListener('click', function(e) {
  var btn = e.target.closest('.btn-local');
  if (!btn) return;
  var addr = btn.dataset.addr;
  if (!addr) return;
  window.open('q2launch://' + addr);
});

applyLocalClientState();

/* ── Q2TV modal ── */
var q2tvModal  = document.getElementById('q2tv-modal');
var q2tvIframe = document.getElementById('q2tv-iframe');

var q2tvLoading = document.getElementById('q2tv-loading');
var q2tvLoadBar  = document.getElementById('q2tv-load-bar');
var q2tvLoadText = document.getElementById('q2tv-load-text');
var q2tvIsMobile = /Mobi|Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);

window.addEventListener('message', function(e) {
  if (e.data === 'q2tv-loaded') {
    q2tvLoading.style.display = 'none';
  } else if (e.data && e.data.type === 'q2tv-progress') {
    var pct = e.data.pct || 0;
    q2tvLoadBar.style.width = pct + '%';
    if (e.data.mb && e.data.total) {
      q2tvLoadText.textContent = '\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430 ' + e.data.mb + ' / ' + e.data.total + ' MB\u2026';
    }
  }
});

document.addEventListener('click', function(e) {
  var btn = e.target.closest('.btn-q2tv');
  if (!btn) return;
  var s = btn.dataset.s;
  if (!s) return;
  q2tvLoadBar.style.width = '0%';
  q2tvLoadText.textContent = '\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430...';
  q2tvLoading.style.display = q2tvIsMobile ? 'none' : 'flex';
  q2tvIframe.src = '/q2tv-auto?s=' + encodeURIComponent(s);
  q2tvModal.classList.add('active');
  document.body.style.overflow = 'hidden';

  /* В iframe position:fixed центрируется по всей высоте документа (может быть
     очень большой). Фиксируем позицию явно у верхнего края видимой области. */
  if (IN_IFRAME) {
    window.scrollTo(0, 0);
    requestAnimationFrame(function() {
      var vw = window.innerWidth;
      var w  = Math.min(1024, Math.round(vw * 0.96));
      var h  = 600; // фиксированная высота в px — не зависит от vh iframe
      q2tvBox.style.position  = 'fixed';
      q2tvBox.style.width     = w + 'px';
      q2tvBox.style.height    = h + 'px';
      q2tvBox.style.maxHeight = 'none';
      q2tvBox.style.left      = Math.round((vw - w) / 2) + 'px';
      q2tvBox.style.top       = '10px';
      q2tvBox.style.transform = 'none';
      q2tvBox.style.margin    = '0';
    });
  }

  setTimeout(function() {
    try { q2tvIframe.contentWindow.postMessage({type:'q2tv-fit', w: q2tvIframe.offsetWidth, h: q2tvIframe.offsetHeight}, '*'); } catch(_) {}
    try { q2tvIframe.focus(); } catch(_) {}
    try { q2tvIframe.contentWindow.postMessage('q2tv-focus', '*'); } catch(_) {}
  }, 300);
});

document.getElementById('q2tv-close').addEventListener('click', closeQ2tv);
document.getElementById('q2tv-backdrop').addEventListener('click', closeQ2tv);

document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape' && q2tvModal.classList.contains('active')) closeQ2tv();
});

document.getElementById('q2tv-fullscreen').addEventListener('click', function() {
  if (IN_IFRAME) {
    /* В iframe Fullscreen API блокируется браузером — используем CSS-растяжение на весь viewport */
    if (q2tvBox.classList.contains('fake-fullscreen')) {
      q2tvBox.classList.remove('fake-fullscreen');
    } else {
      q2tvBox.style.cssText = '';  /* сброс ручного позиционирования перед fake-fullscreen */
      q2tvBox.classList.add('fake-fullscreen');
    }
  } else {
    if (q2tvIframe.requestFullscreen) q2tvIframe.requestFullscreen();
    else if (q2tvIframe.webkitRequestFullscreen) q2tvIframe.webkitRequestFullscreen();
  }
});

document.getElementById('q2tv-clearcache').addEventListener('click', function() {
  var base = q2tvIframe.src.split('?')[0].split('&_nc=')[0];
  if (!base) return;
  var nc = Date.now();
  // Очищаем SW-кэш если есть, затем перезагружаем iframe
  if ('caches' in window) {
    caches.keys().then(function(names) {
      return Promise.all(names.map(function(n) { return caches.delete(n); }));
    }).then(function() {
      q2tvIframe.src = base + (base.indexOf('?') >= 0 ? '&' : '?') + '_nc=' + nc;
    });
  } else {
    q2tvIframe.src = base + (base.indexOf('?') >= 0 ? '&' : '?') + '_nc=' + nc;
  }
  // Показываем loading только на десктопе
  if (!q2tvIsMobile) {
    q2tvLoadBar.style.width = '0%';
    q2tvLoadText.textContent = '\u0417\u0430\u0433\u0440\u0443\u0437\u043a\u0430...';
    q2tvLoading.style.display = 'flex';
  }
});

// Фиксируем высоту модала через JS (dvh ненадёжен на Android)
var q2tvBox = document.getElementById('q2tv-box');
function fixQ2tvBoxHeight() {
  if (q2tvIsMobile) {
    q2tvBox.style.height = window.innerHeight + 'px';
  }
}
window.addEventListener('resize', fixQ2tvBoxHeight);
fixQ2tvBoxHeight();

/* ── Drag Q2TV окна за заголовок ── */
(function() {
  var header = document.getElementById('q2tv-header');
  var dragging = false;
  var startX, startY, origLeft, origTop;

  header.addEventListener('mousedown', function(e) {
    if (e.target.closest('button')) return; // кнопки в шапке не триггерят drag
    if (q2tvBox.classList.contains('fake-fullscreen')) return; // в fake-fullscreen drag не нужен
    // Фиксируем текущую позицию бокса
    var rect = q2tvBox.getBoundingClientRect();
    q2tvBox.style.position  = 'fixed';
    q2tvBox.style.left      = rect.left + 'px';
    q2tvBox.style.top       = rect.top  + 'px';
    q2tvBox.style.width     = rect.width  + 'px';
    q2tvBox.style.height    = rect.height + 'px';
    q2tvBox.style.maxHeight = 'none';
    q2tvBox.style.transform = 'none';
    q2tvBox.style.margin    = '0';
    startX   = e.clientX;
    startY   = e.clientY;
    origLeft = rect.left;
    origTop  = rect.top;
    dragging = true;
    header.classList.add('dragging');
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });

  document.addEventListener('mousemove', function(e) {
    if (!dragging) return;
    q2tvBox.style.left = (origLeft + e.clientX - startX) + 'px';
    q2tvBox.style.top  = (origTop  + e.clientY - startY) + 'px';
  });

  document.addEventListener('mouseup', function() {
    if (!dragging) return;
    dragging = false;
    header.classList.remove('dragging');
    document.body.style.userSelect = '';
  });
})();

/* ── Resize Q2TV окна за края и углы ── */
(function() {
  var MIN_W = 320, MIN_H = 200;
  var resizing = false;
  var dir, startX, startY, origLeft, origTop, origW, origH;

  // Прозрачный оверлей поверх iframe чтобы мышь не "проваливалась" в него
  var overlay = document.createElement('div');
  overlay.style.cssText = 'display:none;position:absolute;inset:0;z-index:15;';
  q2tvBox.appendChild(overlay);

  q2tvBox.addEventListener('mousedown', function(e) {
    var handle = e.target.closest('.q2tv-rh');
    if (!handle) return;
    if (q2tvBox.classList.contains('fake-fullscreen')) return;
    dir = handle.dataset.dir;
    var rect = q2tvBox.getBoundingClientRect();
    // Фиксируем позицию если ещё не fixed
    q2tvBox.style.position  = 'fixed';
    q2tvBox.style.left      = rect.left + 'px';
    q2tvBox.style.top       = rect.top  + 'px';
    q2tvBox.style.width     = rect.width  + 'px';
    q2tvBox.style.height    = rect.height + 'px';
    q2tvBox.style.maxHeight = 'none';
    q2tvBox.style.transform = 'none';
    q2tvBox.style.margin    = '0';
    startX  = e.clientX;
    startY  = e.clientY;
    origLeft = rect.left;
    origTop  = rect.top;
    origW    = rect.width;
    origH    = rect.height;
    resizing = true;
    overlay.style.display = 'block';
    document.body.style.userSelect = 'none';
    e.preventDefault();
    e.stopPropagation();
  });

  document.addEventListener('mousemove', function(e) {
    if (!resizing) return;
    var dx = e.clientX - startX;
    var dy = e.clientY - startY;
    var newLeft = origLeft, newTop = origTop, newW = origW, newH = origH;

    if (dir.indexOf('e') !== -1) newW = Math.max(MIN_W, origW + dx);
    if (dir.indexOf('s') !== -1) newH = Math.max(MIN_H, origH + dy);
    if (dir.indexOf('w') !== -1) {
      newW = Math.max(MIN_W, origW - dx);
      newLeft = origLeft + origW - newW;
    }
    if (dir.indexOf('n') !== -1) {
      newH = Math.max(MIN_H, origH - dy);
      newTop = origTop + origH - newH;
    }

    q2tvBox.style.left   = newLeft + 'px';
    q2tvBox.style.top    = newTop  + 'px';
    q2tvBox.style.width  = newW + 'px';
    q2tvBox.style.height = newH + 'px';
  });

  document.addEventListener('mouseup', function() {
    if (!resizing) return;
    resizing = false;
    overlay.style.display = 'none';
    document.body.style.userSelect = '';
  });
})();

function closeQ2tv() {
  q2tvIframe.src = '';  // обрывает WS-сессию внутри iframe
  q2tvLoading.style.display = 'none';
  q2tvModal.classList.remove('active');
  document.body.style.overflow = '';
  // Сброс ручного позиционирования (iframe-режим / drag) и fake-fullscreen
  q2tvBox.style.cssText = '';
  q2tvBox.classList.remove('fake-fullscreen');
}

/* ── GameTracker modal ── */
var gtModal  = document.getElementById('gt-modal');
var gtBody   = document.getElementById('gt-body');
var gtTitle  = document.getElementById('gt-title');

function closeGtModal() {
  gtModal.classList.remove('active');
  var box = document.getElementById('gt-box');
  box.style.cssText = '';
}

function openGtModal(server) {
  gtTitle.textContent = 'GameTracker — ' + server;
  gtBody.innerHTML = '<div class="gt-loading">&#8987; Загрузка данных...</div>';
  gtModal.classList.add('active');
  if (IN_IFRAME) {
    window.scrollTo(0, 0);
    requestAnimationFrame(function() {
      var box = document.getElementById('gt-box');
      var vw = window.innerWidth;
      var w  = Math.min(960, Math.round(vw * 0.96));
      box.style.position  = 'fixed';
      box.style.width     = w + 'px';
      box.style.maxWidth  = 'none';
      box.style.left      = Math.round((vw - w) / 2) + 'px';
      box.style.top       = '10px';
      box.style.transform = 'none';
      box.style.margin    = '0';
    });
  }
  fetch('/api/gt?s=' + encodeURIComponent(server))
    .then(function(r) { return r.json(); })
    .then(function(d) { renderGtData(d); })
    .catch(function(e) { gtBody.innerHTML = '<div class="gt-error">Ошибка: ' + escHtml(String(e.message)) + '</div>'; });
}

function renderGtData(d) {
  if (d.error) { gtBody.innerHTML = '<div class="gt-error">Ошибка: ' + escHtml(d.error) + '</div>'; return; }
  var statusCls = (d.status || '').toLowerCase() === 'alive' ? 'gt-status-alive' : 'gt-status-dead';
  var h = '<a class="gt-link" href="' + escHtml(d.url) + '" target="_blank" rel="noopener">&#8599; Открыть на GameTracker</a>';

  // Сводка
  h += '<div class="gt-section"><div class="gt-section-title">Сводка</div><div class="gt-info-grid">'
    + '<span class="gt-info-key">Название:</span><span class="gt-info-val">' + escHtml(d.name || d.server) + '</span>'
    + '<span class="gt-info-key">Игра:</span><span class="gt-info-val">Quake 2</span>'
    + '<span class="gt-info-key">Адрес:</span><span class="gt-info-val">' + escHtml(d.server) + '</span>'
    + '<span class="gt-info-key">Статус:</span><span class="gt-info-val ' + statusCls + '">' + escHtml(d.status || '—') + '</span>'
    + '</div></div>';

  // Рейтинг
  if (d.rank) {
    h += '<div class="gt-section"><div class="gt-section-title">Рейтинг сервера</div><div class="gt-info-grid">'
      + '<span class="gt-info-key">Позиция:</span><span class="gt-info-val gt-rank-val">' + escHtml(d.rank) + '</span>';
    if (d.rankHigh) h += '<span class="gt-info-key">Лучшая (30 дн):</span><span class="gt-info-val">' + escHtml(d.rankHigh) + '</span>';
    if (d.rankLow)  h += '<span class="gt-info-key">Худшая (30 дн):</span><span class="gt-info-val">' + escHtml(d.rankLow)  + '</span>';
    h += '</div></div>';
  }

  // Текущая карта
  if (d.currentMap) {
    h += '<div class="gt-section"><div class="gt-section-title">Текущая карта</div>'
      + '<div class="gt-map-wrap">';
    if (d.mapImg) h += '<img class="gt-map-img" src="' + escHtml(d.mapImg) + '" alt="' + escHtml(d.currentMap) + '" loading="lazy">';
    h += '<span class="gt-map-name">' + escHtml(d.currentMap) + '</span>'
      + '</div></div>';
  }

  // Топ игроков
  if (d.players && d.players.length) {
    h += '<div class="gt-section"><div class="gt-section-title">Топ 10 игроков (Online &amp; Offline)</div>'
      + '<table class="gt-players-table"><thead><tr>'
      + '<th>#</th><th>Игрок</th><th>Очки</th><th>Время</th></tr></thead><tbody>';
    d.players.forEach(function(p) {
      var score = parseInt(p.score, 10);
      h += '<tr>'
        + '<td class="gt-rank-cell">'  + escHtml(p.rank) + '</td>'
        + '<td>' + escHtml(p.name) + '</td>'
        + '<td class="gt-score-cell">' + (isNaN(score) ? escHtml(p.score) : score.toLocaleString('ru-RU')) + '</td>'
        + '<td class="gt-time-cell">'  + escHtml(p.time) + '</td>'
        + '</tr>';
    });
    h += '</tbody></table></div>';
  }

  // Графики — свёрнутая секция
  if (d.charts && d.charts.length) {
    h += '<details class="gt-section gt-details"><summary class="gt-section-title gt-summary">Графики</summary><div class="gt-charts">';
    d.charts.forEach(function(c) {
      h += '<div class="gt-chart-wrap"><div class="gt-chart-label">' + escHtml(c.label) + '</div>'
        + '<img src="' + escHtml(c.url) + '" alt="' + escHtml(c.label) + '" loading="lazy"></div>';
    });
    h += '</div></details>';
  }

  gtBody.innerHTML = h;
}

document.getElementById('gt-expand').addEventListener('click', function() {
  var box = document.getElementById('gt-box');
  var expanded = box.classList.toggle('gt-expanded');
  this.innerHTML = expanded ? '&#x2750;' : '&#x26F6;';
  this.title = expanded ? 'Свернуть' : 'Развернуть';
});
document.getElementById('gt-close').addEventListener('click', closeGtModal);
document.getElementById('gt-backdrop').addEventListener('click', closeGtModal);
document.addEventListener('keydown', function(e) {
  if (e.key === 'Escape' && gtModal.classList.contains('active')) closeGtModal();
});
document.addEventListener('click', function(e) {
  var btn = e.target.closest('.btn-gti');
  if (!btn) return;
  e.stopPropagation();
  openGtModal(btn.dataset.gt);
});
</script>
</body>
</html>`;
}

function attachServersRoutes(app) {
  console.log('[servers] attachServersRoutes called');

  // Серверный прокси к API бота — браузер дёргает /api/servers на домене сайта,
  // сервер сам ходит на SERVERS_API (localhost:3001 недоступен браузеру снаружи)
  app.get('/api/servers', async (req, res) => {
    try {
      const qs = new URLSearchParams(req.query).toString();
      const url = SERVERS_API + '/api/servers' + (qs ? '?' + qs : '');
      const upstream = await fetch(url);
      const data = await upstream.json();
      res.status(upstream.status).json(data);
    } catch (err) {
      console.error('[servers] proxy error:', err);
      res.status(502).json({ error: 'API недоступен: ' + err.message });
    }
  });

  app.get('/servers', (req, res) => {
    console.log('[servers] GET /servers');
    res.type('text/html; charset=utf-8').send(buildServersPage());
  });

  // Минимальный favicon — 1x1 прозрачный ICO, чтобы не было 404
  // Структура: ICONDIR(6) + ICONDIRENTRY(16) + BITMAPINFOHEADER(40) + пиксель(4) + маска(4) = 70 байт
  app.get('/favicon.ico', (req, res) => {
    const ico = Buffer.from(
      '000001000100' +                                               // ICONDIR
      '01010000010020003000000016000000' +                          // ICONDIRENTRY: 1x1, 32bpp, 48 bytes, offset 22
      '28000000010000000200000001002000000000000400000000000000000000000000000000000000' + // BITMAPINFOHEADER
      '00000000' +                                                   // пиксель BGRA (прозрачный)
      '00000000',                                                    // маска AND
      'hex'
    );
    res.set('Content-Type', 'image/x-icon');
    res.set('Cache-Control', 'public, max-age=86400');
    res.send(ico);
  });

  // Генерация .reg файла для регистрации протокола q2launch://
  app.get('/q2-gen-reg', (req, res) => {
    const exePath = (req.query.exe || '').trim();
    if (!exePath) return res.status(400).send('Missing exe parameter');
    // Строим команду с сырым путём, затем экранируем для .reg одним проходом
    const rawCmd = "powershell -WindowStyle Hidden -Command \"Start-Process '" + exePath + "' -ArgumentList '+connect',('%1'.substring(11).TrimEnd('\\/'))\"";
    const regCmd = rawCmd.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
    const content =
      'Windows Registry Editor Version 5.00\r\n\r\n' +
      '[HKEY_CLASSES_ROOT\\q2launch]\r\n' +
      '@="URL:Quake2 Launch"\r\n' +
      '"URL Protocol"=""\r\n\r\n' +
      '[HKEY_CLASSES_ROOT\\q2launch\\shell]\r\n\r\n' +
      '[HKEY_CLASSES_ROOT\\q2launch\\shell\\open]\r\n\r\n' +
      '[HKEY_CLASSES_ROOT\\q2launch\\shell\\open\\command]\r\n' +
      '@="' + regCmd + '"\r\n';
    // UTF-16 LE с BOM — максимальная совместимость с regedit
    const bom = Buffer.from([0xFF, 0xFE]);
    const body = Buffer.from(content, 'utf16le');
    res.set('Content-Type', 'application/octet-stream');
    res.set('Content-Disposition', 'attachment; filename="q2launch.reg"');
    res.send(Buffer.concat([bom, body]));
  });

  // GameTracker proxy
  app.get('/api/gt', async (req, res) => {
    const s = (req.query.s || '').trim();
    if (!/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}$/.test(s))
      return res.status(400).json({ error: 'Invalid server' });
    try {
      const html = await gtFetch('https://www.gametracker.com/server_info/' + s + '/');
      const data = gtParse(html, s);
      res.json(data);
    } catch(e) {
      console.error('[gt]', e.message);
      res.status(502).json({ error: e.message });
    }
  });

  // Серверный редирект на нужную версию Q2TV по User-Agent
  app.get('/q2tv-auto', (req, res) => {
    const ua = req.headers['user-agent'] || '';
    const isMobile = /Mobi|Android|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(ua);
    const s = req.query.s || '';
    const qs = s ? '?s=' + encodeURIComponent(s) : '';
    const target = (isMobile ? '/q2tv-mobile/' : '/q2tv/') + qs;
    console.log('[servers] q2tv-auto ua=' + ua.slice(0, 60) + '... → ' + target);
    res.redirect(302, target);
  });
}

module.exports = { attachServersRoutes };
