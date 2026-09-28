// grid.js — Tournament bracket grid page
require('dotenv').config();

const { MongoClient } = require('mongodb');

const MONGODB_URI = process.env.MONGODB_URI || 'BACKUP_REDACTED_SET_LOCALLY';

const SITE_CHAT_ID = (process.env.SITE_CHAT_ID || '')
  .split(',').map(s => s.trim()).filter(Boolean).map(Number).filter(v => !Number.isNaN(v));

// Aliases for back-link (same mapping as site.js: SITE_NAMES order matches SITE_CHAT_ID)
const SITE_NAME_BY_ID = new Map();
(process.env.SITE_NAMES || '').split(',').map(s => s.trim()).filter(Boolean)
  .forEach((alias, i) => { if (SITE_CHAT_ID[i] != null) SITE_NAME_BY_ID.set(SITE_CHAT_ID[i], alias); });

function getSiteNameForChatId(id) {
  return SITE_NAME_BY_ID.get(Number(id)) || null;
}

// ─── Nick aggregation (.env.nicks) ─────────────────────────────────────────
let __NICK_CACHE__ = { mtimeMs: 0, map: {}, aliases: {} };
function loadNickData() {
  try {
    const filePath = require('path').join(process.cwd(), '.env.nicks');
    const st = require('fs').statSync(filePath);
    if (__NICK_CACHE__.mtimeMs === st.mtimeMs) return __NICK_CACHE__;
    const raw = require('fs').readFileSync(filePath, 'utf8');
    const map = Object.create(null);
    const aliases = Object.create(null);
    raw.split(/\r?\n/).forEach(line => {
      const s = String(line || '').trim();
      if (!s || s.startsWith('#') || s.startsWith(';')) return;
      const eq = s.indexOf('=');
      if (eq <= 0) return;
      const canon = s.slice(0, eq).trim();
      const rhs = s.slice(eq + 1).trim();
      if (!canon) return;
      const aliasList = rhs ? rhs.split(',').map(x => x.trim()).filter(Boolean) : [];
      map[String(canon).toLowerCase()] = canon;
      for (const a of aliasList) map[String(a).toLowerCase()] = canon;
      if (aliasList.length > 0) aliases[canon] = aliasList;
    });
    __NICK_CACHE__ = { mtimeMs: st.mtimeMs, map, aliases };
    return __NICK_CACHE__;
  } catch (e) {
    return { map: {}, aliases: {} };
  }
}

let db;
const dbReady = (async () => {
  const client = new MongoClient(MONGODB_URI);
  await client.connect();
  db = client.db();
  console.log('[grid] Connected to MongoDB');
})().catch(e => console.error('[grid] MongoDB connect error:', e));

function esc(s) {
  return String(s || '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

// ─── Helpers ────────────────────────────────────────────────────────────────

function flagImg(country) {
  if (!country) return '<span class="pflag-placeholder"></span>';
  return `<img src="/media/flags/1x1/${esc(String(country).trim().toLowerCase())}.svg" alt="" class="pflag">`;
}

function renderPlayerRow(p, pts, advanced = false, aliases = null) {
  const name = p.nameOrig || p.nameNorm || '';
  const hasPts = pts !== undefined && pts !== null;
  const badgeHtml = (aliases && aliases.length)
    ? `<span class="nick-badge" title="${esc(aliases.join(', '))}">!</span>`
    : '';
  return `<div class="prow${advanced ? ' advanced' : ''}">
      <span class="pflag-wrap">${flagImg(p.country)}</span>
      <span class="pname">${esc(name)}${badgeHtml}</span>
      ${hasPts ? `<span class="ppts">${esc(String(pts))}</span>` : '<span class="ppts-empty"></span>'}
    </div>`;
}

// Строим Map<groupId, results[]> из сырых документов результатов
function buildResultsByGroup(resultDocs) {
  const m = new Map();
  for (const r of (resultDocs || [])) {
    const gid = r.groupId;
    if (!m.has(gid)) m.set(gid, []);
    m.get(gid).push(r);
  }
  return m;
}

function buildGroupCards(stageGroups, ptsMap, pointsType, resultsByGroup, advancedSet = null, stageKey = 'g', nickMap = null, nickAliases = null) {
  const cards = stageGroups
    .map(g => {
      const players = Array.isArray(g.players) ? g.players : [];
      if (!players.length) return '';

      // ── Сортировка, идентичная site.js ──────────────────────────────────
      const arr = players.slice();
      const hasPts = ptsMap && arr.some(p => ptsMap.has(p.nameNorm));

      // Считаем среднюю эффективность по результатам карт для этой группы
      const effAvgByPlayer = new Map();
      if (resultsByGroup) {
        for (const r of (resultsByGroup.get(Number(g.groupId)) || [])) {
          for (const p of (Array.isArray(r.players) ? r.players : [])) {
            if (!p?.nameNorm) continue;
            const eff = Number(p.eff) || 0;
            if (!effAvgByPlayer.has(p.nameNorm)) effAvgByPlayer.set(p.nameNorm, { sum: 0, count: 0 });
            const s = effAvgByPlayer.get(p.nameNorm);
            s.sum += eff; s.count += 1;
          }
        }
      }
      function getEffAvg(nameNorm) {
        const s = effAvgByPlayer.get(nameNorm);
        return (s && s.count) ? s.sum / s.count : Number.NEGATIVE_INFINITY;
      }

      if (hasPts) {
        arr.sort((a, b) => {
          const aHas = ptsMap.has(a.nameNorm), bHas = ptsMap.has(b.nameNorm);
          if (aHas && bHas) {
            const ap = Number(ptsMap.get(a.nameNorm));
            const bp = Number(ptsMap.get(b.nameNorm));
            if (ap !== bp) return pointsType === 1 ? bp - ap : ap - bp;
            const ea = getEffAvg(a.nameNorm), eb = getEffAvg(b.nameNorm);
            if (eb !== ea) return eb - ea;
          } else if (aHas) return -1;
          else if (bHas) return 1;
          return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
        });
      } else {
        arr.sort((a, b) => (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
      }
      // ────────────────────────────────────────────────────────────────────

      const rows = arr.map(p => {
        const advanced = !!(advancedSet && advancedSet.has(p.nameNorm));
        let aliases = null;
        if (nickMap && nickAliases) {
          const key = String(p.nameNorm || p.nameOrig || '').toLowerCase();
          const canon = key ? nickMap[key] : null;
          if (canon && nickAliases[canon] && nickAliases[canon].length) aliases = nickAliases[canon];
        }
        return renderPlayerRow(p, ptsMap && ptsMap.get(p.nameNorm), advanced, aliases);
      }).join('');

      const timeStr = (g.time || '').trim();
      const maps    = Array.isArray(g.maps) ? g.maps.filter(Boolean) : [];
      const hasMeta = !!(timeStr || maps.length);
      const metaId  = `meta-${stageKey}-${g.groupId}`;

      const groupResultList = resultsByGroup ? (resultsByGroup.get(Number(g.groupId)) || []) : [];
      const hasDetails = groupResultList.length > 0;

      const metaHtml = hasMeta ? `<div class="group-meta" id="${esc(metaId)}">
          ${timeStr ? `<div class="group-time"><span class="meta-icon">🕐</span><span class="meta-text">${esc(timeStr)}</span></div>` : ''}
          ${maps.length ? `<div class="group-maps">
            <span class="meta-icon">🗺</span>
            <span class="maps-list">${maps.map(m => `<span class="map-tag">${esc(m)}</span>`).join('')}</span>
          </div>` : ''}
        </div>` : '';

      return `<div class="group-card">
        <div class="group-title-row">
          <div class="group-title-left">
            ${hasMeta ? `<button class="meta-toggle" onclick="toggleMeta('${esc(metaId)}',this)" title="Время и карты">+</button>` : '<span class="meta-toggle-stub"></span>'}
            <span class="group-title-text">Группа ${esc(String(g.groupId))}</span>
          </div>
          ${hasDetails ? `<button class="detail-btn" onclick="showGroupDetails('${esc(stageKey)}',${Number(g.groupId)})">Подробнее</button>` : ''}
        </div>
        ${metaHtml}
        <div class="group-players">${rows}</div>
      </div>`;
    })
    .filter(Boolean);
  return cards.join('');
}

// ─── Page builder ────────────────────────────────────────────────────────────

function buildGridPage({ tournamentName, tournaments, selectedChatId, currentParentId, subsMeta, currentSubId, groups, finals, superfinals, groupPts, finalPts, superPts, groupResultsByGroup, finalResultsByGroup, superResultsByGroup, pointsType, top3 }) {

  const selectorOptions = tournaments.map(t => {
    const sel = t.chatId === selectedChatId ? ' selected' : '';
    return `<option value="${esc(String(t.chatId))}"${sel}>${esc(t.tournamentName || String(t.chatId))}</option>`;
  }).join('');

  const subSelectorHtml = (subsMeta && subsMeta.length > 0)
    ? `<form class="selector-form" method="get" action="/grid">
        <input type="hidden" name="chatId" value="${esc(String(currentParentId))}">
        <label class="selector-label" for="grid-sub">Под-турнир:</label>
        <select id="grid-sub" name="subId" onchange="this.form.submit()" class="selector">
          <option value=""${!currentSubId ? ' selected' : ''}>— все —</option>
          ${subsMeta.map(s => {
            const sel = s.chatId === currentSubId ? ' selected' : '';
            return `<option value="${esc(String(s.chatId))}"${sel}>${esc(s.tournamentName)}</option>`;
          }).join('')}
        </select>
      </form>`
    : '';

  // Sets of players who advanced to the next stage
  function stagePlayerSet(stageGroups) {
    const s = new Set();
    for (const g of stageGroups)
      for (const p of (Array.isArray(g.players) ? g.players : []))
        if (p.nameNorm) s.add(p.nameNorm);
    return s;
  }
  const finalsSet = stagePlayerSet(finals);
  const superSet  = stagePlayerSet(superfinals);
  const top3Set   = new Set(top3.map(p => p.nameNorm).filter(Boolean));

  const { map: nickMap, aliases: nickAliases } = loadNickData();
  const hasNicks = Object.keys(nickAliases).length > 0;
  const nm = hasNicks ? nickMap : null;
  const na = hasNicks ? nickAliases : null;

  const groupCards  = buildGroupCards(groups,      groupPts, pointsType, groupResultsByGroup, finalsSet.size ? finalsSet : null, 'group', nm, na);
  const finalCards  = buildGroupCards(finals,      finalPts, pointsType, finalResultsByGroup,  superSet.size ? superSet  : null, 'final', nm, na);
  const superCards  = buildGroupCards(superfinals, superPts, pointsType, superResultsByGroup,  top3Set.size  ? top3Set   : null, 'super', nm, na);

  const hasGroups = groups.some(g => (g.players || []).length > 0);
  const hasFinals = finals.some(g => (g.players || []).length > 0);
  const hasSuper  = superfinals.some(g => (g.players || []).length > 0);
  const hasPodium = top3.length > 0;

  // Podium
  const medals       = ['👑', '🥈', '🥉'];
  const podiumClass  = ['podium-gold', 'podium-silver', 'podium-bronze'];
  const podiumHtml   = hasPodium
    ? top3.map((p, i) => {
        const name = p.nameOrig || p.nameNorm || '';
        const pts  = superPts.get(p.nameNorm);
        return `<div class="podium-place ${podiumClass[i]}">
          <div class="podium-medal">${medals[i]}</div>
          <div class="podium-player">
            ${flagImg(p.country)}
            <span class="pname">${esc(name)}</span>
            ${pts !== undefined ? `<span class="ppts">${esc(String(pts))}</span>` : ''}
          </div>
        </div>`;
      }).join('')
    : '<div class="stage-empty">рейтинг не задан</div>';

  // Assemble stages with arrows between them
  const stageEls = [];
  function addStage(html) {
    if (stageEls.length) stageEls.push('<div class="stage-arrow"><span>›</span></div>');
    stageEls.push(html);
  }

  function stageCol(colorClass, icon, title, body) {
    return `<div class="stage-col ${colorClass}">
      <div class="stage-header">
        <span class="stage-icon">${icon}</span>
        <span class="stage-title">${esc(title)}</span>
      </div>
      <div class="stage-body">${body || '<div class="stage-empty">нет данных</div>'}</div>
    </div>`;
  }

  if (hasGroups) addStage(stageCol('col-groups', '🎯', 'Квалификации', groupCards));
  if (hasFinals) addStage(stageCol('col-finals', '🏆', 'Финалы', finalCards));
  if (hasSuper)  addStage(stageCol('col-super',  '⚡', 'Суперфинал', superCards));
  if (hasPodium || !hasGroups)
    addStage(`<div class="stage-col col-podium">
      <div class="stage-header"><span class="stage-icon">🏅</span><span class="stage-title">Итоги</span></div>
      <div class="stage-body podium-body">${podiumHtml}</div>
    </div>`);

  const mainContent = stageEls.length
    ? stageEls.join('')
    : '<div class="no-data">Данных для сетки нет. Добавьте группы через /groups, /finals или /superfinal.</div>';

  const selectorHtml = tournaments.length > 1
    ? `<form class="selector-form" method="get" action="/grid">
        <label class="selector-label" for="grid-tid">Турнир:</label>
        <select id="grid-tid" name="chatId" onchange="this.form.submit()" class="selector">
          ${selectorOptions}
        </select>
      </form>`
    : '';

  const headerControls = [selectorHtml, subSelectorHtml].filter(Boolean).join('');

  // Serialize results for client-side Подробнее modal
  function serializeRBG(rbg) {
    const obj = {};
    for (const [gid, results] of rbg) {
      obj[String(gid)] = results.map(r => {
        const out = {};
        for (const [k, v] of Object.entries(r)) {
          if (k === '_id') continue;
          out[k] = v;
        }
        return out;
      });
    }
    return obj;
  }
  const gridResultsJson = JSON.stringify({
    group: serializeRBG(groupResultsByGroup),
    final: serializeRBG(finalResultsByGroup),
    super: serializeRBG(superResultsByGroup),
  }).replace(/</g, '\\u003c');

  const currentSubMeta = (currentSubId && subsMeta) ? subsMeta.find(s => s.chatId === currentSubId) : null;
  const parentAlias = getSiteNameForChatId(currentParentId);
  const backHref = (parentAlias ? '/?T=' + encodeURIComponent(parentAlias) : '/?tournamentId=' + encodeURIComponent(String(currentParentId)))
    + (currentSubMeta?.subCode ? '&Sub=' + encodeURIComponent(currentSubMeta.subCode) : '');

  return `<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(tournamentName)} — Сетка</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
  background: linear-gradient(140deg, #080d1a 0%, #0f1629 40%, #0a1020 70%, #111827 100%);
  color: #e2e8f0;
  font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  min-height: 100vh;
}

/* ── Header ── */
.grid-header {
  position: sticky;
  top: 0;
  z-index: 100;
  background: rgba(10, 16, 32, 0.85);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border-bottom: 1px solid rgba(99, 130, 255, 0.2);
  padding: 12px 24px;
}
.header-inner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  flex-wrap: wrap;
  gap: 10px;
  max-width: 100%;
}
.header-left { display: flex; align-items: center; gap: 12px; }
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
.tournament-name {
  font-size: 17px;
  font-weight: 700;
  color: #e2e8f0;
  letter-spacing: 0.01em;
}
.page-label {
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

/* ── Selector ── */
.selector-form { display: flex; align-items: center; gap: 8px; }
.selector-label { font-size: 13px; color: #94a3b8; white-space: nowrap; }
.selector {
  background: rgba(15, 23, 42, 0.9);
  border: 1px solid rgba(99, 130, 255, 0.3);
  border-radius: 8px;
  color: #cbd5e1;
  padding: 6px 10px;
  font-size: 13px;
  cursor: pointer;
  outline: none;
  transition: border-color 0.15s;
}
.selector:focus { border-color: rgba(99, 130, 255, 0.7); }

/* ── Main layout ── */
.grid-main { padding: 28px 20px 48px; }
.bracket-scroll { overflow-x: auto; padding-bottom: 12px; }
.bracket-inner {
  display: flex;
  align-items: center;
  gap: 0;
  min-width: min-content;
  padding: 4px 4px 8px;
}

/* ── Stage arrow ── */
.stage-arrow {
  display: flex;
  align-items: center;
  padding: 0 6px;
  flex-shrink: 0;
}
.stage-arrow span {
  font-size: 36px;
  color: rgba(148, 163, 184, 0.35);
  line-height: 1;
  user-select: none;
}

/* ── Stage column ── */
.stage-col {
  flex-shrink: 0;
  width: 340px;
  border-radius: 14px;
  overflow: hidden;
  background: rgba(15, 23, 42, 0.6);
  border: 1px solid rgba(255,255,255,0.07);
  box-shadow: 0 4px 24px rgba(0,0,0,0.4);
}

.stage-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 12px 14px;
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.stage-icon { font-size: 16px; flex-shrink: 0; }
.stage-title { flex: 1; }

/* Stage color themes */
.col-groups  { border-top: 3px solid #3b82f6; }
.col-groups  .stage-header { background: linear-gradient(90deg, rgba(59,130,246,0.25) 0%, rgba(59,130,246,0.08) 100%); color: #93c5fd; }

.col-finals  { border-top: 3px solid #f59e0b; }
.col-finals  .stage-header { background: linear-gradient(90deg, rgba(245,158,11,0.25) 0%, rgba(245,158,11,0.08) 100%); color: #fcd34d; }

.col-super   { border-top: 3px solid #a855f7; }
.col-super   .stage-header { background: linear-gradient(90deg, rgba(168,85,247,0.25) 0%, rgba(168,85,247,0.08) 100%); color: #d8b4fe; }

.col-podium  { border-top: 3px solid #f59e0b; width: 320px; }
.col-podium  .stage-header { background: linear-gradient(90deg, rgba(245,158,11,0.3) 0%, rgba(239,68,68,0.1) 100%); color: #fcd34d; }

/* ── Stage body ── */
.stage-body { padding: 10px 10px 12px; display: flex; flex-direction: column; gap: 8px; }
.stage-empty { font-size: 13px; color: #475569; padding: 8px 4px; text-align: center; }
.no-data { color: #64748b; font-size: 14px; padding: 40px 20px; text-align: center; }

/* ── Group card ── */
.group-card {
  border-radius: 10px;
  background: rgba(255,255,255,0.04);
  border: 1px solid rgba(255,255,255,0.08);
  overflow: hidden;
}
.group-title-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 5px 8px 5px 6px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  gap: 6px;
}
.group-title-left {
  display: flex;
  align-items: center;
  gap: 5px;
  min-width: 0;
}
.group-title-text {
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.07em;
  text-transform: uppercase;
  color: #64748b;
}
.meta-toggle {
  flex-shrink: 0;
  width: 18px; height: 18px;
  border-radius: 4px;
  border: 1px solid rgba(99,130,255,0.35);
  background: rgba(99,130,255,0.12);
  color: #93c5fd;
  font-size: 13px;
  line-height: 1;
  cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background 0.15s;
  padding: 0;
}
.meta-toggle:hover { background: rgba(99,130,255,0.25); }
.meta-toggle-stub { display: inline-block; width: 18px; flex-shrink: 0; }
.detail-btn {
  flex-shrink: 0;
  font-size: 11px;
  font-weight: 600;
  color: #93c5fd;
  background: rgba(99,130,255,0.12);
  border: 1px solid rgba(99,130,255,0.3);
  border-radius: 5px;
  padding: 2px 8px;
  cursor: pointer;
  white-space: nowrap;
  transition: background 0.15s;
}
.detail-btn:hover { background: rgba(99,130,255,0.25); }

/* ── Group meta (time + maps) — свёрнуто по умолчанию ── */
.group-meta {
  display: none;
  flex-direction: column;
  gap: 4px;
  padding: 6px 10px 5px;
  border-bottom: 1px solid rgba(255,255,255,0.06);
  background: rgba(255,255,255,0.02);
}
.group-meta.open { display: flex; }
.group-time,
.group-maps {
  display: flex;
  align-items: flex-start;
  gap: 5px;
  font-size: 11.5px;
}
.meta-icon { flex-shrink: 0; font-size: 12px; line-height: 1.5; }
.meta-text { color: #94a3b8; line-height: 1.5; }
.maps-list { display: flex; flex-wrap: wrap; gap: 3px; align-items: center; }
.map-tag {
  display: inline-block;
  font-size: 11px;
  color: #cbd5e1;
  background: rgba(99,130,255,0.12);
  border: 1px solid rgba(99,130,255,0.2);
  border-radius: 4px;
  padding: 1px 6px;
  white-space: nowrap;
  font-family: 'Consolas', 'Courier New', monospace;
}

/* ── Player row ── */
.prow {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 5px 10px;
  transition: background 0.12s;
  border-bottom: 1px solid rgba(255,255,255,0.04);
}
.prow:last-child { border-bottom: none; }
.prow:hover { background: rgba(255,255,255,0.05); }
.prow.advanced { background: rgba(34,197,94,0.10); border-left: 2px solid rgba(34,197,94,0.55); }
.prow.advanced:hover { background: rgba(34,197,94,0.17); }
.prow.advanced .pname { color: #86efac; }

.pflag-wrap { flex-shrink: 0; width: 18px; display: flex; align-items: center; }
.pflag { width: 16px; height: 12px; object-fit: cover; border-radius: 2px; display: block; }
.pflag-placeholder { display: inline-block; width: 16px; height: 12px; }

.pname { flex: 1; font-size: 13px; color: #cbd5e1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; display: flex; align-items: center; gap: 4px; }
.nick-badge {
  display: inline-flex; align-items: center; justify-content: center;
  width: 14px; height: 14px; flex-shrink: 0;
  border-radius: 50%;
  background: rgba(251,191,36,0.18); border: 1px solid rgba(251,191,36,0.4);
  color: #fbbf24; font-size: 9px; font-weight: 800;
  cursor: help;
}
.ppts  { flex-shrink: 0; font-size: 12px; font-weight: 600; color: #94a3b8; background: rgba(255,255,255,0.07); border-radius: 5px; padding: 1px 6px; }
.ppts-empty { flex-shrink: 0; width: 0; }

/* ── Podium ── */
.podium-body { gap: 10px; }

.podium-place {
  border-radius: 10px;
  padding: 10px 12px;
  display: flex;
  align-items: center;
  gap: 10px;
}
.podium-medal { font-size: 22px; flex-shrink: 0; line-height: 1; }
.podium-player {
  display: flex;
  align-items: center;
  gap: 6px;
  flex: 1;
  min-width: 0;
}
.podium-player .pname { font-size: 14px; font-weight: 600; color: #f1f5f9; }

.podium-gold   { background: linear-gradient(90deg, rgba(217,147,6,0.22) 0%, rgba(217,147,6,0.08) 100%); border: 1px solid rgba(217,147,6,0.35); }
.podium-silver { background: linear-gradient(90deg, rgba(148,163,184,0.18) 0%, rgba(148,163,184,0.06) 100%); border: 1px solid rgba(148,163,184,0.25); }
.podium-bronze { background: linear-gradient(90deg, rgba(180,83,9,0.18) 0%, rgba(180,83,9,0.06) 100%);  border: 1px solid rgba(180,83,9,0.25); }

.podium-gold   .pname { color: #fde68a; }
.podium-silver .pname { color: #e2e8f0; }
.podium-bronze .pname { color: #d4a574; }

/* ── Modal ── */
.modal-overlay {
  display: none;
  position: fixed; inset: 0;
  background: rgba(0,0,0,0.65);
  backdrop-filter: blur(4px);
  z-index: 500;
  align-items: center;
  justify-content: center;
  padding: 16px;
}
.modal-overlay.open { display: flex; }
.modal-box {
  background: #0f172a;
  border: 1px solid rgba(99,130,255,0.25);
  border-radius: 14px;
  box-shadow: 0 20px 60px rgba(0,0,0,0.7);
  width: 100%;
  max-width: 680px;
  max-height: 80vh;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}
.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 18px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
  flex-shrink: 0;
}
.modal-title { font-size: 15px; font-weight: 700; color: #e2e8f0; }
.modal-close {
  background: rgba(255,255,255,0.07);
  border: none;
  border-radius: 6px;
  color: #94a3b8;
  font-size: 16px;
  width: 28px; height: 28px;
  cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background 0.15s;
}
.modal-close:hover { background: rgba(255,255,255,0.14); color: #e2e8f0; }
.modal-body {
  overflow-y: auto;
  padding: 16px 18px 20px;
  display: flex;
  flex-direction: column;
  gap: 20px;
}
.modal-body::-webkit-scrollbar { width: 5px; }
.modal-body::-webkit-scrollbar-track { background: rgba(255,255,255,0.03); }
.modal-body::-webkit-scrollbar-thumb { background: rgba(99,130,255,0.3); border-radius: 3px; }
.result-block { display: flex; flex-direction: column; gap: 8px; }
.result-map {
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #64748b;
  padding-bottom: 4px;
  border-bottom: 1px solid rgba(255,255,255,0.07);
}
.result-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
}
.result-table th {
  text-align: right;
  color: #64748b;
  font-weight: 600;
  font-size: 11px;
  letter-spacing: 0.05em;
  padding: 4px 8px;
  border-bottom: 1px solid rgba(255,255,255,0.08);
}
.result-table th.col-name { text-align: left; }
.result-table td {
  text-align: right;
  color: #94a3b8;
  padding: 5px 8px;
  border-bottom: 1px solid rgba(255,255,255,0.04);
}
.result-table td.col-name { text-align: left; color: #cbd5e1; }
.result-table .nick-badge { width: 13px; height: 13px; font-size: 8px; margin-left: 4px; vertical-align: middle; }
.result-table tr:last-child td { border-bottom: none; }
.result-table tr:hover td { background: rgba(255,255,255,0.04); }
.modal-empty { color: #475569; font-size: 13px; text-align: center; padding: 20px 0; }

/* ── Scrollbar ── */
.bracket-scroll::-webkit-scrollbar { height: 6px; }
.bracket-scroll::-webkit-scrollbar-track { background: rgba(255,255,255,0.04); border-radius: 3px; }
.bracket-scroll::-webkit-scrollbar-thumb { background: rgba(99,130,255,0.35); border-radius: 3px; }
.bracket-scroll::-webkit-scrollbar-thumb:hover { background: rgba(99,130,255,0.55); }

/* ── Responsive ── */
@media (max-width: 600px) {
  .grid-main { padding: 16px 10px 32px; }
  .grid-header { padding: 10px 14px; }
  .tournament-name { font-size: 15px; }
  .page-label { display: none; }
  .stage-col { width: 270px; }
  .col-podium { width: 260px; }
}
</style>
</head>
<body>

<header class="grid-header">
  <div class="header-inner">
    <div class="header-left">
      <a href="${backHref}" class="back-link" title="На главную">←</a>
      <span class="tournament-name">${esc(tournamentName)}</span>
      <span class="page-label">Сетка</span>
    </div>
    ${headerControls}
  </div>
</header>

<main class="grid-main">
  <div class="bracket-scroll">
    <div class="bracket-inner">
      ${mainContent}
    </div>
  </div>
</main>

<div id="modal-overlay" class="modal-overlay" onclick="if(event.target===this)hideModal()">
  <div class="modal-box">
    <div class="modal-header">
      <span class="modal-title" id="modal-title">Результаты</span>
      <button class="modal-close" onclick="hideModal()">✕</button>
    </div>
    <div class="modal-body" id="modal-body"></div>
  </div>
</div>

<script>
const GRID_RESULTS = ${gridResultsJson};

function toggleMeta(id, btn) {
  const el = document.getElementById(id);
  if (!el) return;
  const open = el.classList.toggle('open');
  btn.textContent = open ? '−' : '+';
}

function hideModal() {
  document.getElementById('modal-overlay').classList.remove('open');
}

function escHtml(s) {
  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

const FIELD_LABELS = {
  eff:'Эфф', kills:'Убийства', deaths:'Смерти', assists:'Помощи',
  headshots:'HS', score:'Очки', damage:'Урон', ping:'Пинг',
  kd:'K/D', adr:'ADR', rating:'Рейтинг', mvp:'MVP',
  frags:'Frags', fph:'FPH', dgiv:'DGiv', drec:'DRec'
};
const SKIP_FIELDS = new Set(['chatId','groupId','groupRunId','nameNorm','nameOrig','country','_id']);

// Nick alias lookup for modal (uses server-embedded data)
const NICK_MAP     = ${JSON.stringify(hasNicks ? nickMap : {})};
const NICK_ALIASES = ${JSON.stringify(hasNicks ? nickAliases : {})};

function getNickAliasesForName(nameOrig, nameNorm) {
  const key = String(nameNorm || nameOrig || '').toLowerCase();
  if (!key) return null;
  const canon = NICK_MAP[key];
  if (!canon) return null;
  const list = NICK_ALIASES[canon];
  return (list && list.length) ? list : null;
}

function renderResultTable(r, idx) {
  const players = Array.isArray(r.players) ? r.players : [];
  if (!players.length) return '';

  // collect numeric stat fields in consistent order
  const statFields = [];
  for (const p of players) {
    for (const k of Object.keys(p)) {
      if (!SKIP_FIELDS.has(k) && !statFields.includes(k) && typeof p[k] === 'number') {
        statFields.push(k);
      }
    }
  }

  const mapLabel = r.mapName || r.map || ('Игра ' + (idx + 1));
  const sorted = players.slice().sort((a, b) => (Number(b.eff) || 0) - (Number(a.eff) || 0));

  const headerCells = statFields.map(f =>
    '<th>' + escHtml(FIELD_LABELS[f] || f) + '</th>'
  ).join('');

  const rows = sorted.map(p => {
    const cells = statFields.map(f =>
      '<td>' + (p[f] !== undefined ? escHtml(String(p[f])) : '—') + '</td>'
    ).join('');
    const aliases = getNickAliasesForName(p.nameOrig, p.nameNorm);
    const badge = aliases ? '<span class="nick-badge" title="' + escHtml(aliases.join(', ')) + '">!</span>' : '';
    return '<tr><td class="col-name">' + escHtml(p.nameOrig || p.nameNorm || '') + badge + '</td>' + cells + '</tr>';
  }).join('');

  return '<div class="result-block">' +
    '<div class="result-map">' + escHtml(mapLabel) + '</div>' +
    '<table class="result-table">' +
      '<thead><tr><th class="col-name">Игрок</th>' + headerCells + '</tr></thead>' +
      '<tbody>' + rows + '</tbody>' +
    '</table></div>';
}

function showGroupDetails(stageKey, gid) {
  const stageData = GRID_RESULTS[stageKey];
  const results = stageData && stageData[String(gid)];

  const stageNames = { group: 'Квалификация', final: 'Финал', super: 'Суперфинал' };
  document.getElementById('modal-title').textContent =
    (stageNames[stageKey] || stageKey) + ' — Группа ' + gid + ' — Результаты';

  if (!results || !results.length) {
    document.getElementById('modal-body').innerHTML = '<div class="modal-empty">Нет данных</div>';
  } else {
    document.getElementById('modal-body').innerHTML =
      results.map((r, i) => renderResultTable(r, i)).join('');
  }
  document.getElementById('modal-overlay').classList.add('open');
}

document.addEventListener('keydown', e => { if (e.key === 'Escape') hideModal(); });
</script>

</body>
</html>`;
}

// ─── Route ───────────────────────────────────────────────────────────────────

function attachGridRoutes(app) {
  app.get('/grid', async (req, res) => {
    try {
      await dbReady;
      if (!db) return res.status(500).send('DB not initialized');

      const chatsCol = db.collection('chats');
      const parentChats = await chatsCol
        .find({ chatId: { $in: SITE_CHAT_ID } })
        .sort({ tournamentName: 1 })
        .toArray();

      if (!parentChats.length) {
        return res.status(200).type('text/html; charset=utf-8')
          .send('<!DOCTYPE html><html><body><h1>Нет доступных турниров</h1></body></html>');
      }

      // Collect all sub-tournament IDs
      const allSubIds = Array.from(new Set(
        parentChats
          .flatMap(c => Array.isArray(c.subTournaments) ? c.subTournaments : [])
          .filter(v => Number.isFinite(Number(v)))
          .map(v => Number(v))
      ));

      const subChats = allSubIds.length
        ? await chatsCol.find({ chatId: { $in: allSubIds } }).sort({ tournamentName: 1 }).toArray()
        : [];

      // Parse URL params
      const rawTournamentParam = req.query.tournamentId !== undefined ? req.query.tournamentId : req.query.chatId;
      const rawSubParam =
        req.query.SubID !== undefined ? req.query.SubID :
        req.query.SubId !== undefined ? req.query.SubId :
        req.query.subID !== undefined ? req.query.subID :
        req.query.subId !== undefined ? req.query.subId :
        req.query.subid !== undefined ? req.query.subid :
        undefined;

      const parsedTournamentId = (rawTournamentParam !== undefined && rawTournamentParam !== null && rawTournamentParam !== '')
        ? Number(rawTournamentParam) : null;
      const parsedSubId = (rawSubParam !== undefined && rawSubParam !== null && rawSubParam !== '')
        ? Number(rawSubParam) : null;

      // Build lookup maps
      const allSubSet = new Set(allSubIds);
      const parentBySubId = new Map();
      for (const p of parentChats) {
        for (const sid of (Array.isArray(p.subTournaments) ? p.subTournaments : [])) {
          parentBySubId.set(Number(sid), p.chatId);
        }
      }

      // Resolve currentParentId
      let currentParentId = null;
      let currentSubId = null;

      if (Number.isFinite(parsedTournamentId) && SITE_CHAT_ID.includes(parsedTournamentId)) {
        currentParentId = parsedTournamentId;
      } else if (Number.isFinite(parsedTournamentId) && allSubSet.has(parsedTournamentId)) {
        // compatibility: sub chatId passed as tournamentId — resolve to parent
        currentParentId = parentBySubId.get(parsedTournamentId) || SITE_CHAT_ID[0];
        currentSubId = parsedTournamentId;
      } else {
        currentParentId = SITE_CHAT_ID[0] || null;
      }

      const currentParentChat = parentChats.find(c => c.chatId === currentParentId) || parentChats[0];
      if (currentParentChat && currentParentChat.chatId !== currentParentId) {
        currentParentId = currentParentChat.chatId;
      }

      // Validate subId — allowed only if in current parent's subTournaments
      if (Number.isFinite(parsedSubId) && currentParentChat) {
        const allowedSubs = Array.isArray(currentParentChat.subTournaments)
          ? currentParentChat.subTournaments.map(Number) : [];
        if (allowedSubs.includes(parsedSubId)) {
          currentSubId = parsedSubId;
        }
      }

      const effectiveChatId = Number.isFinite(currentSubId) ? currentSubId : currentParentId;

      // Sub-selector meta for current parent
      const parentSubIds = Array.isArray(currentParentChat?.subTournaments)
        ? currentParentChat.subTournaments.map(Number) : [];
      const subsMeta = subChats
        .filter(c => parentSubIds.includes(c.chatId))
        .map(c => ({ chatId: c.chatId, tournamentName: c.tournamentName || String(c.chatId), subCode: c.tournamentSubCode || '' }));

      // Tournament name (effective chat's name)
      const effectiveChat = subChats.find(c => c.chatId === currentSubId) || currentParentChat;
      const tournamentName = effectiveChat?.tournamentName || `Турнир ${effectiveChatId}`;

      const pointsType = effectiveChat?.pointsType ?? currentParentChat?.pointsType ?? 0;

      // Selected chatId in tournament selector = parent
      const selectedChatId = currentParentId;

      // Load groups/finals/superfinals first — нужны runId для загрузки результатов
      const [groups, finals, superfinals] = await Promise.all([
        db.collection('game_groups').find({ chatId: effectiveChatId }).sort({ groupId: 1 }).toArray(),
        db.collection('final_groups').find({ chatId: effectiveChatId }).sort({ groupId: 1 }).toArray(),
        db.collection('super_final_groups').find({ chatId: effectiveChatId }).sort({ groupId: 1 }).toArray(),
      ]);

      // Load все остальные данные параллельно
      const [
        groupPtsDoc, finalPtsDoc, superPtsDoc,
        superRatingDoc,
        allUsers,
        groupResultDocs, finalResultDocs, superResultDocs,
      ] = await Promise.all([
        db.collection('group_points').findOne({ chatId: effectiveChatId }),
        db.collection('final_points').findOne({ chatId: effectiveChatId }),
        db.collection('super_final_points').findOne({ chatId: effectiveChatId }),
        db.collection('super_final_ratings').findOne({ chatId: effectiveChatId }),
        db.collection('users').find({}).toArray(),
        db.collection('group_results').find({ chatId: effectiveChatId }).toArray(),
        db.collection('final_results').find({ chatId: effectiveChatId }).toArray(),
        db.collection('superfinal_results').find({ chatId: effectiveChatId }).toArray(),
      ]);

      // Points maps
      const groupPts = new Map((groupPtsDoc?.points || []).map(p => [p.nameNorm, Number(p.pts)]));
      const finalPts = new Map((finalPtsDoc?.points || []).map(p => [p.nameNorm, Number(p.pts)]));
      const superPts = new Map((superPtsDoc?.points || []).map(p => [p.nameNorm, Number(p.pts)]));

      // User country lookup
      const userCountry = new Map();
      for (const u of allUsers) {
        const norm = (u.nickNorm || u.nick || '').trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
        if (norm && u.country) userCountry.set(norm, String(u.country).toLowerCase());
      }

      // Enrich players with country from users collection
      function enrichPlayers(stageGroups) {
        for (const g of stageGroups) {
          for (const p of (Array.isArray(g.players) ? g.players : [])) {
            if (!p.country && p.nameNorm) {
              p.country = userCountry.get(p.nameNorm) || '';
            }
          }
        }
      }
      enrichPlayers(groups);
      enrichPlayers(finals);
      enrichPlayers(superfinals);

      // Top 3
      const ratingPlayers = Array.isArray(superRatingDoc?.players) ? superRatingDoc.players.slice() : [];
      ratingPlayers.sort((a, b) => Number(a.rank) - Number(b.rank));
      const top3 = ratingPlayers.slice(0, 3).map(p => ({
        ...p,
        country: p.country || userCountry.get(p.nameNorm) || '',
      }));

      const groupResultsByGroup = buildResultsByGroup(groupResultDocs);
      const finalResultsByGroup = buildResultsByGroup(finalResultDocs);
      const superResultsByGroup = buildResultsByGroup(superResultDocs);

      const html = buildGridPage({
        tournamentName,
        tournaments: parentChats,
        selectedChatId,
        currentParentId,
        subsMeta,
        currentSubId,
        groups, finals, superfinals,
        groupPts, finalPts, superPts,
        groupResultsByGroup, finalResultsByGroup, superResultsByGroup,
        pointsType,
        top3,
      });

      res.type('text/html; charset=utf-8').send(html);
    } catch (err) {
      console.error('[grid] Error:', err);
      res.status(500).send('Internal Server Error');
    }
  });
}

module.exports = { attachGridRoutes };
