// site.js
require('dotenv').config();
const express = require('express');
const { MongoClient } = require('mongodb');
const path = require('path');

const MONGODB_URI = process.env.MONGODB_URI;
const SITE_CHAT_ID = process.env.SITE_CHAT_ID; // может быть один ID или несколько через запятую
const SITE_NAMES_RAW = process.env.SITE_NAMES || ''; // имена-алиасы турниров для URL (?T=OpenFFA2025)
const PORT = Number(process.env.SITE_PORT || 3000);
const SCREENSHOTS_DIR = process.env.SCREENSHOTS_DIR || path.resolve(process.cwd(), 'screenshots');
const ANALYTICS_PORT = Number(process.env.SITE_ANALITICS_PORT || 3010);
var PLAYER_STATS_URL = ""; // process.env.PLAYER_STATS_URL || ''; // https://q2.agly.eu/?lang=ru&r=r_6901e479cced6
var PLAYER_STATS_ENABLED = false; // /^(1|true|yes)$/i.test(String(process.env.PLAYER_STATS_ENABLED || ''));

// Параметр для принудительного подключения CSS quake2.com.ru
const FORCE_Q2CSS_PARAM = 'forceQuake2ComRuCSS';
// Параметр для сворачивания всех новостных секций по умолчанию
const COLLAPSE_ALL_PARAM = 'CollapseAll';

// Новый параметр для выбора турнира в URL
const TOURNAMENT_QUERY_PARAM = 'tournamentId';
// новый alias-параметр из SITE_NAMES
const TOURNAMENT_NAME_PARAM = 'T';

// Новый query-параметр для выбора "подтурнира" (subTournament)
const SUB_TOURNAMENT_PARAM = 'Sub';

// Cookies для сохранения пользовательских предпочтений
const Q2CSS_COOKIE = 'qj_q2css';
const COLLAPSE_COOKIE = 'qj_collapse';
const SECTIONS_COOKIE = 'qj_sections'; // порядок главных секций

const SITE_BG_IMAGE = process.env.SITE_BG_IMAGE || '/images/fon1.png';

if (!SITE_CHAT_ID) {
  console.error('SITE_CHAT_ID is required in .env (ID чата/группы/канала Telegram)');
  process.exit(1);
}

// Поддержка множественных ID в SITE_CHAT_ID (через запятую)
function parseAllowedChatIds(raw = '') {
  return String(raw || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(v => Number(v))
    .filter(n => Number.isFinite(n));
}

const ALLOWED_CHAT_IDS = parseAllowedChatIds(SITE_CHAT_ID);
if (ALLOWED_CHAT_IDS.length === 0) {
  console.error('SITE_CHAT_ID must contain at least one valid numeric chat id');
  process.exit(1);
}
const DEFAULT_CHAT_ID = ALLOWED_CHAT_IDS[0];

// Алиасы турниров из SITE_NAMES: порядок соответствует SITE_CHAT_ID
const SITE_NAME_LIST = String(SITE_NAMES_RAW)
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

const SITE_NAME_BY_ID = new Map();
const ID_BY_SITE_NAME = new Map();

ALLOWED_CHAT_IDS.forEach((id, index) => {
  const alias = SITE_NAME_LIST[index];
  if (alias) {
    const normalized = String(alias);
    SITE_NAME_BY_ID.set(id, normalized);
    ID_BY_SITE_NAME.set(normalized.toLowerCase(), id);
  }
});

// Хелперы для соответствия chatId ↔ alias
function getSiteNameForChatId(id) {
  return SITE_NAME_BY_ID.get(Number(id)) || null;
}

function getChatIdForSiteName(name) {
  if (!name) return null;
  const n = ID_BY_SITE_NAME.get(String(name).toLowerCase());
  return (typeof n === 'number' && Number.isFinite(n)) ? n : null;
}

// Стили quake2.com.ru — подключаются только при наличии ?forceQuake2ComRuCSS=1
const QUAKE2_COM_RU_CSS = `
body{ padding-left: 0px;  padding-bottom: 0px;  padding-right: 0px;  padding-top: 0px;}
A {color: #A22C21;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size: 11px;text-decoration: none;}
A:hover {color: #A22C21;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size: 11px;text-decoration: underline;}
FORM {margin: 1px;}
input {color: black;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size: 11px; background-color: #ffffff; border-color: black; padding-left: 3px;  border: 1px solid; }
.text1{ padding-bottom: 3px;  padding-left: 3px;  padding-right: 3px;  padding-top: 3px;}
.text11{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 11px;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 1px;
  padding-bottom: 1px;
  background-color: #FEF1DE;
  color: Black;
  }
.text12{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 11px;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 1px;
  padding-bottom: 1px;
  background-color: #FEF1DE;
  color: #A22C21;
  }
.text13{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 11px;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 1px;
  padding-bottom: 1px;
  background-color: #FAD3BC;
  color: Black;
  }
.text21{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 10px;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 3px;
  padding-bottom: 3px;
  background-color: #FEF1DE;
  }
.text22{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 10px;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 3px;
  padding-bottom: 3px;
  background-color: #FEF1DE;
  color: #A22C21;
  }
.text23{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 2px;
  padding-bottom: 2px;
  background-color: #FAD3BC;
  color: Black;
  font-size: 10px;
  font-weight: bold;
  }
.text24{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 2px;
  padding-bottom: 2px;
  background-color: #FAD3BC;
  color: Black;
  font-size: 10px;
  } 
.text31{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 2px;
  padding-bottom: 2px;
  background-color: #FEF1DE;
  color: Green;
  font-size: 10px;
  }
.text32{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  padding-left: 3px;
  padding-right: 3px;
  padding-top: 2px;
  padding-bottom: 2px;
  background-color: #FEF1DE;
  color: Green;
  font-size: 11px;
  }     
.title11{
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 11px;
  padding-left: 0px;
  padding-right: 0px;
  padding-top: 0px;
  padding-bottom: 0px;
  background-color: #A22C21;
  color: White;
  }
.txt {color:#000000;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;  padding-bottom: 1px;  padding-left: 3px;  padding-right: 3px;  padding-top: 1px;    }

.txt4 {color: #A22C21;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;}
.txt5 {color: White;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;text-align:center;}
.txt6 {color: #000000;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;text-align:right;}
.txt7 {color: red;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;text-align:center;}
.txt8 {color:green;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;text-align:center;}
.txt10 {color:#000000;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:10px;margin-left:3px;margin-right:3px;margin-top:3px;margin-bottom:3px;}
TEXTAREA {color: black;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;background-color: #ffffff;padding-top: 5px;padding-bottom: 5px;padding-right: 5px;padding-left: 5px;  border: 1px solid Black; }
select {font-size: 11px;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;color: #cccccc;background : #ffffff;}
option {font-size: 11px;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;background : #ffffff;color: black;}
.main {border: #000000; border-style: solid; border-top-width: 1px; border-right-width: 1px; border-bottom-width: 1px; border-left-width: 1px;}
.main1 {border: #A22C21; border-style: solid; border-top-width: 1px; border-right-width: 1px; border-bottom-width: 1px; border-left-width: 1px;}
.main2 {border-top:1px Solid Black; border-bottom:1px Solid Black;}
.main3 {
  border: #000000;
  border-style: solid;
  border-top-width: 1px;
  border-right-width: 1px;
  border-bottom-width: 1px;
  border-left-width: 1px;
  margin-right: 0px;
  margin-left: 0px;
  }
.bline {border-bottom:#000000 1px solid; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;}
.bline2 {border-top:#000000 1px solid;}
.bline3 {border-right:1px Solid Black;}
.bline4 {border-bottom:1px Solid Black;border-top:1px Solid Black;border-right:1px Solid Black; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;  background-color: #FEF1DE; }
.bline5 {border-left:1px Solid Black;color:#000000;font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;font-size:11px;text-align:left;}
.bline6 {border-bottom:#000000 1px solid;}
.bor{ background-image: url(/images/pset.gif);  width: 100%; height:1px}
.title1{background-image: url(/images/button/title1.gif);}
.titleblock{
  background-image: url(/images/button/title1.gif);
  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 10px;
  color: #3D3D3D;
}
.radio{ background-color: #FEF1DE ;  border-color: #FEF1DE;  border-width: 1px;  }
.table1{background-color:#FAD3BC;}
.table2{background-color:#FEF1DE;}
.table3{
  background: #A22C21;
}
.tablequote{
  background: #FEECD3;
}
.button1{
  background-image: url(/images/button/2_60_20.gif);
  width: 60px;
  height: 20px;
  background-color: #FEF1DE;
  border: #FEF1DE;
  color: White;
}
.button2{
  background-image: url(/images/button/2_170_20.gif);
  width: 170px;
  height: 20px;
  background-color: #FEF1DE;
  border: #FEF1DE;
  color: White;
}
.tbl1{padding: 1px;}
.tbl2{border: 1px solid Black;}
tr#bline1 { color: Black;  font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;  font-size: 11px;}
tr#bline1 td {border-bottom: 1px solid Black;  }
tr#line1 {font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;  font-size: 11px;  background-color: #FEF1DE;}
tr#line1 td {
  padding-left: 5px;
  padding-right: 5px;
  padding-bottom: 0px;
  padding-top: 0px;
}
tr#line11 {font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;  font-size: 11px;  background-color: #FAD3BC;}
tr#line11 td {
  padding-left: 5px;
  padding-right: 5px;
  padding-bottom: 0px;
  padding-top: 0px;
}
tr#line12 td {
  padding-left: 5px;
  padding-right: 5px;
  padding-bottom: 5px;
  padding-top: 5px;
}
.head_image {background-image: url(/images/head_1_1002_qj.jpg);}
`;

if (!MONGODB_URI) {
  console.error('MONGODB_URI is required in .env');
  process.exit(1);
}
if (!SITE_CHAT_ID) {
  console.error('SITE_CHAT_ID is required in .env (ID чата/группы/канала Telegram)');
  process.exit(1);
}
const CHAT_ID = Number(SITE_CHAT_ID);

let db;
let colChats, colGameGroups, colFinalGroups, colSuperFinalGroups, colScreenshots;
let colGroupPoints, colFinalPoints, colSuperFinalPoints;
let colNews;
let colPlayerRatings, colFinalRatings, colSuperFinalRatings;
let colCustomGroups, colCustomPoints;   // NEW: кастомные группы/очки
let colAchievements;                    // NEW: ачивки
let colMaps;                            // NEW: карты
// НОВОЕ: результаты карт по стадиям
let colGroupResults;       // коллекция group_results
let colFinalResults;       // коллекция final_results
let colSuperFinalResults;  // коллекция superfinal_results
// НОВОЕ: глобальные коллекции пользователей, команд и регистрации
let colUsers;
let colTeams;
let colRegistrationSettings;
let colSignups;

function escapeHtml(s = '') {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function escapeAttr(s = '') {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Булевый query-параметр: 1|true|yes|on -> true
function getBoolQuery(req, name, def = false) {
  const raw = req.query?.[name];
  if (raw === undefined) return def;
  return /^(1|true|yes|on)$/i.test(String(raw));
}

function getBoolCookie(req, name, def = false) {
  const raw = req.headers?.cookie || '';
  if (!raw) return def;
  const pairs = raw.split(';').map(s => s.trim()).filter(Boolean);
  for (const p of pairs) {
    const idx = p.indexOf('=');
    if (idx === -1) continue;
    const k = decodeURIComponent(p.slice(0, idx).trim());
    if (k !== name) continue;
    const v = decodeURIComponent(p.slice(idx + 1).trim());
    return /^(1|true|yes|on)$/i.test(v);
  }
  return def;
}

function getCookieValue(req, name) {
  const raw = req.headers?.cookie || '';
  if (!raw) return null;
  const pairs = raw.split(';').map(s => s.trim()).filter(Boolean);
  for (const p of pairs) {
    const idx = p.indexOf('=');
    if (idx === -1) continue;
    const k = decodeURIComponent(p.slice(0, idx).trim());
    if (k !== name) continue;
    return decodeURIComponent(p.slice(idx + 1).trim());
  }
  return null;
}

function parseSectionsOrderCookie(req) {
  const raw = getCookieValue(req, SECTIONS_COOKIE);
  if (!raw) return [];
  return raw.split(',').map(s => s.trim()).filter(Boolean);
}

function linkify(text = '') {
  // 1) экранируем
  const escaped = escapeHtml(String(text || ''));

  // 2) первый проход: http/https и #якоря (оставь твой current 're' и коллбэк как есть)
  const re = /(\bhttps?:\/\/[^\s<>"']+)|(^|[\s(])#([A-Za-z][\w-]{0,100})/g;
  const withUrls = escaped.replace(re, (m, url, pre, anchor) => {
    if (url) {
      return `<a href="${url}" target="_blank" rel="noopener">${url}</a>`;
    }
    if (anchor) {
      return `${pre}<a href="#${anchor}">#${anchor}</a>`;
    }
    return m;
  });

  // 3) второй проход: @username → https://t.me/username (не трогаем email адреса)
  //   - Матчим @ после начала строки или НЕ [a-zA-Z0-9_@], чтобы не задевать user@domain
  return withUrls.replace(/(^|[^a-zA-Z0-9_@])@([A-Za-z0-9_]{4,64})\b/g,
    (m, pre, user) => `${pre}<a href="https://t.me/${user}" target="_blank" rel="noopener">@${user}</a>`);
}

function tournamentHasStreams(tournament) {
  const raw = tournament?.streams;
  if (!raw) return false;

  const arr = Array.isArray(raw) ? raw : [raw];

  for (const item of arr) {
    const candidates = [];

    if (typeof item === 'string') {
      candidates.push(item);
    } else if (item && typeof item === 'object') {
      ['url', 'href', 'link'].forEach(k => {
        if (item[k] != null) candidates.push(item[k]);
      });
    }

    for (const v of candidates) {
      const s = String(v || '').trim();
      if (!s) continue;

      // Считаем, что это «нормальный» стрим, если там явно есть ссылка
      if (
        s.includes('http://') ||
        s.includes('https://') ||
        s.includes('twitch.') ||
        s.includes('youtube.') ||
        s.includes('vk.com') ||
        s.includes('rutube.')
      ) {
        return true;
      }
    }
  }

  return false;
}


function renderServersSection(tournament, containerClass, collapsedByDefault = false) {
  const hasServers = Array.isArray(tournament.servers) && tournament.servers.length > 0;
  if (!hasServers) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  return `
    <section class="mb-4">
      <details id="section-servers" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">🖥️ 🖥️ 🖥️ Список серверов</span>
          <a href="#section-servers" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
        </summary>
        <div class="mt-2">
          <div class="card shadow-sm">
            <div class="card-body">
              <ul class="list-unstyled mb-0">
                ${tournament.servers.map(s => `<li class="mb-1"><span class="qj-tag">${escapeHtml(s)}</span></li>`).join('')}
              </ul>
            </div>
          </div>
        </div>
      </details>
    </section>
  `;
}

function renderPackSection(tournament, containerClass, collapsedByDefault = false) {
  const hasPack = Boolean(tournament.pack);
  if (!hasPack) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  const safe = escapeHtml(tournament.pack);
  return `
    <section class="mb-4">
      <details id="section-pack" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">🗂️ Архив с картами</span>
          <a href="#section-pack" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
        </summary>
        <div class="mt-2">
          <div class="card shadow-sm">
            <div class="card-body">
              <a href="${safe}" target="_blank" rel="noopener">${safe}</a>
            </div>
          </div>
        </div>
      </details>
    </section>
  `;
}


function renderMapsListSection(mapsList = [], containerClass, collapsedByDefault = false) {
  if (!Array.isArray(mapsList) || mapsList.length === 0) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  const items = mapsList.map(m => {
    const name = m?.nameOrig || m?.nameNorm || '';
    return `<li class="list-inline-item"><span class="qj-tag qj-map-tag">${escapeHtml(name)}</span></li>`;
  }).join('');

  return `
    <section class="mb-4">
      <details id="section-maps-list" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">🎮 Список карт</span>
          <a href="#section-maps-list" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
        </summary>
        <div class="mt-2">
          <div class="card shadow-sm">
            <div class="card-body">
              <ul class="list-inline mb-0">
                ${items}
              </ul>
            </div>
          </div>
        </div>
      </details>
    </section>
  `;
}

function renderNewsRichText(text = '') {
  // Поддержка markdown-подобных цитат: строки, начинающиеся с "> " или ">"
  const src = String(text || '').replace(/\r\n?/g, '\n');
  const lines = src.split('\n');

  const blocks = [];
  let buf = [];
  let inQuote = false;

  function pushBlock() {
    if (!buf.length) return;

    // Собираем блок и слегка нормализуем «лесенки»:
    // 3+ пустых строк -> 2 (чтобы не раздувать вертикальные отступы)
    const raw = buf.join('\n').replace(/(\n){3,}/g, '\n\n');

    // linkify уже экранирует HTML и превращает URL/якоря в ссылки,
    // linkifyTelegramHandles дополняет @handles -> https://t.me/<handle>
    // Затем переводим \n в <br>, чтобы не зависеть от pre-wrap
    const innerHtml = linkifyTelegramHandles(linkify(raw)).replace(/\n/g, '<br>');

    blocks.push(
      inQuote
        ? `<blockquote class="qj-quote">${innerHtml}</blockquote>`
        : `<div class="qj-paragraph">${innerHtml}</div>`
    );
    buf = [];
  }

  for (const line of lines) {
    const isQuote = /^>\s?/.test(line);
    const stripped = isQuote ? line.replace(/^>\s?/, '') : line;

    if (buf.length && isQuote !== inQuote) {
      pushBlock();
    }
    inQuote = isQuote;
    buf.push(stripped);
  }
  pushBlock();

  return blocks.join('');
}


function renderTopMenu({
  tournament,
  tournamentNews = [],
  groups = [],
  finals = [],
  superfinals = [],
  achievementsAch = [],
  achievementsPerc = [],
  showStats = false,
  showFeedback = false,
  analyticsUrl = '',          // NEW: ссылка на аналитику
}) {
  const raw = [];

  // Есть ли вообще демки у турнира (по группам/финалам/суперфиналам)
  const hasDemos = (() => {
    function has(arr) {
      if (!Array.isArray(arr)) return false;
      for (const g of arr) {
        const urls = Array.isArray(g?.demos)
          ? g.demos.map(u => String(u || '').trim()).filter(Boolean)
          : [];
        if (urls.length) return true;
      }
      return false;
    }
    return has(groups) || has(finals) || has(superfinals);
  })();

  // Новости: якорь на последнюю турнирную новость
  if (Array.isArray(tournamentNews) && tournamentNews.length > 0) {
    const n = tournamentNews[0];
    const nid = (n && n._id && typeof n._id.toString === 'function') ? n._id.toString() : String(n?._id || '');
    if (nid) raw.push({ label: '📰 Новости', href: `#news-${nid}` });
  }

  if (tournament?.desc) raw.push({ label: 'ℹ️ Информация', href: '#section-desc' });
  if (Array.isArray(groups) && groups.length > 0) raw.push({ label: '🎯 Квалификации', href: '#section-groups' });
  if (Array.isArray(finals) && finals.length > 0) raw.push({ label: '🏆 Финал', href: '#section-finals' });
  if (Array.isArray(superfinals) && superfinals.length > 0) raw.push({ label: '👑 Суперфинал', href: '#section-superfinals' });
  // NEW: Аналитика — до Статистики
  if (analyticsUrl) raw.push({ label: '📊 Аналитика', href: analyticsUrl });
  if (showStats) raw.push({ label: '📈 Статистика', href: '#section-stats' });
  if (Array.isArray(achievementsAch) && achievementsAch.length > 0) raw.push({ label: '🏅 Ачивки', href: '#section-achievements' });
  if (Array.isArray(achievementsPerc) && achievementsPerc.length > 0) raw.push({ label: '⚡ Перки', href: '#section-perks' });

  // NEW: кнопка "Демки" — строго после "Перки" и только если есть хотя бы одна ссылка на демо
  if (hasDemos) {
    raw.push({
      label: '🎥 Демки',
      href: '#demos-modal',
      badgeText: '',
      external: false,
      anchor: false,
    });
  }

  if (Array.isArray(tournament?.servers) && tournament.servers.length > 0) raw.push({ label: '🖥️ Сервера', href: '#section-servers' });

  const hasStreams = tournamentHasStreams(tournament);
  if (hasStreams) raw.push({ label: '📺 Стримы', href: '#section-streams' });
  //if (Array.isArray(tournament?.streams) && tournament.streams.length > 0) raw.push({ label: 'Стримы', href: '#section-streams' });
  if (showFeedback) raw.push({ label: '💬 Отзывы', href: '#section-feedback' });

  // НОВОЕ: кнопки для модалок "Игроки / Команды / Заявки"
  raw.push({
    label: '🧍 Игроки',
    href: '#players-modal',
    badgeText: '',
    external: false,
    anchor: false,
  });

  raw.push({
    label: '🛡️ Команды',
    href: '#teams-modal',
    badgeText: '',
    external: false,
    anchor: false,
  });

  raw.push({
    label: '📝 Заявки',
    href: '#signups-modal',
    badgeText: '',
    external: false,
    anchor: false,
  });

  if (!raw.length) return '';

  // Де-дупликация на этапе рендера (страховка)
  const seen = new Set();
  const items = [];
  for (const it of raw) {
    const href = String(it.href || '').trim();
    const label = String(it.label || '').trim();
    if (!href || !label) continue;
    const key = href + '|' + label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    items.push({ href, label });
  }

  const labelsCanonical = items.map(it => it.label.trim().toLowerCase());
  const links = items
    .map(it => `<a class="qj-chip" data-qj-key="${escapeAttr(it.href + '|' + it.label.toLowerCase())}" href="${escapeAttr(it.href)}">${escapeHtml(it.label)}</a>`)
    .join('');

  return `
    <nav class="qj-menu mt-2" id="qj-top-menu">
      <div class="qj-menu-scroll">${links}</div>
    </nav>

    <script>
      (function QJ_MENU_STRONG_DEDUP(){
        if (window.__QJ_MENU_STRONG_DEDUP_READY__) return;
        window.__QJ_MENU_STRONG_DEDUP_READY__ = true;

        // Эталонная последовательность текстов (с сервера)
        var REF = ${JSON.stringify(labelsCanonical)};

        // Помощник: получить "пункты" внутри контейнера — это и <a>, и <li>, и кнопки,
        // любой элемент-строка меню (фильтруем пустые)
        function collectMenuItems(container){
          if (!container) return [];
          // Выбираем широким селектором
          var candidates = container.querySelectorAll('a, li, button, .qj-chip');
          var out = [];
          for (var i=0;i<candidates.length;i++){
            var el = candidates[i];
            // Берём видимый текст
            var txt = (el.textContent || '').replace(/\\s+/g, ' ').trim();
            if (!txt) continue;
            out.push({ el: el, text: txt });
          }
          return out;
        }

        // Нормализация текста
        function norm(s){ return String(s||'').trim().toLowerCase(); }

        // Пытаемся найти в контейнере два подряд идущих одинаковых блока меню и удалить второй
        function dedupeSequentialBlock(container){
          var items = collectMenuItems(container);
          if (items.length < 2*REF.length) return false;

          // Соберём тексты в нижнем регистре
          var texts = items.map(function(x){ return norm(x.text); });

          // Поищем подряд два одинаковых блока длиной REF.length
          var L = REF.length;
          for (var start=0; start + 2*L <= texts.length; start++){
            var blockA = texts.slice(start, start+L);
            var blockB = texts.slice(start+L, start+2*L);

            // Вариант 1: точное совпадение по текстам
            var eqBlocks = true;
            for (var i=0;i<L;i++){ if (blockA[i] !== blockB[i]) { eqBlocks = false; break; } }
            if (!eqBlocks) continue;

            // Доп. проверка: блок совпадает с нашей эталонной последовательностью (по позициям).
            // Это защищает от ложных срабатываний.
            var eqRef = true;
            for (var j=0;j<L;j++){ if (blockA[j] !== norm(REF[j])) { eqRef = false; break; } }
            if (!eqRef) continue;

            // Удаляем второй блок (start+L .. start+2L-1)
            for (var k = start+L; k < start+2*L; k++){
              if (items[k] && items[k].el && items[k].el.parentNode) {
                items[k].el.parentNode.removeChild(items[k].el);
              }
            }
            return true;
          }
          return false;
        }

        // Проходим по всем разумным контейнерам мобильного меню
        function run(){
          var containers = document.querySelectorAll(
            '.offcanvas, .offcanvas-body, .collapse, .navbar-collapse, .mobile-menu, .drawer, .drawer-body, nav, .qj-menu, .qj-menu-scroll, ul, .menu'
          );
          var changed = false;
          for (var i=0;i<containers.length;i++){
            try { changed = dedupeSequentialBlock(containers[i]) || changed; } catch(_){}
          }
          return changed;
        }

        // Запуск при загрузке
        if (document.readyState === 'loading') {
          document.addEventListener('DOMContentLoaded', function(){ try{ run(); }catch(_){} }, { once: true });
        } else {
          try { run(); } catch(_){}
        }

        // На события открытия/показа (Bootstrap и подобные)
        ['show.bs.offcanvas','shown.bs.offcanvas','show.bs.collapse','shown.bs.collapse'].forEach(function(ev){
          document.addEventListener(ev, function(){ setTimeout(run, 0); });
        });

        // На любые клики по возможным триггерам
        document.addEventListener('click', function(e){
          var t = e.target;
          if (!t) return;
          if (t.closest && (t.closest('.navbar-toggler') || t.closest('[data-bs-toggle]') || t.closest('.menu-toggle') || t.closest('.offcanvas-toggle'))) {
            setTimeout(run, 0);
          } else {
            var txt = (t.textContent||'').trim().toLowerCase();
            if (txt === 'меню' || txt === 'menu') setTimeout(run, 0);
          }
        });

        // На ресайз
        window.addEventListener('resize', function(){ setTimeout(run, 0); });

        // На любые мутации DOM — ловим "тихие" дубли
        var mo = new MutationObserver(function(){
          if (window.__qj_menu_seq_timer) clearTimeout(window.__qj_menu_seq_timer);
          window.__qj_menu_seq_timer = setTimeout(run, 25);
        });
        mo.observe(document.documentElement, { childList: true, subtree: true });
      })();
    </script>
  `;
}


// BB-коды для ачивок -> HTML (жирный/курсив/ссылки)
function bbToHtmlAchievements(text = '') {
  let s = String(text || '');

  // Перенос строки по желанию
  s = s.replace(/\[br\s*\/?\]/gi, '<br/>');

  // Жирный/курсив
  s = s.replace(/\[b\]([\s\S]*?)\[\/b\]/gi, '<strong>$1</strong>');
  s = s.replace(/\[i\]([\s\S]*?)\[\/i\]/gi, '<em>$1</em>');

  // [url=...]текст[/url]
  s = s.replace(/\[url=([^\]]+)\]([\s\S]*?)\[\/url\]/gi, (m, href, label) => {
    const h = String(href || '').trim();
    const l = String(label || '');
    return `<a href="${escapeAttr(h)}" target="_blank" rel="noopener">${escapeHtml(l)}</a>`;
  });

  // [url]ссылка[/url]
  s = s.replace(/\[url\]([\s\S]*?)\[\/url\]/gi, (m, href) => {
    const h = String(href || '').trim();
    return `<a href="${escapeAttr(h)}" target="_blank" rel="noopener">${escapeHtml(h)}</a>`;
  });

  return s;
}

// Временное маскирование уже существующих <a>...</a>, чтобы autolink не лез внутрь
function maskExistingAnchors(html = '') {
  const placeholders = [];
  const masked = String(html).replace(/<a\b[^>]*>[\s\S]*?<\/a>/gi, (m) => {
    placeholders.push(m);
    return `__ACH_A_${placeholders.length}__`;
  });
  return { masked, placeholders };
}
function unmaskExistingAnchors(html = '', placeholders = []) {
  return String(html).replace(/__ACH_A_(\d+)__/g, (_, i) => placeholders[Number(i) - 1] || '');
}

// Autolink для http/https и #якорей без предварительного экранирования
function linkifyAchievements(html = '') {
  const re = /(\bhttps?:\/\/[^\s<>"']+)|(^|[\s(])#([A-Za-z][\w-]{0,100})/g;
  return String(html).replace(re, (m, url, pre, anchor) => {
    if (url) {
      const href = escapeAttr(url);
      const text = escapeHtml(url);
      return `<a href="${href}" target="_blank" rel="noopener">${text}</a>`;
    }
    if (anchor) {
      return `${pre}<a href="#${escapeAttr(anchor)}">#${escapeHtml(anchor)}</a>`;
    }
    return m;
  });
}

// Санитайзер: оставляем только <a>, <strong>, <em>, <br>, <iframe> (только безопасные источники)
function sanitizeAchievementHtml(html = '') {
  let s = String(html || '');

  // Удаляем все теги, кроме разрешённых
  s = s.replace(/<\/?(?!a\b|strong\b|em\b|br\b|iframe\b)[a-z][^>]*>/gi, '');

  // Чистим теги strong/em/br от любых атрибутов
  s = s.replace(/<(strong|em|br)\b[^>]*>/gi, (m, tag) => `<${tag}>`);

  // Чистим <a>: оставляем только безопасный href, навешиваем target/rel для внешних ссылок
  s = s.replace(/<a\b([^>]*)>/gi, (m, attrs) => {
    const mHref = attrs.match(/\bhref\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
    let href = mHref ? (mHref[1] || mHref[2] || mHref[3] || '') : '';
    href = String(href).trim();

    // Разрешённые схемы/типы
    const ok = /^(https?:|mailto:|#|\/)/i.test(href) && !/^javascript:/i.test(href);
    href = ok ? href : '#';

    const isAnchor = href.startsWith('#');
    const extra = isAnchor ? '' : ' target="_blank" rel="noopener"';
    return `<a href="${escapeAttr(href)}"${extra}>`;
  });

  // Безопасные <iframe> (YouTube, Twitch, VK Video, RuTube, VK Play)
  s = s.replace(/<iframe\b[^>]*>[\s\S]*?<\/iframe>/gi, (m) => {
    const getAttr = (name) => {
      const re = new RegExp(name + '\\s*=\\s*(?:"([^"]*)"|\'([^\']*)\'|([^\\s>]+))', 'i');
      const mm = m.match(re);
      return mm ? (mm[1] || mm[2] || mm[3] || '') : '';
    };

    // ВАЖНО: src может прийти уже с &amp; — нормализуем перед проверками и финальным выводом
    let src = String(getAttr('src') || '').trim();
    src = src.replace(/&amp;/g, '&');

    const cls = String(getAttr('class') || '').trim();
    const dataChannel = String(getAttr('data-channel') || '').trim();

    const isYouTube = /^https?:\/\/(www\.)?youtube\.com\/embed\/[A-Za-z0-9_-]{6,}/i.test(src);
    const isVkPlay = /^https?:\/\/[^\/]*vkplay/i.test(src);
    const isVkVideo = /^https?:\/\/(www\.)?vk\.com\/video_ext\.php\?/i.test(src);
    const isRuTube = /^https?:\/\/rutube\.ru\/play\/embed\/[A-Za-z0-9]+/i.test(src);
    const isTwitch = !src && /\bjs-twitch-embed\b/.test(cls) && /^[A-Za-z0-9_]{2,30}$/.test(dataChannel);

    if (isYouTube) {
      return `<iframe src="${escapeAttr(src)}" title="Видео YouTube" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
    }

    if (isVkVideo) {
      return `<iframe src="${escapeAttr(src)}" title="Видео VK" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
    }

    if (isRuTube) {
      return `<iframe src="${escapeAttr(src)}" title="Видео RuTube" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
    }

    if (isVkPlay) {
      return `<iframe src="${escapeAttr(src)}" title="Видео" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
    }

    if (isTwitch) {
      return `<iframe class="js-twitch-embed" data-channel="${escapeAttr(dataChannel)}" title="Видео Twitch" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
    }

    // Неизвестный iframe — вырезаем
    return '';
  });

  return s;
}


// Инлайн-вставка видео в HTML новости: <a href="...">...</a> -> <iframe ...>
// Заменяем только те ссылки, где текст ссылки равен самому URL (типичный "голый" URL).
function injectEmbedsIntoNewsHtml(html = '') {
  let s = String(html || '');
  s = s.replace(/<a\b([^>]*)>([\s\S]*?)<\/a>/gi, (m, attrs, label) => {
    const labelText = String(label || '').trim();
    const mHref = attrs.match(/\bhref\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
    const href = (mHref ? (mHref[1] || mHref[2] || mHref[3]) : '' || '').trim();

    // Вставляем плеер только если текст ссылки именно "голый" URL
    if (!href || labelText !== href) return m;

    const iframe = mediaIframeInlineFromUrl(href);
    return iframe || m;
  });
  return s;
}

// Финальный рендер для описания ачивок
function renderAchievementRichText(text = '') {
  // 1) BB -> HTML
  let html = bbToHtmlAchievements(text);

  // 2) Маскируем уже существующие <a>...</a>, чтобы autolink не лез внутрь
  const { masked, placeholders } = maskExistingAnchors(html);

  // 3) Autolink http/https и #якорей
  html = linkifyAchievements(masked);

  // 4) Возвращаем исходные <a>...</a>
  html = unmaskExistingAnchors(html, placeholders);

  // 5) Вставляем видео-превью (YouTube/Twitch/VK) внутрь текста после соответствующих ссылок
  html = injectEmbedsIntoHtml(html);

  // 6) Санитизируем (оставляем только разрешённые теги/атрибуты, iframe только для известных источников)
  html = sanitizeAchievementHtml(html);

  return html;
}

// Раздел (обобщённый): cards по списку элементов
function renderAchievementsSectionTitled(title, sectionId, items = [], collapsedByDefault = false) {
  if (!items?.length) return '';
  const openAttr = collapsedByDefault ? '' : ' open';

  const cards = items.map(a => {
    const id = makeAchievementId(a);
    const titleText = a?.player?.nameOrig
      ? `${a?.name || 'Ачивка'} - ${a.player.nameOrig}`
      : `${a?.name || 'Ачивка'}`;
    const href = `#${escapeHtml(id)}`;
    const imgUrl = a?.image?.relPath ? '/media/' + relToUrl(a.image.relPath) : null;
    const descHtml = renderAchievementRichText(a?.desc || '');

    return `
      <div>
        <details id="${escapeHtml(id)}" class="sub-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">${escapeHtml(titleText)}</span>
            <a href="${href}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на элемент">#</a>
          </summary>
          <div class="mt-2">
            <div class="card shadow-sm">
              <div class="card-body">
                <div class="table-responsive">
                  <table class="table table-borderless align-middle mb-0">
                    <tbody>
                      <tr>
                        <td style="width:120px;">
                          ${imgUrl
        ? `<img src="${escapeHtml(imgUrl)}" alt="${escapeHtml(a?.name || 'ach')}" class="ach-thumb" loading="lazy" />`
        : '<div class="text-muted small">(нет изображения)</div>'}
                        </td>
                        <td>
                          <div class="news-text" style="white-space: pre-wrap;">${descHtml}</div>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </details>
      </div>
    `;
  }).join('');

  return `
    <section class="mb-5">
      <details id="${escapeHtml(sectionId)}" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">${escapeHtml(title)}</span>
          <a href="#${escapeHtml(sectionId)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${items.length}</span>
        </summary>
        <div class="mt-2">
          <!-- ВАЖНО: модификатор cards-grid--ach ограничивает сетку до 2 колонок -->
          <div class="cards-grid cards-grid--ach">${cards}</div>
        </div>
      </details>
    </section>
  `;
}

function renderStreamsSection(tournament, containerClass, collapsedByDefault = false) {
  const hasStreams = Array.isArray(tournament.streams) && tournament.streams.length > 0;
  if (!hasStreams) return '';
  const openAttr = collapsedByDefault ? '' : ' open';

  const items = tournament.streams.map((raw) => {
    const url = String(raw || '').trim();
    const safe = escapeHtml(url);
    const yt = toYouTubeEmbed(url);
    const twitchChan = parseTwitchChannel(url);
    const vk = toVkPlayEmbed(url);
    const vkVideo = toVkVideoEmbed(url);
    const rutube = toRutubeEmbed(url);

    if (yt) {
      return `
        <div class="stream-embed mb-2">
          <iframe data-src="${yt}" title="📺 Трансляция YouTube"
                  loading="lazy" tabindex="-1"
                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                  allowfullscreen
                  class="js-video-iframe"
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>`;
    }
    if (twitchChan) {
      return `
        <div class="stream-embed mb-2">
          <iframe class="js-video-iframe js-twitch-embed"
                  data-channel="${escapeHtml(twitchChan)}"
                  title="📺 Трансляция Twitch"
                  loading="lazy" tabindex="-1"
                  allowfullscreen
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>`;
    }
    if (vkVideo) {
      return `
        <div class="stream-embed mb-2">
          <iframe data-src="${escapeHtml(vkVideo)}" title="📺 Трансляция VK"
                  loading="lazy" tabindex="-1"
                  allowfullscreen
                  class="js-video-iframe"
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>`;
    }
    if (rutube) {
      return `
        <div class="stream-embed mb-2">
          <iframe data-src="${escapeHtml(rutube)}" title="📺 Трансляция RuTube"
                  loading="lazy" tabindex="-1"
                  allowfullscreen
                  class="js-video-iframe"
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>`;
    }
    if (vk) {
      return `
        <div class="stream-embed mb-2">
          <iframe data-src="${escapeHtml(vk)}" title="📺 Трансляция"
                  loading="lazy" tabindex="-1"
                  allowfullscreen
                  class="js-video-iframe"
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>`;
    }
    return `<div class="mb-2"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>`;
  }).join('');

  return `
    <section class="mb-5">
      <details id="section-streams" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">📺 Стримеры</span>
          <a href="#section-streams" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${tournament.streams.length}</span>
        </summary>
        <div class="mt-2">
          <div class="card shadow-sm h-100">
            <div class="card-body">
              ${items || '<div class="text-muted small">(нет)</div>'}
            </div>
          </div>
        </div>
      </details>
    </section>
  `;
}

function renderFeedbackSection(feedbackEntries = [], containerClass, collapsedByDefault = false) {
  const hasFeedback = Array.isArray(feedbackEntries) && feedbackEntries.length > 0;
  if (!hasFeedback) return '';

  const openAttr = collapsedByDefault ? '' : ' open';
  // Функция для рендеринга одного отзыва
  function renderFeedbackItem(f) {
    const ts = f.createdAtMSK; //? formatRuMskDateTime(f.createdAtMSK) : '';
    // Формируем id отзыва и ссылку-якорь
    const fid = f._id?.toString ? f._id.toString() : String(f._id || '');
    const idAttr = fid ? ` id="feedback-${escapeHtml(fid)}"` : '';
    const selfLink = fid ? `<a href="#feedback-${escapeHtml(fid)}" class="ms-2 text-decoration-none" aria-label="Ссылка на отзыв">#</a>` : '';
    // Имя и username автора
    const name = String(f.name || '').trim();
    const username = String(f.username || '').trim();
    let whoText = '';
    if (name) {
      whoText = name;
      //if (username) whoText += ` (${username})`;
    } else if (username) {
      whoText = `${username}`;
    }

    whoText = whoText.replace(/@/g, '');

    // Комбинируем мета-информацию: Имя (@user), Дата
    let metaText = whoText;
    if (ts) {
      metaText += (metaText ? ' - ' : '') + ts;
    }
    const metaHtml = linkify(metaText);
    // Текст отзыва с поддержкой BB-кодов и медиа
    const textHtml = renderAchievementRichText(f.text || '').trim();
    return `
      <li class="list-group-item qj-feedback-item"${idAttr} style="margin-bottom: 1.25rem;">
        <div class="small text-muted">${metaHtml}${selfLink}</div>
        <div class="news-text" style="white-space: pre-wrap;">${textHtml}</div>
      </li>`;
  }

  const itemsHtml = feedbackEntries.map(renderFeedbackItem).join('');
  return `
    <section class="mb-5">
      <details id="section-feedback" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">💬 Отзывы</span>
          <a href="#section-feedback" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${feedbackEntries.length}</span>
        </summary>
        <div class="mt-2">
          <ul class="list-group qj-feedback-list mt-2">
            ${itemsHtml}
          </ul>
        </div>
      </details>
    </section>
  `;
}

// === НОВОЕ: контент модалок "Игроки / Команды / Заявки" ===

function renderUsersModalBody(users = []) {
  if (!Array.isArray(users) || users.length === 0) {
    return '<div class="text-muted small">Пока нет зарегистрированных игроков.</div>';
  }

  const rows = users.map(u => {
    const nick = u.nick || '';
    const bio = u.bio || '';
    const created = u.createdAt ? formatRuMskDateTime(u.createdAt) : '';
    const updated = u.updatedAt ? formatRuMskDateTime(u.updatedAt) : '';

    return `
      <tr>
        <td class="small fw-semibold">
          ${u.country
        ? `<img src="/media/flags/1x1/${escapeHtml(u.country)}.svg" alt="" 
                    style="height:14px; vertical-align:middle; margin-right:4px;">`
        : `<img src="/media/flags/1x1/question.svg" alt="?" 
                    style="height:14px; vertical-align:middle; margin-right:4px;">`}
          ${escapeHtml(nick)}
        </td>
        <td class="small">${escapeHtml(bio || '')}</td>
      </tr>
    `;
  }).join('');

  return `
    <div class="qj-modal-meta small mb-2">Всего игроков: ${users.length}</div>
    <div class="table-responsive qj-modal-shell">
      <table class="table table-sm align-middle qj-table qj-modal-table mb-0 js-sortable-table">
        <thead>
          <tr>
            <th class="small text-secondary" data-sort-type="string">Ник</th>
            <th class="small text-secondary" data-sort-type="string">Описание</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderTeamsModalBody(teams = []) {
  if (!Array.isArray(teams) || teams.length === 0) {
    return '<div class="text-muted small">Пока нет игровых команд.</div>';
  }

  const rows = teams.map(t => {
    const name = t.name || '';
    const desc = t.description || '';
    const members = Array.isArray(t.memberNicks) && t.memberNicks.length
      ? t.memberNicks.join(', ')
      : '(нет участников)';
    const created = t.createdAt ? formatRuMskDateTime(t.createdAt) : '';
    const updated = t.updatedAt ? formatRuMskDateTime(t.updatedAt) : '';

    return `
      <tr>
        <td class="small fw-semibold">${escapeHtml(name)}</td>
        <td class="small">${escapeHtml(desc || '')}</td>
        <td class="small">${escapeHtml(members)}</td>
        <td class="small text-nowrap text-muted qj-col-created">${escapeHtml(created || '')}</td>
        <td class="small text-nowrap text-muted qj-col-updated">${escapeHtml(updated || '')}</td>
      </tr>
    `;
  }).join('');

  return `
    <div class="qj-modal-meta small mb-2">Всего команд: ${teams.length}</div>
    <div class="table-responsive qj-modal-shell">
      <table class="table table-sm align-middle qj-table qj-modal-table mb-0 js-sortable-table">
        <thead>
          <tr>
            <th class="small text-secondary" data-sort-type="string">Команда</th>
            <th class="small text-secondary" data-sort-type="string">Описание</th>
            <th class="small text-secondary" data-sort-type="string">Игроки</th>
            <th class="small text-secondary text-nowrap qj-col-created" data-sort-type="string">Создана</th>
            <th class="small text-secondary text-nowrap qj-col-updated" data-sort-type="string">Обновлена</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}


function renderSignupsModalBody(registrationSettings, signups = []) {
  const hasSettings = !!registrationSettings;
  const hasSignups = Array.isArray(signups) && signups.length > 0;

  if (!hasSettings && !hasSignups) {
    return '<div class="text-muted small">Регистрация на турнир ещё не настроена.</div>';
  }

  let settingsHtml = '';
  let hideMembersCol = false;
  let type = '';

  if (hasSettings) {
    const maxPlayers = registrationSettings.maxPlayers ?? null;
    type = registrationSettings.tournamentType || '';
    const typeNorm = String(type).toLowerCase();

    // НЕ показываем колонку "Состав" для 1v1 и FFA
    hideMembersCol = typeNorm.includes('1v1') || typeNorm.includes('ffa');

    const deadline = registrationSettings.deadline
      ? formatRuMskDateTime(registrationSettings.deadline)
      : '';
    const enabled = !!registrationSettings.registrationEnabled;
    const openedAt = registrationSettings.registrationOpenedAt
      ? formatRuMskDateTime(registrationSettings.registrationOpenedAt)
      : '';
    const closedAt = registrationSettings.registrationClosedAt
      ? formatRuMskDateTime(registrationSettings.registrationClosedAt)
      : '';

    settingsHtml = `
      <div class="card mb-3">
        <div class="card-body py-2">
          <div class="small mb-1">
            <span class="fw-semibold">Тип турнира:</span>
            ${escapeHtml(type || '—')}
          </div>
          <div class="small mb-1">
            <span class="fw-semibold">Тип регистрации:</span> ${escapeHtml(registrationSettings.sgAddMode || '—')}
          </div>
          <div class="small mb-1">
            <span class="fw-semibold">Максимум участников:</span>
            ${maxPlayers !== null ? maxPlayers : '—'}
          </div>
          <div class="small mb-1">
            <span class="fw-semibold">Дедлайн подачи заявок:</span>
            ${escapeHtml(deadline || '—')}
          </div>
          <div class="small mb-1">
            <span class="fw-semibold">Регистрация:</span>
            ${enabled
        ? '<span class="text-success">открыта</span>'
        : '<span class="text-muted">закрыта</span>'}
          </div>
          <div class="small text-muted">
            ${openedAt ? `Открыта: ${escapeHtml(openedAt)}` : ''}
            ${closedAt ? `<br>Закрыта: ${escapeHtml(closedAt)}` : ''}
          </div>
        </div>
      </div>
    `;
  } else {
    // если настроек нет, подстраховка — колонку "Состав" показываем
    hideMembersCol = false;
  }

  let tableHtml = '';
  if (hasSignups) {
    const rows = signups.map(s => {
      const kind = s.kind === 'team' ? 'Командная заявка' : 'Индивидуальная';
      const name = s.teamName || s.playerNick || '';
      const members = Array.isArray(s.teamMembers) && s.teamMembers.length
        ? s.teamMembers.join(', ')
        : (s.playerNick || '—');
      const created = s.createdAt ? formatRuMskDateTime(s.createdAt) : '';
      const confirmed = s.confirmed
        ? '<span class="badge bg-success">подтверждена</span>'
        : '<span class="badge bg-secondary">ожидает</span>';

      return `
        <tr>
          <td class="small fw-semibold">
            ${s.country
          ? `<img src="/media/flags/1x1/${escapeHtml(s.country)}.svg" alt="" 
                      style="height:14px; vertical-align:middle; margin-right:4px;">`
          : `<img src="/media/flags/1x1/question.svg" alt="?" 
                      style="height:14px; vertical-align:middle; margin-right:4px;">`}
            ${escapeHtml(name)}
          </td>
          ${hideMembersCol ? '' : `<td class="small">${escapeHtml(members)}</td>`}
          <td class="small text-nowrap text-muted">${escapeHtml(created || '')}</td>
          <td class="small text-nowrap">${confirmed}</td>
        </tr>
      `;
    }).join('');

    tableHtml = `
      <div class="qj-modal-meta small mb-2">Всего заявок: ${signups.length}</div>
      <div class="table-responsive qj-modal-shell">
        <table class="table table-sm align-middle qj-table qj-modal-table mb-0 js-sortable-table">
          <thead>
            <tr>
              <th class="small text-secondary" data-sort-type="string">Команда / игрок</th>
              ${hideMembersCol ? '' : `<th class="small text-secondary" data-sort-type="string">Состав</th>`}
              <th class="small text-secondary text-nowrap" data-sort-type="string">Создана</th>
              <th class="small text-secondary text-nowrap" data-sort-type="string">Статус</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  } else {
    tableHtml = '<div class="text-muted small">Заявок пока нет.</div>';
  }

  return settingsHtml + tableHtml;
}


function renderDemosModalBody(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="text-muted small">Нет демо-записей для этого турнира.</div>';
  }

  const tbody = rows.map((row, idx) => {
    const stageSafe = escapeHtml(row.stage || '');
    const groupSafe = row.groupId != null ? escapeHtml(String(row.groupId)) : '';

    const playersArr = Array.isArray(row.players) ? row.players : [];
    const playersText = playersArr.length
      ? playersArr.join(', ')
      : '—';
    const playersSafe = escapeHtml(playersText);

    const url = String(row.url || '').trim();
    const urlSafeAttr = escapeAttr(url);
    const urlSafeText = escapeHtml(url);

    return `
      <tr>
        <td class="small text-secondary text-end">${idx + 1}</td>
        <td class="small">${stageSafe}</td>
        <td class="small text-end">${groupSafe}</td>
        <td class="small">${playersSafe}</td>
        <td class="small">
          <a href="${urlSafeAttr}" target="_blank" rel="noopener">${urlSafeText}</a>
        </td>
      </tr>
    `;
  }).join('');

  return `
    <div class="qj-modal-shell">
      <div class="qj-modal-meta small text-muted mb-2">
        Всего демо-записей: ${rows.length}
      </div>
      <div class="table-responsive qj-modal-table-container">
        <table class="table table-sm table-hover align-middle qj-modal-table">
          <thead>
            <tr>
              <th class="small text-secondary text-end" style="width:40px;">#</th>
              <th class="small text-secondary">Стадия</th>
              <th class="small text-secondary text-end" style="width:80px;">№ группы</th>
              <th class="small text-secondary">Игроки группы</th>
              <th class="small text-secondary">Ссылка</th>
            </tr>
          </thead>
          <tbody>${tbody}</tbody>
        </table>
      </div>
    </div>
  `;
}


// Формат даты/времени для России (МСК, 24 часа)
const dtfRU_MSK = new Intl.DateTimeFormat('ru-RU', {
  timeZone: 'Europe/Moscow',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

function formatRuMskDateTime(value) {
  const d = value instanceof Date ? value : new Date(value);
  return dtfRU_MSK.format(d);
}


// тот же формат runId, что в боте
function formatRunId(date) {
  const d = date instanceof Date ? date : new Date(date);
  return d.toISOString().replace(/[:.]/g, '-');
}

function relToUrl(relPath) {
  // безопасное преобразование относительного пути в URL-путь
  return relPath.split(path.sep).map(encodeURIComponent).join('/');
}

// Получение метаданных турниров по списку chatId
async function getTournamentsMeta(chatIds = []) {
  if (!Array.isArray(chatIds) || chatIds.length === 0) return [];
  // Берём имя турнира из коллекции chats (поле tournamentName)
  const docs = await colChats.find(
    { chatId: { $in: chatIds } },
    { projection: { chatId: 1, tournamentName: 1 } }
  ).toArray();

  const nameById = new Map();
  for (const d of docs) {
    if (Number.isFinite(d?.chatId)) {
      nameById.set(Number(d.chatId), String(d?.tournamentName || '').trim());
    }
  }

  return chatIds.map(id => {
    const name = nameById.get(id);
    return { id, name: name || `Чат ${id}` };
  });
}

// Построение контекста подтурниров для текущего chatId + параметра ?Sub=...
// Возвращает:
//   { effectiveChatId, items, rootChatId, rootAlias }
// где items = [{ id, name, code }] — корневой турнир + его подтурниры.
async function buildSubTournamentsContext(chatId, subCodeRaw) {
  const chatIdNum = Number(chatId);
  if (!Number.isFinite(chatIdNum)) {
    return {
      effectiveChatId: chatId,
      items: null,
      rootChatId: null,
      rootAlias: null,
    };
  }

  // Текущий турнир
  const current = await colChats.findOne(
    { chatId: chatIdNum },
    { projection: { chatId: 1, tournamentName: 1, subTournaments: 1 } },
  );

  // Родительский турнир, у которого в subTournaments есть текущий
  const parent = await colChats.findOne(
    { subTournaments: chatIdNum },
    { projection: { chatId: 1, tournamentName: 1, subTournaments: 1 } },
  );

  let root = null;

  if (parent && Array.isArray(parent.subTournaments) && parent.subTournaments.length > 0) {
    // Мы находимся внутри дочернего турнира — корневой это parent
    root = parent;
  } else if (current && Array.isArray(current.subTournaments) && current.subTournaments.length > 0) {
    // Мы на корневом турнире, у которого есть subTournaments
    root = current;
  }

  if (!root) {
    return {
      effectiveChatId: chatIdNum,
      items: null,
      rootChatId: null,
      rootAlias: null,
    };
  }

  const subIds = (root.subTournaments || [])
    .map(x => Number(x))
    .filter(Number.isFinite);

  if (!subIds.length) {
    return {
      effectiveChatId: chatIdNum,
      items: null,
      rootChatId: null,
      rootAlias: null,
    };
  }

  const childDocs = await colChats.find(
    { chatId: { $in: subIds } },
    { projection: { chatId: 1, tournamentName: 1, tournamentSubCode: 1 } },
  ).toArray();

  const byId = new Map();
  const byCode = new Map();

  for (const d of childDocs) {
    const cid = Number(d.chatId);
    if (!Number.isFinite(cid)) continue;

    const name = String(d.tournamentName || '').trim() || `Чат ${cid}`;
    const code = (d.tournamentSubCode || '').toString().trim();

    const item = { id: cid, name, code };
    byId.set(cid, item);
    if (code) byCode.set(code.toLowerCase(), item);
  }

  let effectiveChatId = chatIdNum;
  const subCode = subCodeRaw && String(subCodeRaw).trim()
    ? String(subCodeRaw).trim()
    : '';

  // Если в URL есть ?Sub=КОД — пробуем найти по tournamentSubCode
  if (subCode && byCode.size) {
    const found = byCode.get(subCode.toLowerCase());
    if (found) {
      effectiveChatId = found.id;
    }
  }

  // Формируем список: сначала корневой, потом дети в порядке subIds
  const items = [];
  const rootId = Number(root.chatId);
  const rootName = String(root.tournamentName || '').trim() || `Чат ${rootId}`;

  // Корневой турнир (без кода — означает "нет Sub")
  items.push({ id: rootId, name: rootName, code: '' });

  for (const id of subIds) {
    const it = byId.get(id);
    if (!it) continue;
    items.push(it);
  }

  const rootAlias = getSiteNameForChatId(rootId);

  return {
    effectiveChatId,
    items,
    rootChatId: rootId,
    rootAlias,
  };
}



// Безопасные утилиты для Telegram-ссылок и текстов

function escapeHtml(s = '') {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Нормализуем значение из tournamentNewsChannel в полноценный URL (или null)
function normalizeTelegramLink(input) {
  if (!input) return null;
  let s = String(input).trim();

  // Канал/юзер в виде @handle
  if (s.startsWith('@')) {
    const handle = s.slice(1);
    if (/^[A-Za-z0-9_]{5,32}$/.test(handle)) return `https://t.me/${handle}`;
    return null;
  }

  // Уже ссылка t.me
  if (/^https?:\/\/t\.me\//i.test(s)) return s;
  if (/^t\.me\//i.test(s)) return 'https://' + s;

  // Иначе — не распознали как телеграм-ссылку
  return null;
}

// Отображаемая подпись: всегда в виде @handle, даже если хранилось как URL
function displayTelegramHandle(input) {
  if (!input) return '';
  const s = String(input).trim();
  if (s.startsWith('@')) return s;
  const m = s.match(/^https?:\/\/t\.me\/([A-Za-z0-9_]{5,32})/i);
  if (m) return '@' + m[1];
  return escapeHtml(s);
}

/**
 * Линкуем @handle в произвольном HTML-фрагменте.
 * - Не трогаем уже существующие <a ...>...</a>
 * - @handle должен быть 5–32 символа [A-Za-z0-9_]
 */
function linkifyTelegramHandles(html = '') {
  // Разбиваем по тегам <a> — обрабатываем только невнутренние куски
  const parts = html.split(/(<a\b[^>]*>.*?<\/a>)/gis);
  for (let i = 0; i < parts.length; i++) {
    if (i % 2 === 1) continue; // это внутри <a>...</a>
    parts[i] = parts[i].replace(
      /(^|[^A-Za-z0-9_])@([A-Za-z0-9_]{5,32})(?![A-Za-z0-9_])/g,
      (m, p1, handle) => `${p1}<a href="https://t.me/${handle}" target="_blank" rel="noopener">@${handle}</a>`
    );
  }
  return parts.join('');
}


async function getTournament(chatId) {
  const doc = await colChats.findOne({ chatId });
  return {
    name: doc?.tournamentName || '',
    site: doc?.tournamentSite || '',
    wiki: doc?.tournamentWiki || '',
    desc: doc?.tournamentDesc || '',
    logo: doc?.tournamentLogo || null, // { relPath, ... }
    back: doc?.tournamentBack || null, // { relPath, ... }
    // Новые поля для верхнего блока
    servers: Array.isArray(doc?.tournamentServers) ? doc.tournamentServers : [],
    pack: doc?.tournamentPack || '',
    streams: Array.isArray(doc?.tournamentStreams) ? doc.tournamentStreams : [],
    newsChannel: doc?.tournamentNewsChannel || '',   // ← ДОБАВИТЬ ЭТО
    tournamentStatsUrl: doc?.tournamentStatsUrl || '',
    tournamentStatsEnabled: doc?.tournamentStatsEnabled || false,
  };
}

async function getGroups(chatId) {
  return colGameGroups.find({ chatId }).sort({ groupId: 1 }).toArray();
}
async function getFinals(chatId) {
  return colFinalGroups.find({ chatId }).sort({ groupId: 1 }).toArray();
}
async function getSuperfinals(chatId) {
  return colSuperFinalGroups.find({ chatId }).sort({ groupId: 1 }).toArray();
}

async function getCustomGroups(chatId) {
  return colCustomGroups
    .find({ chatId })
    .sort({ createdAt: 1, groupId: 1 })
    .toArray();
}

// Вернёт Map<groupId, Map<nameNorm, pts>>
async function getCustomPointsByGroup(chatId) {
  const docs = await colCustomPoints.find({ chatId }).toArray();
  const out = new Map();
  for (const d of docs) {
    const gid = Number(d.groupId);
    const m = new Map();
    for (const p of (d.points || [])) {
      if (!p?.nameNorm) continue;
      m.set(p.nameNorm, Number(p.pts));
    }
    out.set(gid, m);
  }
  return out;
}

// Рендер карточек кастом‑групп (каждая группа — сворачиваемая подсекция)
// Рендер карточек кастом‑групп (добавлены демки и скриншоты)
function renderCustomSection(items = [], pointsByGroup = new Map(), screensMap = new Map(), collapsedByDefault = false, achIndex = null) {
  if (!items?.length) return '<div class="text-muted">Нет данных</div>';
  const openAttr = collapsedByDefault ? '' : ' open';

  const cells = items.map(g => {
    const id = `custom-${g.groupId}`;
    const title = (g.name && String(g.name).trim()) ? String(g.name).trim() : `Группа №${g.groupId}`;
    const ptsMap = pointsByGroup.get(Number(g.groupId)) || new Map();

    const players = renderPlayers(Array.isArray(g.players) ? g.players : [], ptsMap, achIndex);
    const maps = renderMaps(Array.isArray(g.maps) ? g.maps : []);
    const demos = renderDemos(Array.isArray(g.demos) ? g.demos : []);
    const files = screensMap.get(Number(g.groupId)) || [];
    const shots = renderScreenshots(files, id);

    return `
      <div>
        <details id="${escapeHtml(id)}" class="sub-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">${escapeHtml(title)}</span>
            <a href="#${escapeHtml(id)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на ${escapeHtml(title)}">#</a>
          </summary>
          <div class="mt-2">
            <div class="card shadow-sm h-100">
              <div class="card-body d-flex flex-column">
                ${players}
                ${maps}
                ${demos}
                <div class="mt-auto">${shots}</div>
              </div>
            </div>
          </div>
        </details>
      </div>
    `;
  }).join('');

  return `<div class="cards-grid">${cells}</div>`;
}


async function getScreensForScope(chatId, scope, groups) {
  // вернём Map<groupId, files[]>, где files — массив из colScreenshots.files
  const result = new Map();
  if (!groups?.length) return result;
  const wants = groups.map(g => ({
    groupId: Number(g.groupId),
    runId: formatRunId(g.createdAt || new Date(0)),
  }));

  await Promise.all(
    wants.map(async w => {
      const doc = await colScreenshots.findOne({
        chatId,
        scope,
        groupId: w.groupId,
        groupRunId: w.runId,
      });
      result.set(w.groupId, doc?.files || []);
    })
  );
  return result;
}

async function getGroupPointsMap(chatId) {
  const doc = await colGroupPoints.findOne({ chatId });
  const arr = doc?.points || []; // [{nameNorm,nameOrig,pts}]
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}
async function getFinalPointsMap(chatId) {
  const doc = await colFinalPoints.findOne({ chatId });
  const arr = doc?.points || [];
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}
async function getSuperFinalPointsMap(chatId) {
  const doc = await colSuperFinalPoints.findOne({ chatId });
  const arr = doc?.points || [];
  const m = new Map();
  for (const p of arr) m.set(p.nameNorm, Number(p.pts));
  return m;
}

// Вспомогательный: переводим документ в числовое "время матча" для сортировки
function getMatchTimeValue(r = {}) {
  // Пытаемся сначала по ISO-дате
  if (r.matchDateTimeIso) {
    const t = Date.parse(r.matchDateTimeIso);
    if (!Number.isNaN(t)) return t;
  }
  // потом по числовому ts
  if (typeof r.matchTs === 'number' && Number.isFinite(r.matchTs)) {
    return r.matchTs;
  }
  // потом по строковому matchDateTime / createdAt
  if (r.matchDateTime) {
    const t = Date.parse(r.matchDateTime);
    if (!Number.isNaN(t)) return t;
  }
  if (r.createdAt) {
    const t = Date.parse(r.createdAt);
    if (!Number.isNaN(t)) return t;
  }
  return 0;
}

// форматируем дату завершения матча (matchDateTimeIso) в МСК
function formatMatchFinishedRuMsk(r = {}) {
  if (!r.matchDateTimeIso) return '';
  const d = new Date(r.matchDateTimeIso);
  return formatRuMskDateTime(d); // использует уже существующий dtfRU_MSK
}

// форматируем длительность матча
function formatMatchDuration(r = {}) {
  // 1) если matchPlaytime уже есть в виде "6:50" — используем его
  if (typeof r.matchPlaytime === 'string' && r.matchPlaytime.trim()) {
    return r.matchPlaytime.trim();
  }

  // 2) иначе пытаемся вычислить из matchTs
  const raw = Number(r.matchTs);
  if (!Number.isFinite(raw) || raw <= 0) return '';

  let seconds = null;

  // если значение похоже на "секунды" (меньше суток)
  if (raw < 24 * 60 * 60) {
    seconds = raw;
  }
  // если похоже на миллисекунды (меньше суток)
  else if (raw < 24 * 60 * 60 * 1000) {
    seconds = Math.round(raw / 1000);
  }

  if (seconds == null) return '';

  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return String(m) + ':' + String(s).padStart(2, '0');
}

// общая функция получения результатов по стадии
async function getStageResultsMap(chatId, scope) {
  let col = null;
  if (scope === 'group') col = colGroupResults;
  else if (scope === 'final') col = colFinalResults;
  else if (scope === 'superfinal') col = colSuperFinalResults;
  else return new Map();

  const docs = await col
    .find({ chatId })
    .sort({ matchDateTimeIso: 1, matchTs: 1, createdAt: 1 })
    .toArray();

  const map = new Map(); // Map<groupId, Array<result>>
  for (const r of docs) {
    const gid = Number(r.groupId);
    if (!Number.isFinite(gid)) continue;
    if (!map.has(gid)) map.set(gid, []);
    map.get(gid).push(r);
  }
  return map;
}

async function getGroupResultsMap(chatId) {
  return getStageResultsMap(chatId, 'group');
}
async function getFinalResultsMap(chatId) {
  return getStageResultsMap(chatId, 'final');
}
async function getSuperFinalResultsMap(chatId) {
  return getStageResultsMap(chatId, 'superfinal');
}


async function getDefinedGroupRating(chatId) {
  const doc = await colPlayerRatings.findOne({ chatId });
  const players = Array.isArray(doc?.players) ? doc.players.slice() : [];
  players.sort((a, b) => Number(a.rank) - Number(b.rank) || (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
  return { players, updatedAt: doc?.updatedAt || null };
}

async function getDefinedFinalRating(chatId) {
  const doc = await colFinalRatings.findOne({ chatId });
  const players = Array.isArray(doc?.players) ? doc.players.slice() : [];
  players.sort((a, b) => Number(a.rank) - Number(b.rank) || (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
  return { players, updatedAt: doc?.updatedAt || null };
}

async function getDefinedSuperFinalRating(chatId) {
  const doc = await colSuperFinalRatings.findOne({ chatId });
  const players = Array.isArray(doc?.players) ? doc.players.slice() : [];
  players.sort((a, b) => Number(a.rank) - Number(b.rank) || (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
  return { players, updatedAt: doc?.updatedAt || null };
}

async function getAchievements(chatId) {
  // сортируем по idx (если есть), затем по createdAt
  const list = await colAchievements.find({ chatId }).sort({ idx: 1, createdAt: 1 }).toArray();
  return list || [];
}

function makeAchievementId(a) {
  if (a?.idx !== undefined && a?.idx !== null) return `ach-${String(a.idx)}`;
  const oid = (a?._id && typeof a._id.toString === 'function') ? a._id.toString() : String(a?._id || '');
  return oid ? `ach-${oid}` : `ach-${Math.random().toString(36).slice(2)}`;
}

// Индекс: Map<nameNorm, AchInfo[]>, где AchInfo = { id, title, url, name, playerName }
// Индекс: Map<nameNorm, { achs: AchInfo[], percs: AchInfo[] }>
function buildAchievementsIndex(achievements = []) {
  const byPlayer = new Map();
  for (const a of achievements) {
    const id = makeAchievementId(a);
    const url = a?.image?.relPath ? '/media/' + relToUrl(a.image.relPath) : null;
    const playerName = a?.player?.nameOrig || a?.player?.nameNorm || '';
    const pNorm = (a?.player?.nameNorm || '').trim().toLowerCase();
    const name = String(a?.name || '').trim();
    const title = playerName ? `${name} — ${playerName}` : (name || id);
    if (!pNorm) continue;

    const info = { id, title, url, name, playerName };
    const type = String(a?.type || 'achievement').toLowerCase();

    if (!byPlayer.has(pNorm)) byPlayer.set(pNorm, { achs: [], percs: [] });
    const bucket = (type === 'perc') ? byPlayer.get(pNorm).percs : byPlayer.get(pNorm).achs;
    bucket.push(info);
  }
  for (const v of byPlayer.values()) {
    v.achs.sort((x, y) => (x.name || '').localeCompare(y.name || '', undefined, { sensitivity: 'base' }));
    v.percs.sort((x, y) => (x.name || '').localeCompare(y.name || '', undefined, { sensitivity: 'base' }));
  }
  return byPlayer;
}

// Мини-иконки ачивок/перков рядом с именем игрока (с подсветкой контуров)
// Мини-иконки ачивок/перков рядом с именем игрока (c hover-увеличением x4)
function renderAchievementBadgesInline(nameNorm, achIndex) {
  const key = String(nameNorm || '').trim().toLowerCase();
  if (!key || !achIndex || !achIndex.has(key)) return '';
  const pack = achIndex.get(key) || { achs: [], percs: [] };
  const achs = pack.achs || [];
  const percs = pack.percs || [];
  if (!achs.length && !percs.length) return '';

  // Общий обработчик на ссылке: увеличиваем первый дочерний элемент (img или span)
  const handlers =
    ' onmouseenter="(function(a){var el=a.firstElementChild;if(el){el.style.transform=\'scale(4)\';el.style.transformOrigin=\'left center\';el.style.transition=\'transform .12s ease, box-shadow .12s ease\';el.style.position=\'relative\';el.style.zIndex=\'1060\';el.style.boxShadow=\'0 12px 36px rgba(0,0,0,.35)\';}})(this)"' +
    ' onmouseleave="(function(a){var el=a.firstElementChild;if(el){el.style.transform=\'\';el.style.boxShadow=\'\';el.style.zIndex=\'\';el.style.position=\'\';}})(this)"' +
    ' onfocus="(function(a){var el=a.firstElementChild;if(el){el.style.transform=\'scale(4)\';el.style.transformOrigin=\'left center\';el.style.transition=\'transform .12s ease, box-shadow .12s ease\';el.style.position=\'relative\';el.style.zIndex=\'1060\';el.style.boxShadow=\'0 12px 36px rgba(0,0,0,.35)\';}})(this)"' +
    ' onblur="(function(a){var el=a.firstElementChild;if(el){el.style.transform=\'\';el.style.boxShadow=\'\';el.style.zIndex=\'\';el.style.position=\'\';}})(this)"';

  const renderItem = (ai, cls, kind) => {
    const href = `#${escapeHtml(ai.id)}`;
    const linkCls = kind === 'perc' ? 'perc-badge-link' : 'ach-badge-link';

    if (ai.url) {
      const alt = escapeHtml(ai.title || 'ach');
      return `<a href="${href}" class="me-1 align-middle ${linkCls}" title="${alt}"${handlers}>
        <img src="${escapeHtml(ai.url)}" alt="${alt}" class="${cls}" loading="lazy"
             style="transition: transform .12s ease, box-shadow .12s ease; transform-origin: left center;" />
      </a>`;
    }

    // Фоллбек без картинки — увеличиваем сам emoji как замену
    return `<a href="${href}" class="me-1 align-middle ${linkCls}" title="${escapeHtml(ai.title || 'ach')}"${handlers}>
      <span class="ach-badge-fallback" style="display:inline-block; transition: transform .12s ease;">🏆</span>
    </a>`;
  };

  const achHtml = achs.map(ai => renderItem(ai, 'ach-badge-img', 'ach')).join('');
  const percHtml = percs.map(ai => renderItem(ai, 'perc-badge-img', 'perc')).join('');

  return `<span class="ach-badges ms-2">${achHtml}${percHtml}</span>`;
}


// Раздел «Ачивки» — сворачиваемая секция + подсекции по каждой ачивке
function renderAchievementsSection(achievements = [], collapsedByDefault = false) {
  if (!achievements?.length) return '';
  const openAttr = collapsedByDefault ? '' : ' open';

  const cards = achievements.map(a => {
    const id = makeAchievementId(a);
    const title = a?.player?.nameOrig
      ? `${a?.name || 'Ачивка'} - ${a.player.nameOrig}`
      : `${a?.name || 'Ачивка'}`;
    const href = `#${escapeHtml(id)}`;
    const imgUrl = a?.image?.relPath ? '/media/' + relToUrl(a.image.relPath) : null;
    const descHtml = renderAchievementRichText(a?.desc || '');

    return `
      <div>
        <details id="${escapeHtml(id)}" class="sub-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">${escapeHtml(title)}</span>
            <a href="${href}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на ачивку">#</a>
          </summary>
          <div class="mt-2">
            <div class="card shadow-sm">
              <div class="card-body">
                <div class="table-responsive">
                  <table class="table table-borderless align-middle mb-0">
                    <tbody>
                      <tr>
                        <td style="width:120px;">
                          ${imgUrl
        ? `<img src="${escapeHtml(imgUrl)}" alt="${escapeHtml(a?.name || 'ach')}" class="ach-thumb" loading="lazy" />`
        : '<div class="text-muted small">(нет изображения)</div>'}
                        </td>
                        <td>
                          <div class="news-text" style="white-space: pre-wrap;">${descHtml}</div>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </details>
      </div>
    `;
  }).join('');

  return `
    <section class="mb-5">
      <details id="section-achievements" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">Ачивки</span>
          <a href="#section-achievements" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${achievements.length}</span>
        </summary>
        <div class="mt-2">
          <div class="cards-grid">${cards}</div>
        </div>
      </details>
    </section>
  `;
}

async function getFeedback(chatId) {
  const list = await colFeedback.find({ chatId }).sort({ createdAtMSK: -1 }).toArray();
  return list || [];
}


async function listNews(chatId, scope, groupRunId = null) {
  const col = colNews;
  const q = { chatId, scope };
  if (groupRunId) q.groupRunId = groupRunId;
  return col.find(q).sort({ createdAt: -1 }).toArray();
}

async function findLatestRunIdForScope(chatId, scope) {
  let col;
  if (scope === 'group') col = colGameGroups;
  else if (scope === 'final') col = colFinalGroups;
  else if (scope === 'superfinal') col = colSuperFinalGroups;
  else return null;

  const last = await col.find({ chatId }).sort({ createdAt: -1 }).limit(1).toArray();
  if (!last.length) return null;
  return formatRunId(last[0].createdAt || new Date());
}

function renderTimeStr(time) {
  const s = String(time || '').trim();
  if (!s) return '';
  return `<div class="group-time text-secondary small mb-2">Время: ${escapeHtml(s)}</div>`;
}

// UPDATED: рендер списка новостей с опциональным ID для стилизации секций
function renderNewsList(title, news = [], collapsedByDefault = false, sectionId = null) {
  if (!news?.length) return '';

  // Берём самую свежую (первая в массиве — сортировка по createdAt: -1 уже есть)
  const latest = news[0];
  const older = news.slice(1);

  function renderItem(n) {
    const ts = n.createdAt ? formatRuMskDateTime(n.createdAt) : '';

    // Автор: сначала из n.author.username, потом из твоих старых полей
    const whoUsername =
      (n.author && n.author.username) ||
      n.authorUsername ||
      n.username ||
      '';
    const who = whoUsername
      ? `@${String(whoUsername)}`
      : (n.authorId ? `#${n.authorId}` : '');

    const nid = (n && n._id && typeof n._id.toString === 'function')
      ? n._id.toString()
      : String(n?._id || '');
    const idAttr = nid ? ` id="news-${escapeHtml(nid)}"` : '';
    const selfLink = nid
      ? `<a href="#news-${escapeHtml(nid)}" class="ms-2 text-decoration-none" aria-label="Ссылка на новость">#</a>`
      : '';

    // 1) Текст новости (rich + embeds). Переносы уже <br> внутри renderNewsRichText.
    const baseHtml = renderNewsRichText(n.text || '');
    const textWithEmbeds = injectEmbedsIntoNewsHtml(baseHtml).trim();

    // 2) Обложка новости при наличии news_img_file_name
    // Путь относительно /media: "<chatId>\\news\\<file>"
    let coverHtml = '';
    if (n.news_img_file_name && n.chatId != null) {
      const relPathRaw = `${String(n.chatId).trim()}\\news\\${String(n.news_img_file_name).trim()}`;
      const url = `/media/${relToUrl(relPathRaw)}`;
      coverHtml = `
        <div class="news-cover" style="margin:0 0 .5rem 0;">
          <img
            src="${escapeAttr(url)}"
            alt=""
            loading="lazy"
            style="display:block; margin:0 auto; max-width:100%; height:auto;"
          />
        </div>`;
    }

    return `
      <li class="list-group-item qj-news-item"${idAttr} style="margin-bottom: 1.25rem;">
        <div class="d-flex flex-column flex-md-row">
          <div class="flex-grow-1">
            ${coverHtml}
            <div class="news-text">
              ${textWithEmbeds}
            </div>
          </div>
          <div class="news-meta small text-muted mt-2 mt-md-0 ms-0 ms-md-3">
            ${escapeHtml(ts)}${who ? ` (${linkify(who)})` : ''}${selfLink}
          </div>
        </div>
      </li>`;
  }

  const latestHtml = renderItem(latest);
  const olderHtml = older.map(renderItem).join('');
  const olderBlock = older.length ? `
    <details class="sub-collapse">
      <summary class="qj-toggle">
        <span class="section-title">Предыдущие новости</span>
        <span class="qj-badge ms-auto">${older.length}</span>
      </summary>
      <ul class="list-group qj-news-list mt-2">
        ${olderHtml}
      </ul>
    </details>
  ` : '';

  const openAttr = collapsedByDefault ? '' : ' open';
  const idAttr = sectionId ? ` id="${escapeHtml(sectionId)}"` : '';

  return `
    <section class="mb-4">
      <details class="news-collapse"${openAttr}${idAttr}>
        <summary class="qj-toggle">
          <span class="section-title">${escapeHtml(title)}</span>
          <span class="qj-badge ms-auto">${news.length}</span>
        </summary>

        <!-- Последняя новость -->
        <ul class="list-group qj-news-list mt-2">
          ${latestHtml}
        </ul>

        <!-- Предыдущие новости (по умолчанию скрыты) -->
        ${olderBlock}
      </details>
    </section>
  `;
}


function renderPlayers(
  players = [],
  ptsMap = null,
  achIndex = null,
  resultsByGroup = null,
  groupId = null,
  isSuperfinal = false
) {
  if (!players?.length) return '<div class="text-muted small">(пусто)</div>';

  const arr = players.slice();
  const hasPtsFlag = ptsMap && arr.some(p => ptsMap.has(p.nameNorm));

  // Предрасчёт средней эффективности по результатам карт для этой группы (если есть)
  let effAvgByPlayer = null;
  if (resultsByGroup && typeof resultsByGroup.get === 'function' && groupId != null) {
    const gid = Number(groupId);
    if (Number.isFinite(gid)) {
      const matches = resultsByGroup.get(gid) || [];
      if (matches && matches.length) {
        effAvgByPlayer = new Map(); // nameNorm -> { sum, count }
        for (const m of matches) {
          const resPlayers = Array.isArray(m.players) ? m.players : [];
          for (const p of resPlayers) {
            const nameNorm = p?.nameNorm;
            if (!nameNorm) continue;
            const eff = Number(p.eff);
            if (!Number.isFinite(eff)) continue;
            let s = effAvgByPlayer.get(nameNorm);
            if (!s) {
              s = { sum: 0, count: 0 };
              effAvgByPlayer.set(nameNorm, s);
            }
            s.sum += eff;
            s.count++;
          }
        }
      }
    }
  }

  function getEffAvgForPlayer(nameNorm) {
    if (!effAvgByPlayer) return Number.NEGATIVE_INFINITY;
    const s = effAvgByPlayer.get(nameNorm);
    if (!s || !s.count) return Number.NEGATIVE_INFINITY;
    return s.sum / s.count;
  }

  if (hasPtsFlag) {
    arr.sort((a, b) => {
      const aHas = ptsMap.has(a.nameNorm);
      const bHas = ptsMap.has(b.nameNorm);
      if (aHas && bHas) {
        const ap = Number(ptsMap.get(a.nameNorm));
        const bp = Number(ptsMap.get(b.nameNorm));

        if (ap !== bp) return ap - bp;

        // Тайбрейк: при равных очках — по средней эффективности (убывание)
        const ea = getEffAvgForPlayer(a.nameNorm);
        const eb = getEffAvgForPlayer(b.nameNorm);
        if (eb !== ea) return eb - ea;

        return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
      }
      if (aHas !== bHas) return aHas ? -1 : 1;
      return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
    });
  } else {
    arr.sort((a, b) => (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
  }

  function getHaloClassByIndex(idx) {
    if (!isSuperfinal || !hasPtsFlag) return '';
    if (idx === 0) return ' qj-halo qj-halo-gold';
    if (idx === 1) return ' qj-halo qj-halo-silver';
    if (idx === 2) return ' qj-halo qj-halo-bronze';
    return '';
  }

  return `<ul class="players list-unstyled mb-2">
    ${arr.map((p, i) => {
    const pts = ptsMap?.get(p.nameNorm);
    const badges = renderAchievementBadgesInline(p.nameNorm, achIndex);

    const posHtml = hasPtsFlag
      ? `<span class="player-pos text-muted">${i + 1}.</span>`
      : '';

    const ptsHtml = (pts !== undefined && pts !== null)
      ? `<span class="player-pts qj-pts">${pts}</span>`
      : '';

    const metaHtml = (ptsHtml || badges)
      ? `<span class="player-meta ms-2">${ptsHtml}${badges}</span>`
      : '';

    const displayName = p.nameOrig || p.nameNorm || '';
    const haloClass = getHaloClassByIndex(i);

    // Формируем HTML флага
    const flagCode = p.country || '';
    const flagHtml = flagCode
      ? `<img src="/media/flags/1x1/${escapeHtml(flagCode)}.svg" alt="" 
           style="height:14px; vertical-align:middle; margin-right:4px;">`
      : `<img src="/media/flags/1x1/question.svg" alt="?" 
           style="height:14px; vertical-align:middle; margin-right:4px;">`;
    const pnameHtml = PLAYER_STATS_ENABLED
      ? `<a href="#" class="player-name player-link qj-accent fw-semibold js-player-stat${haloClass}"
            data-player="${escapeAttr(displayName)}">${flagHtml}${escapeHtml(displayName)}</a>`
      : `<span class="player-name qj-accent fw-semibold${haloClass}">${flagHtml}${escapeHtml(displayName)}</span>`;

    return `<li>
          ${posHtml}
          ${pnameHtml}
          ${metaHtml}
        </li>`;
  }).join('')}
  </ul>`;
}

// === ФЛАГИ ИГРОКОВ =========================================================

let PLAYER_COUNTRY_BY_SIGNUP_ID = new Map();
let PLAYER_COUNTRY_BY_NORM_NAME = new Map();

function normalizePlayerKey(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/\s+/g, '')
    .replace(/[^a-z0-9а-яё]+/g, ''); // убираем пробелы/спецсимволы
}

function initPlayerCountryIndexes(signups = [], users = []) {
  PLAYER_COUNTRY_BY_SIGNUP_ID = new Map();
  PLAYER_COUNTRY_BY_NORM_NAME = new Map();

  const usersById = new Map();

  // Индекс по userId
  for (const u of users || []) {
    let id = null;
    if (u?._id && typeof u._id.toString === 'function') {
      id = u._id.toString();
    } else if (u?._id != null) {
      id = String(u._id);
    }
    if (!id) continue;

    usersById.set(id, u);

    const nickKey = normalizePlayerKey(u.nickNorm || u.nick);
    const c = (u.country || '').trim().toLowerCase();
    if (nickKey && c && !PLAYER_COUNTRY_BY_NORM_NAME.has(nickKey)) {
      PLAYER_COUNTRY_BY_NORM_NAME.set(nickKey, c);
    }
  }

  // Индекс по signupId + нормализованному нику из заявок
  for (const s of signups || []) {
    const signupId = s.signupId;
    if (!signupId) continue;

    let uid = null;
    if (s.userId && typeof s.userId.toString === 'function') {
      uid = s.userId.toString();
    } else if (s.userId != null) {
      uid = String(s.userId);
    }

    const u = uid ? usersById.get(uid) : null;
    const c = (u?.country || '').trim().toLowerCase();
    if (!c) continue;

    // связываем signupId -> country
    PLAYER_COUNTRY_BY_SIGNUP_ID.set(signupId, c);

    // и сразу же ник из заявки -> country (на всякий случай)
    const nKey = normalizePlayerKey(s.playerNickNorm || s.playerNick);
    if (nKey && !PLAYER_COUNTRY_BY_NORM_NAME.has(nKey)) {
      PLAYER_COUNTRY_BY_NORM_NAME.set(nKey, c);
    }
  }
}

function resolvePlayerCountryCode(nameNormOrOrig, signupId) {
  // 1) приоритет по signupId
  if (signupId && PLAYER_COUNTRY_BY_SIGNUP_ID.has(signupId)) {
    return PLAYER_COUNTRY_BY_SIGNUP_ID.get(signupId);
  }

  // 2) fallback по нормализованному нику
  const key = normalizePlayerKey(nameNormOrOrig);
  if (key && PLAYER_COUNTRY_BY_NORM_NAME.has(key)) {
    return PLAYER_COUNTRY_BY_NORM_NAME.get(key);
  }

  return null;
}

// Универсальный рендер имени с флагом
function renderPlayerNameWithFlag(nameOrig, nameNorm, signupId, extraClasses = '') {
  const displayName = nameOrig || nameNorm || '';
  const code = resolvePlayerCountryCode(nameNorm || nameOrig, signupId);
  const finalCode = (code && typeof code === 'string' && code.trim())
    ? code.trim().toLowerCase()
    : 'question';

  const flagUrl = `/media/flags/1x1/${encodeURIComponent(finalCode)}.svg`;

  const baseClass = extraClasses ? ' ' + extraClasses.trim() : '';

  const nameHtml = PLAYER_STATS_ENABLED
    ? `<a href="#"
          class="player-name player-link qj-accent fw-semibold${baseClass}"
          data-player="${escapeAttr(displayName)}">${escapeHtml(displayName)}</a>`
    : `<span class="player-name qj-accent fw-semibold${baseClass}">${escapeHtml(displayName)}</span>`;

  return `
    <span class="qj-player-flag-wrap">
      <img src="${escapeAttr(flagUrl)}"
           alt=""
           class="qj-player-flag me-1"
           loading="lazy"
           onerror="this.style.visibility='hidden';">
      ${nameHtml}
    </span>
  `;
}


function renderScreenshots(files = [], groupKey = '') {
  if (!files?.length) {
    return '<div class="text-muted small">Скриншоты отсутствуют</div>';
  }

  const thumbs = files.map(f => {
    const url = '/media/' + relToUrl(f.relPath || '');
    const alt = escapeHtml(f.mime || 'image');
    return `
      <button type="button"
              class="qj-shot-btn me-1 mb-1"
              data-src="${escapeHtml(url)}"
              onclick="window.QJ_LB_open && window.QJ_LB_open(this, event)"
              aria-label="Открыть скриншот">
        <img src="${escapeHtml(url)}" alt="${alt}" loading="lazy"
             style="max-width: 120px; max-height: 90px; object-fit: cover; border-radius: 6px; border: 1px solid rgba(0,0,0,0.08);" />
      </button>`;
  }).join('');

  return `
    <div class="mt-1 d-flex flex-wrap qj-shots" data-shots-group="${escapeAttr(groupKey)}">
      ${thumbs}
    </div>
    <script>(function QJ_LB_BOOT_80P(){
      if (window.__QJ_LB_READY__) return;
      window.__QJ_LB_READY__ = true;

      // ===== CSS =====
      if (!document.getElementById('qj-lightbox-style')) {
        var styleEl = document.createElement('style');
        styleEl.id = 'qj-lightbox-style';
        styleEl.textContent =
          'html.qj-lock, body.qj-lock{overflow:hidden; overscroll-behavior:contain; touch-action:none}' +
          '.qj-lb-root{position:fixed;inset:0;z-index:9999;display:none}' +
          '.qj-lb-root.qj-visible{display:block}' +
          '.qj-lb-backdrop{position:absolute;inset:0;background:rgba(0,0,0,.6)}' +
          '.qj-lb-panel{position:absolute;box-shadow:0 8px 32px rgba(0,0,0,.35);border-radius:12px;background:#000;display:flex;align-items:flex-start;justify-content:center;overflow:auto}' +
          '.qj-lb-img{width:100%;height:auto;display:block}' +
          '.qj-lb-btn{position:absolute;border:0;background:rgba(0,0,0,.45);color:#fff;cursor:pointer;border-radius:8px;font-size:28px;line-height:1;padding:.25rem .6rem}' +
          '.qj-lb-btn:hover{background:rgba(0,0,0,.75)}' +
          '.qj-lb-prev{left:8px;top:50%;transform:translateY(-50%)}' +
          '.qj-lb-next{right:8px;top:50%;transform:translateY(-50%)}' +
          '.qj-lb-close{right:8px;top:8px;font-size:32px}' +
          '@media (max-width:768px){.qj-lb-btn{font-size:22px}}';
        (document.head || document.documentElement).appendChild(styleEl);
      }

      // ===== DOM =====
      var root = document.querySelector('.qj-lb-root');
      if (!root) {
        root = document.createElement('div');
        root.className = 'qj-lb-root';
        root.innerHTML =
          '<div class="qj-lb-backdrop"></div>' +
          '<div class="qj-lb-panel" role="dialog" aria-modal="true">' +
            '<button class="qj-lb-btn qj-lb-prev" aria-label="Предыдущий">‹</button>' +
            '<img class="qj-lb-img" alt="screenshot"/>' +
            '<button class="qj-lb-btn qj-lb-next" aria-label="Следующий">›</button>' +
            '<button class="qj-lb-btn qj-lb-close" aria-label="Закрыть">×</button>' +
          '</div>';
        (document.body || document.documentElement).appendChild(root);
      }

      var backdrop = root.querySelector('.qj-lb-backdrop');
      var panel    = root.querySelector('.qj-lb-panel');
      var imgEl    = root.querySelector('.qj-lb-img');
      var prevBtn  = root.querySelector('.qj-lb-prev');
      var nextBtn  = root.querySelector('.qj-lb-next');
      var closeBtn = root.querySelector('.qj-lb-close');

      var currentGroup = [];
      var currentIndex = 0;

      // ширина «80% сайта/iframe» на время сессии
      var sessionWidth = null; // px
      // вертикальная привязка к миниатюре (для iframe)
      var sessionTop = null;

      function clamp(n,a,b){ return Math.max(a, Math.min(b, n)); }
      function isInIframe(){ try { return window.top !== window.self; } catch(_){ return true; } }
      function updateNav(){
        prevBtn.style.display = currentIndex > 0 ? 'block' : 'none';
        nextBtn.style.display = currentIndex < currentGroup.length - 1 ? 'block' : 'none';
      }

      // === Ширина сайта/iframe * 0.8 ===
      function measureSiteWidth80(){
        var vw = window.innerWidth || document.documentElement.clientWidth || 0;
        var bodyW = document.body ? (document.body.clientWidth || 0) : 0;
        var selectors = '.container,.container-fluid,.qj-container,.content,.main,.page,.wrapper,main';
        var maxCont = 0;
        try {
          var nodes = document.querySelectorAll(selectors);
          for (var i=0;i<nodes.length;i++){
            var r = nodes[i].getBoundingClientRect();
            if (r.width > maxCont) maxCont = r.width;
          }
        } catch(_){}
        var base = Math.max(vw, bodyW, maxCont);   // «ширина сайта»
        var target = Math.floor(base * 0.80);      // 80% от неё
        // но не выходить за рамки вьюпорта (оставим поля)
        return clamp(target, 320, Math.floor(vw * 0.95));
      }

      // Рассчитываем бокс (ширина фикс, высота ≤ 92% окна)
      function computeBox(widthPx, ar){
        var vw = window.innerWidth, vh = window.innerHeight;
        var maxH = Math.floor(vh * 0.92);
        var w = clamp(widthPx, 320, Math.floor(vw * 0.95));
        var idealH = (ar && ar > 0) ? Math.floor(w / ar) : Math.floor(w / (16/9));
        var h = Math.min(idealH, maxH);
        return { w: w, h: h };
      }

      // позиционирование: по центру по X; по Y — центр или привязка к миниатюре (в iframe)
      function placePanel(sz, anchorBtn){
        var vw = window.innerWidth, vh = window.innerHeight;
        var left = Math.max(8, Math.floor((vw - sz.w)/2));
        var top;
        if (sessionTop != null) {
          top = clamp(sessionTop, 8, vh - sz.h - 8);
        } else if (anchorBtn && isInIframe()) {
          var r = anchorBtn.getBoundingClientRect();
          top = clamp(Math.floor(r.top), 8, vh - sz.h - 8);
          sessionTop = top;
        } else {
          top = Math.max(8, Math.floor((vh - sz.h)/2));
        }
        panel.style.left   = left + 'px';
        panel.style.top    = top  + 'px';
        panel.style.width  = sz.w + 'px';
        panel.style.height = sz.h + 'px';
      }

      function lockScroll(){
        document.documentElement.classList.add('qj-lock');
        document.body.classList.add('qj-lock');
      }
      function unlockScroll(){
        document.documentElement.classList.remove('qj-lock');
        document.body.classList.remove('qj-lock');
      }

      function openAt(index, anchorBtn){
        currentIndex = clamp(index, 0, currentGroup.length - 1);
        var src = currentGroup[currentIndex].dataset.src;

        if (sessionWidth == null) sessionWidth = measureSiteWidth80();

        lockScroll(); // мягкий lock — без scrollTo

        var probe = new Image();
        probe.onload = function(){
          var ar = (probe.naturalWidth && probe.naturalHeight)
            ? (probe.naturalWidth / probe.naturalHeight)
            : (16/9);
          var box = computeBox(sessionWidth, ar);
          placePanel(box, anchorBtn);
          imgEl.src = src;
          root.classList.add('qj-visible');
          updateNav();
        };
        probe.onerror = function(){
          var box = computeBox(sessionWidth, 16/9);
          placePanel(box, anchorBtn);
          imgEl.src = src;
          root.classList.add('qj-visible');
          updateNav();
        };
        probe.src = src;
      }

      function closeLB(){
        root.classList.remove('qj-visible');
        imgEl.src = '';
        sessionWidth = null;
        sessionTop = null;
        unlockScroll();
      }

      // Навигация
      prevBtn.addEventListener('click', function(e){ e.preventDefault(); e.stopPropagation(); if (currentIndex > 0) openAt(currentIndex - 1, currentGroup[currentIndex - 1]); });
      nextBtn.addEventListener('click', function(e){ e.preventDefault(); e.stopPropagation(); if (currentIndex < currentGroup.length - 1) openAt(currentIndex + 1, currentGroup[currentIndex + 1]); });
      closeBtn.addEventListener('click', function(e){ e.preventDefault(); e.stopPropagation(); closeLB(); });
      root.querySelector('.qj-lb-backdrop').addEventListener('click', function(e){ e.preventDefault(); e.stopPropagation(); closeLB(); });

      // Свайпы
      var touchX = null;
      panel.addEventListener('touchstart', function(e){ touchX = e.touches && e.touches[0] ? e.touches[0].clientX : null; }, {passive:true});
      panel.addEventListener('touchend', function(e){
        if (touchX == null) return;
        var dx = (e.changedTouches && e.changedTouches[0] ? e.changedTouches[0].clientX : 0) - touchX;
        if (Math.abs(dx) > 40){
          if (dx < 0 && currentIndex < currentGroup.length - 1) openAt(currentIndex + 1, currentGroup[currentIndex + 1]);
          if (dx > 0 && currentIndex > 0) openAt(currentIndex - 1, currentGroup[currentIndex - 1]);
        }
        touchX = null;
      });

      document.addEventListener('keydown', function(e){
        if (!root.classList.contains('qj-visible')) return;
        if (e.key === 'Escape') { e.preventDefault(); closeLB(); }
        if (e.key === 'ArrowLeft'  && currentIndex > 0) { e.preventDefault(); openAt(currentIndex - 1, currentGroup[currentIndex - 1]); }
        if (e.key === 'ArrowRight' && currentIndex < currentGroup.length - 1) { e.preventDefault(); openAt(currentIndex + 1, currentGroup[currentIndex + 1]); }
      });

      // Глобальный open из миниатюры
      window.QJ_LB_open = function(btn, evt){
        try {
          if (evt) { evt.preventDefault(); evt.stopPropagation(); if (evt.stopImmediatePropagation) evt.stopImmediatePropagation(); }
        } catch(_){}
        var groupEl = btn.closest && btn.closest('[data-shots-group]') ? btn.closest('[data-shots-group]') : document.body;
        currentGroup = Array.prototype.slice.call(groupEl.querySelectorAll('[data-src]'));
        var idx = Math.max(0, currentGroup.indexOf ? currentGroup.indexOf(btn) : currentGroup.findIndex(function(x){return x===btn;}));
        openAt(idx, btn);
      };

      // Ресайз: пересчитываем «80% ширины сайта» и перецентрируем текущий скрин
      window.addEventListener('resize', function(){
        if (!root.classList.contains('qj-visible')) return;
        sessionWidth = measureSiteWidth80();
        var img = new Image();
        img.onload = function(){
          var ar = (img.naturalWidth && img.naturalHeight) ? (img.naturalWidth / img.naturalHeight) : (16/9);
          var box = computeBox(sessionWidth, ar);
          placePanel(box, currentGroup[currentIndex] || null);
        };
        img.src = imgEl.src || '';
      });
    })();</script>
  `;
}


function renderMaps(maps = []) {
  if (!maps?.length) {
    return '<div class="maps text-muted small">Карты: (нет)</div>';
  }
  return `
    <div class="maps mb-2">
      <div class="small text-secondary fw-semibold mb-1">Карты</div>
      <ul class="list-inline mb-0">
        ${maps.map(m => `<li class="list-inline-item">
          <span class="qj-tag qj-map-tag">${escapeHtml(m)}</span>
        </li>`).join('')}
      </ul>
    </div>
  `;
}


function renderDemos(demos = []) {
  if (!Array.isArray(demos) || demos.length === 0) {
    return '<div class="demos text-muted small">Демки: (нет)</div>';
  }
  const items = demos.map(u => {
    const url = String(u || '').trim();
    const safe = escapeHtml(url);
    return `<li class="mb-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></li>`;
  }).join('');
  return `
    <div class="demos mb-2">
      <div class="small text-secondary fw-semibold mb-1">Демки</div>
      <ul class="list-unstyled mb-0">
        ${items}
      </ul>
    </div>
  `;
}

// Результаты игровых карт для одной группы/финала/суперфинала
// Результаты игровых карт для одной группы/финала/суперфинала
function renderGroupResultsDetails(scope, group, resultsByGroup = new Map()) {
  const gid = Number(group.groupId);
  if (!Number.isFinite(gid) || !resultsByGroup || !resultsByGroup.size) return '';

  const list = resultsByGroup.get(gid);
  if (!list || !list.length) return '';

  // id для якоря секции "Подробнее" у конкретной группы
  const detailsId = `${scope}-${gid}-details`;

  // сортировка по времени матча (на всякий случай ещё раз)
  const items = list.slice().sort((a, b) => getMatchTimeValue(a) - getMatchTimeValue(b));

  // локальный хелпер для флага
  function renderPlayerFlagSmall(player) {
    // предполагаем, что при сохранении результатов карты в player.country уже лежит код страны (ru, ua, us и т.д.)
    const raw = (player && player.country) ? String(player.country).trim().toLowerCase() : '';
    const code = raw || '';
    const fileName = code ? `${code}.svg` : 'question.svg';
    const alt = code || '?';

    return `
      <img
        src="/media/flags/1x1/${escapeAttr(fileName)}"
        alt="${escapeAttr(alt)}"
        class="me-1"
        style="width:16px; height:16px; object-fit:cover; border-radius:2px; vertical-align:middle;"
      />
    `;
  }

  const blocks = items.map(r => {
    const mapName = r.map || r.mapNorm || '';
    const finishedStr = formatMatchFinishedRuMsk(r);
    const durationStr = formatMatchDuration(r);
    const players = Array.isArray(r.players) ? r.players.slice() : [];

    // сортировка игроков внутри карты:
    // по фрагам (убыв), при равных фрагах — по эффективности (убыв)
    players.sort((a, b) => {
      const fa = Number(a.frags) || 0;
      const fb = Number(b.frags) || 0;
      if (fb !== fa) return fb - fa;

      const ea = Number(a.eff) || 0;
      const eb = Number(b.eff) || 0;
      return eb - ea;
    });

    const rowsHtml = players.map(p => {
      const displayName = p.nameOrig || p.nameNorm || '';
      const flagHtml = renderPlayerFlagSmall(p);
      const nameHtml = `
        <span class="d-inline-flex align-items-center">
          <span>${escapeHtml(displayName)}</span>
        </span>
      `;

      return `
        <tr>
          <td>${nameHtml}</td>
          <td class="text-end">${Number(p.frags) || 0}</td>
          <td class="text-end">${Number(p.kills) || 0}</td>
          <td class="text-end">${Number(p.eff) || 0}</td>
          <td class="text-end">${Number(p.fph) || 0}</td>
          <td class="text-end">${Number(p.dgiv) || 0}</td>
          <td class="text-end">${Number(p.drec) || 0}</td>
        </tr>
      `;
    }).join('');

    return `
      <div class="card shadow-sm mb-3">
        <div class="card-body py-3">
          <div class="d-flex flex-wrap align-items-center small text-muted mb-2">
            <div class="me-3">
              🎮 Карта: <span class="fw-semibold">${escapeHtml(mapName || '(не указано)')}</span>
            </div>
            <div class="me-3">
              ${finishedStr ? `⏱️ Завершена: ${escapeHtml(finishedStr)}` : ''}
            </div>
            <div class="me-3">
              ${durationStr ? `⏳ Длительность: ${escapeHtml(durationStr)}` : ''}
            </div>
          </div>
          <div class="table-responsive">
            <table class="table table-sm table-striped align-middle qj-table mb-0 js-sortable-table">
              <thead>
                <tr>
                  <th class="small text-secondary" data-sort-type="string">Игрок</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Frags</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Deaths</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Eff</th>
                  <th class="small text-secondary text-end" data-sort-type="number">FPH</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Dgiv</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Drec</th>
                </tr>
              </thead>
              <tbody>${rowsHtml}</tbody>
            </table>
          </div>
        </div>
      </div>
    `;
  }).join('');

  // ВАЖНО: по умолчанию секция "Подробнее" всегда свёрнута (open не ставим)
  return `
    <details id="${escapeHtml(detailsId)}" class="sub-collapse mt-3">
      <summary class="qj-toggle">
        <span class="section-title">👁️ Подробнее</span>
        <a href="#${escapeHtml(detailsId)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел Подробнее">#</a>
      </summary>
      <div class="mt-2">
        <div class="small text-secondary mb-2">
          🗺️ Результаты карт группы №${escapeHtml(String(group.groupId))}
        </div>
        ${blocks}
      </div>
    </details>
  `;
}


function renderDefinedRating(title, data, sectionId, collapsedByDefault = false, achIndex = null) {
  const players = Array.isArray(data?.players) ? data.players : [];
  if (!players.length) return '';

  const rows = players.map((p, i) => {
    const badges = renderAchievementBadgesInline(p.nameNorm, achIndex);
    const pname = p.nameOrig || p.nameNorm || '';
    const pnameHtml = PLAYER_STATS_ENABLED
      ? `<a href="#" class="player-link qj-accent fw-semibold js-player-stat"
             data-player="${escapeAttr(pname)}">${escapeHtml(pname)}</a>`
      : `<span class="qj-accent fw-semibold">${escapeHtml(pname)}</span>`;
    return `
      <tr>
        <td class="pos text-muted">${i + 1}</td>
        <td class="pname">${pnameHtml}${badges}</td>
        <td class="pts qj-pts fw-semibold">${Number(p.rank)}</td>
      </tr>
    `;
  }).join('');

  const openAttr = collapsedByDefault ? '' : ' open';
  const updatedAtStr = data?.updatedAt ? formatRuMskDateTime(data.updatedAt) : null;
  const updatedAtHtml = updatedAtStr ? `<div class="small text-muted mt-1">обновлено: ${escapeHtml(updatedAtStr)}</div>` : '';

  return `
    <section class="mb-4">
      <details id="${escapeHtml(sectionId)}" class="sub-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">${escapeHtml(title)}</span>
          <a href="#${escapeHtml(sectionId)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${players.length}</span>
        </summary>
        <div class="mt-2">
          <div class="table-responsive">
            <table class="table table-hover align-middle rating-table qj-table">
              <thead>
                <tr>
                  <th class="pos small text-secondary" style="width:64px;">№</th>
                  <th class="small text-secondary">Игрок</th>
                  <th class="small text-secondary text-end" style="width:120px;">Ранг</th>
                </tr>
              </thead>
              <tbody>${rows}</tbody>
            </table>
            ${updatedAtHtml}
          </div>
        </div>
      </details>
    </section>
  `;
}

function renderMapsListCard(mapsList = [], collapsedByDefault = false) {
  if (!Array.isArray(mapsList) || mapsList.length === 0) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  const items = mapsList.map(m => {
    const name = m?.nameOrig || m?.nameNorm || '';
    return `<li class="list-inline-item"><span class="qj-tag qj-map-tag">${escapeHtml(name)}</span></li>`;
  }).join('');
  return `
    <div class="col-12 col-lg-4">
      <div class="card shadow-sm h-100">
        <div class="card-body">
          <details id="section-maps-list" class="sub-collapse"${openAttr}>
            <summary class="qj-toggle">
              <span class="section-title">Список карт</span>
              <a href="#section-maps-list" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
            </summary>
            <div class="mt-2">
              <ul class="list-inline mb-0">
                ${items}
              </ul>
            </div>
          </details>
        </div>
      </div>
    </div>
  `;
}


async function getMaps(chatId) {
  return colMaps.find({ chatId }).sort({ nameOrig: 1 }).toArray();
}

function toYouTubeEmbed(url) {
  try {
    const u = new URL(url);
    if (u.hostname.includes('youtu.be')) {
      const id = u.pathname.slice(1);
      if (id) return `https://www.youtube.com/embed/${id}`;
    }
    if (u.hostname.includes('youtube.com')) {
      const id = u.searchParams.get('v');
      if (id) return `https://www.youtube.com/embed/${id}`;
    }
  } catch (_) { }
  return null;
}

// Для Twitch нужен параметр parent=hostname — зададим на клиенте через data-* атрибут
function parseTwitchChannel(url) {
  try {
    const u = new URL(url.startsWith('http') ? url : 'https://' + url);
    const host = u.hostname.replace(/^www\./, '');
    if (host === 'twitch.tv') {
      const parts = u.pathname.split('/').filter(Boolean);
      if (parts[0]) return parts[0];
    }
  } catch (_) { }
  return null;
}

// VK Play (если удастся в iframe; иначе оставим ссылкой)
function toVkPlayEmbed(url) {
  try {
    const u = new URL(url.startsWith('http') ? url : 'https://' + url);
    if (u.hostname.includes('vkplay')) {
      return u.toString();
    }
  } catch (_) { }
  return null;
}

// VK Video: обычные ссылки вида https://<subdomain>.vkvideo.ru/video-<oid>_<id>
// Преобразуем в официальный embed-плеер VK: https://vk.com/video_ext.php?oid=<oid>&id=<id>&hd=2
function toVkVideoEmbed(url) {
  try {
    const u = new URL(url.startsWith('http') ? url : 'https://' + url);
    // Любые поддомены *.vkvideo.ru и прямые vk.com/video-... тоже поддержим
    const host = u.hostname.replace(/^www\./, '');
    const isVkVideo = host.endsWith('vkvideo.ru') || host === 'vk.com';
    if (!isVkVideo) return null;

    // Ищем в pathname шаблон /video-<oid>_<id> или просто video-<oid>_<id>
    const m = u.pathname.match(/\/?video(-?\d+)_(\d+)/i);
    if (!m) return null;
    const oid = m[1];
    const id = m[2];
    return `https://vk.com/video_ext.php?oid=${encodeURIComponent(oid)}&id=${encodeURIComponent(id)}&hd=2`;
  } catch (_) { }
  return null;
}

// RuTube: обычные ссылки вида https://rutube.ru/video/<id>/...
// Embed: https://rutube.ru/play/embed/<id>
function toRutubeEmbed(url) {
  try {
    const u = new URL(url.startsWith('http') ? url : 'https://' + url);
    const host = u.hostname.replace(/^www\./, '');
    if (host !== 'rutube.ru') return null;
    const m = u.pathname.match(/\/video\/([0-9a-f]{10,})/i);
    if (!m) return null;
    return `https://rutube.ru/play/embed/${m[1]}`;
  } catch (_) { }
  return null;
}

function escapeRegExp(str = '') {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Удаляем «голые» URL-строки и нормализуем пустые строки
function escapeRegExp(str = '') {
  return String(str).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// Удаляем «голые» URL-строки и нормализуем пустые строки
function cleanupNewsTextForEmbeds(text = '', urls = []) {
  let s = String(text || '').replace(/\r\n?/g, '\n');

  // Удаляем строки, которые целиком состоят из URL (по которым построен embed)
  for (const u of urls || []) {
    if (!u) continue;
    const re = new RegExp(`^\\s*${escapeRegExp(u)}\\s*$`, 'gm');
    s = s.replace(re, '');
  }

  // Удаляем трейлинговые пробелы по строкам
  s = s.replace(/[ \t]+\n/g, '\n');

  // Схлопываем 2+ пустых строк в одну (важно после удаления строк с URL)
  s = s.replace(/\n{2,}/g, '\n');

  // Подчищаем ведущие/замыкающие пустые строки
  s = s.replace(/^\n+|\n+$/g, '');

  return s;
}

// Извлечение всех http/https ссылок из текста
function extractUrls(text = '') {
  const s = String(text || '');
  const re = /\bhttps?:\/\/[^\s<>"')]+/gi;
  const out = [];
  let m;
  while ((m = re.exec(s))) {
    out.push(m[0]);
  }
  return out;
}

// HTML embed для одного URL (для новостей — только iframe, без «второй» ссылки)
function mediaEmbedBlockFromUrl(urlRaw = '') {
  const url = String(urlRaw || '').trim();
  if (!url) return '';

  // YouTube
  const yt = toYouTubeEmbed(url);
  if (yt) {
    return `
      <div class="stream-embed mb-2">
        <iframe
                data-src="${yt}"
                title="Видео YouTube"
                loading="lazy"
                tabindex="-1"
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                allowfullscreen
                class="js-video-iframe"
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // Twitch (канал)
  const twitchChan = parseTwitchChannel(url);
  if (twitchChan) {
    return `
      <div class="stream-embed mb-2">
        <iframe class="js-video-iframe js-twitch-embed"
                data-channel="${escapeHtml(twitchChan)}"
                title="Видео Twitch"
                loading="lazy"
                tabindex="-1"
                allowfullscreen
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // VK Video (vkvideo.ru -> vk.com/video_ext.php)
  const vkVideo = toVkVideoEmbed(url);
  if (vkVideo) {
    return `
      <div class="stream-embed mb-2">
        <iframe data-src="${escapeHtml(vkVideo)}"
                title="Видео VK"
                loading="lazy"
                tabindex="-1"
                allowfullscreen
                class="js-video-iframe"
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // RuTube
  const rutube = toRutubeEmbed(url);
  if (rutube) {
    return `
      <div class="stream-embed mb-2">
        <iframe data-src="${escapeHtml(rutube)}"
                title="Видео RuTube"
                loading="lazy"
                tabindex="-1"
                allowfullscreen
                class="js-video-iframe"
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // VK Play
  const vk = toVkPlayEmbed(url);
  if (vk) {
    return `
      <div class="stream-embed mb-2">
        <iframe data-src="${escapeHtml(vk)}"
                title="Видео"
                loading="lazy"
                tabindex="-1"
                allowfullscreen
                class="js-video-iframe"
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  return '';
}


// HTML iframe для одного URL (вариант «инлайн» — без дивов), для ачивок/перков
function mediaIframeInlineFromUrl(urlRaw = '') {
  const url = String(urlRaw || '').trim();
  if (!url) return '';

  // YouTube
  const yt = toYouTubeEmbed(url);
  if (yt) {
    return `<iframe data-src="${yt}" title="Видео YouTube"
                    loading="lazy" tabindex="-1"
                    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                    allowfullscreen
                    class="js-video-iframe"
                    style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // Twitch
  const twitchChan = parseTwitchChannel(url);
  if (twitchChan) {
    return `<iframe class="js-video-iframe js-twitch-embed" data-channel="${escapeHtml(twitchChan)}"
                    title="Видео Twitch"
                    loading="lazy" tabindex="-1"
                    allowfullscreen
                    style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // VK Video
  const vkVideo = toVkVideoEmbed(url);
  if (vkVideo) {
    return `<iframe data-src="${vkVideo}" title="Видео VK"
                    loading="lazy" tabindex="-1"
                    allowfullscreen
                    class="js-video-iframe"
                    style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // RuTube
  const rutube = toRutubeEmbed(url);
  if (rutube) {
    return `<iframe data-src="${rutube}" title="Видео RuTube"
                    loading="lazy" tabindex="-1"
                    allowfullscreen
                    class="js-video-iframe"
                    style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // VK Play
  const vk = toVkPlayEmbed(url);
  if (vk) {
    return `<iframe data-src="${vk}" title="Видео"
                    loading="lazy" tabindex="-1"
                    allowfullscreen
                    class="js-video-iframe"
                    style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  return '';
}


// Формирование набора embed-блоков для новостей по исходному тексту
function renderNewsEmbeds(text = '') {
  const urls = extractUrls(text);
  const usedUrls = [];
  const seen = new Set();
  const blocks = urls.map(u => {
    if (seen.has(u)) return '';
    const html = mediaEmbedBlockFromUrl(u);
    if (html) { seen.add(u); usedUrls.push(u); }
    return html;
  }).join('');
  return { html: blocks, usedUrls };
}


// Вставка инлайн-iframe в HTML ачивок/перков: если ссылка — видео, заменяем её на iframe (без дублирования ссылки)
function injectEmbedsIntoHtml(html = '') {
  let s = String(html || '');
  s = s.replace(/<a\b([^>]*)>([\s\S]*?)<\/a>/gi, (m, attrs) => {
    const mHref = attrs.match(/\bhref\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/i);
    const href = mHref ? (mHref[1] || mHref[2] || mHref[3] || '') : '';
    const iframe = mediaIframeInlineFromUrl(href);
    if (!iframe) return m; // не видео — оставляем ссылку как есть
    // Видео — возвращаем только плеер (компактно, без лишних переводов строк)
    return iframe;
  });
  return s;
}


// Подсчёт частоты назначения карт для набора групп
function computeMapStats(items = []) {
  const map = new Map(); // key: lower-case name => { name, count }
  for (const g of items || []) {
    const arr = Array.isArray(g.maps) ? g.maps : [];
    for (const m of arr) {
      const name = String(m || '').trim();
      if (!name) continue;
      const key = name.toLowerCase();
      const cur = map.get(key);
      if (cur) {
        cur.count++;
      } else {
        map.set(key, { name, count: 1 });
      }
    }
  }
  const rows = Array.from(map.values());
  rows.sort((a, b) => b.count - a.count || a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
  return rows;
}

// Таблица «Рейтинг карт» для раздела
function renderMapsPopularityTable(sectionId, items = [], collapsedByDefault = false) {
  const stats = computeMapStats(items);
  if (!stats.length) return '';

  const tr = stats.map((r, idx) => `
    <tr>
      <td class="pos text-muted">${idx + 1}</td>
      <td class="map-name"><span class="qj-tag qj-map-tag">${escapeHtml(r.name)}</span></td>
      <td class="cnt text-end fw-semibold">${r.count}</td>
    </tr>
  `).join('');

  const openAttr = collapsedByDefault ? '' : ' open';

  return `
    <section class="mb-4">
      <details id="${escapeHtml(sectionId)}" class="sub-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">🔂 Частота назначений карт</span>
          <a href="#${escapeHtml(sectionId)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${stats.length}</span>
        </summary>
        <div class="mt-2">
          <div class="table-responsive">
            <table class="table table-hover align-middle qj-table maps-rating-table">
              <thead>
                <tr>
                  <th class="small text-secondary" style="width:64px;">№</th>
                  <th class="small text-secondary">Карта</th>
                  <th class="small text-secondary text-end" style="width:180px;">Кол-во назначений</th>
                </tr>
              </thead>
              <tbody>${tr}</tbody>
            </table>
          </div>
        </div>
      </details>
    </section>
  `;
}


//секция описания турнира
// секция описания турнира — без внутреннего container, ширина как у остальных секций
// секция описания турнира — без внутреннего container, ширина как у остальных секций
function renderTournamentDescSection(tournament, containerClass, collapsedByDefault = false) {
  if (!tournament?.desc) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  //const descHtml = linkify(tournament.desc);
  let descHtml = linkify(tournament.desc);
  descHtml = linkifyTelegramHandles(descHtml);

  return `
    <section class="mb-4">
      <details id="section-desc" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">ℹ️ Описание турнира</span>
          <a href="#section-desc" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
        </summary>
        <div class="mt-2">
          <div class="card shadow-sm">
            <div class="card-body">
              <div class="tournamentDesc" style="white-space: pre-wrap;">${descHtml}</div>
            </div>
          </div>
        </div>
      </details>
    </section>
  `;
}




// Полный верхний блок — в режиме Q2CSS выводим те же секции (серверы/пак/список карт) как отдельные секции.
// Параметр linksOnly влияет только на стримы (если решите их тоже делать тут).
function renderTournamentExtrasFull(tournament, containerClass, linksOnly = false, collapsedByDefault = false, mapsList = []) {
  const serversSec = renderServersSection(tournament, containerClass, collapsedByDefault);
  const packSec = renderPackSection(tournament, containerClass, collapsedByDefault);
  const mapsSec = renderMapsListSection(mapsList, containerClass, collapsedByDefault);
  // Примечание: блок «стримеры» оставляем без изменений внизу страницы (как и было ранее),
  // поэтому здесь ничего дополнительного не добавляем.
  if (!serversSec && !packSec && !mapsSec) return '';
  return serversSec + packSec + mapsSec;
}


// Только верхние блоки (серверы, пак и список карт) — отдельными секциями уровня «Описание/Новости»
function renderTournamentExtrasTopOnly(tournament, containerClass, collapsedByDefault = false, mapsList = []) {
  const serversSec = renderServersSection(tournament, containerClass, collapsedByDefault);
  const packSec = renderPackSection(tournament, containerClass, collapsedByDefault);
  const mapsSec = renderMapsListSection(mapsList, containerClass, collapsedByDefault);
  if (!serversSec && !packSec && !mapsSec) return '';
  return serversSec + packSec + mapsSec;
}

function renderTournamentStatsSection(statsUrl, containerClass, collapsedByDefault = true) {
  const url = String(statsUrl || '').trim();
  if (!url) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  const safe = escapeHtml(url);
  return `
    <section class="mb-5">
      <details id="section-stats" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">📈 Статистика турнира</span>
          <a href="#section-stats" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
        </summary>
        <div class="mt-2">
          <div class="card shadow-sm h-100">
            <div class="card-body">
              <iframe src="${safe}"
                      title="Статистика турнира"
                      loading="lazy"
                      style="width:100%; min-height:70vh; border:0; border-radius:10px;"></iframe>
            </div>
          </div>
        </div>
      </details>
    </section>
  `;
}


function renderSection(title, items, scope, screensMap, ptsMap = null, collapsedByDefault = false, achIndex = null, resultsByGroup = new Map()) {
  if (!items?.length) return '<div class="text-muted">Нет данных</div>';

  const label = (scope === 'group') ? '🎯 Квалификация' : (scope === 'final') ? '🏆 Финал' : '👑 Суперфинал';
  const openAttr = collapsedByDefault ? '' : ' open';

  const cells = items.map(g => {
    const id = `${scope}-${g.groupId}`;
    //const players = renderPlayers(g.players || [], ptsMap, achIndex);
    const players = renderPlayers(
      g.players || [],
      ptsMap,
      achIndex,
      resultsByGroup,
      g.groupId,
      scope === 'superfinal'
    );
    const maps = renderMaps(g.maps || []);
    const demos = renderDemos(Array.isArray(g.demos) ? g.demos : []);
    const files = screensMap.get(Number(g.groupId)) || [];
    const shots = renderScreenshots(files);
    const timeLine = renderTimeStr(g.time);

    // НОВОЕ: секция "Подробнее" с результатами карт для этой группы
    const detailsHtml = renderGroupResultsDetails(scope, g, resultsByGroup);

    return `
      <div>
        <details id="${escapeHtml(id)}" class="sub-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">${label} №${g.groupId}</span>
            <a href="#${escapeHtml(id)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на ${label.toLowerCase()} №${g.groupId}">#</a>
          </summary>
          <div class="mt-2">
            <div class="card shadow-sm h-100">
              <div class="card-body d-flex flex-column">
                ${timeLine || ''}
                ${players}
                ${maps}
                ${demos}
                <div class="mt-auto">${shots}</div>
                ${detailsHtml} <!-- "Подробнее" идёт сразу после области со скриншотами -->
              </div>
            </div>
          </div>
        </details>
      </div>
    `;
  }).join('');

  //return `<div class="cards-grid cards-grid--stage">${cells}</div>`;
  // ВАЖНО: суперфинал получает модификатор .cards-grid--super (одна карточка в ряд на десктопе)
  const gridClass = (scope === 'superfinal')
    ? 'cards-grid cards-grid--stage cards-grid--super'
    : 'cards-grid cards-grid--stage';
  return `<div class="${gridClass}">${cells}</div>`;
}

function renderStageRating(
  title,
  items,
  ptsMap,
  sectionId,
  collapsedByDefault = false,
  achIndex = null,
  resultsByGroup = new Map()
) {
  if (!items?.length || !ptsMap || ptsMap.size === 0) return '';

  // --- 1) Считаем суммарную статистику по игрокам по результатам карт ---
  const stageStats = new Map(); // key = nameNorm

  if (resultsByGroup && resultsByGroup.size) {
    for (const g of items) {
      const gid = Number(g.groupId);
      if (!Number.isFinite(gid)) continue;
      const matches = resultsByGroup.get(gid);
      if (!matches || !matches.length) continue;

      for (const m of matches) {
        const players = Array.isArray(m.players) ? m.players : [];
        for (const p of players) {
          const nameNorm = p?.nameNorm;
          if (!nameNorm) continue;

          let s = stageStats.get(nameNorm);
          if (!s) {
            s = {
              frags: 0,
              kills: 0,
              dgiv: 0,
              drec: 0,
              effSum: 0,
              effCount: 0,
              fphSum: 0,
              fphCount: 0,
              nameOrigLast: p.nameOrig || p.nameNorm || '',
            };
            stageStats.set(nameNorm, s);
          }

          const fr = Number(p.frags);
          const kl = Number(p.kills);
          const ef = Number(p.eff);
          const fp = Number(p.fph);
          const dg = Number(p.dgiv);
          const dr = Number(p.drec);

          if (Number.isFinite(fr)) s.frags += fr;
          if (Number.isFinite(kl)) s.kills += kl;
          if (Number.isFinite(dg)) s.dgiv += dg;
          if (Number.isFinite(dr)) s.drec += dr;
          if (Number.isFinite(ef)) { s.effSum += ef; s.effCount++; }
          if (Number.isFinite(fp)) { s.fphSum += fp; s.fphCount++; }

          if (p.nameOrig) s.nameOrigLast = p.nameOrig;
        }
      }
    }
  }

  // --- 2) Собираем строки рейтинга (по pts) ---
  const seen = new Set();
  const rows = [];

  for (const g of items) {
    for (const p of (g.players || [])) {
      if (!p?.nameNorm) continue;
      if (seen.has(p.nameNorm)) continue;
      if (!ptsMap.has(p.nameNorm)) continue;

      seen.add(p.nameNorm);
      const pts = Number(ptsMap.get(p.nameNorm));
      const stats = stageStats.get(p.nameNorm) || null;

      let effAvg = '';
      let effAvgNum = Number.NEGATIVE_INFINITY;
      let fphAvg = '';
      let frags = '';
      let kills = '';
      let dgiv = '';
      let drec = '';

      if (stats) {
        if (stats.effCount > 0) {
          const val = stats.effSum / stats.effCount;
          effAvgNum = val;
          effAvg = val.toFixed(1);
        }
        if (stats.fphCount > 0) fphAvg = (stats.fphSum / stats.fphCount).toFixed(1);
        if (stats.frags !== 0) frags = String(stats.frags);
        if (stats.kills !== 0) kills = String(stats.kills);
        if (stats.dgiv !== 0) dgiv = String(stats.dgiv);
        if (stats.drec !== 0) drec = String(stats.drec);
      }

      rows.push({
        nameOrig: p.nameOrig || (stats && stats.nameOrigLast) || p.nameNorm,
        nameNorm: p.nameNorm,
        pts,
        frags,
        kills,
        effAvg,
        effAvgNum,
        fphAvg,
        dgiv,
        drec,
      });
    }
  }

  if (!rows.length) return '';

  // сортировка:
  // 1) по очкам (как было, по возрастанию)
  // 2) при равных очках — по средней эффективности (effAvgNum, по убыванию)
  // 3) при полном равенстве — по имени
  rows.sort((a, b) => {
    if (a.pts !== b.pts) return a.pts - b.pts;

    const ea = Number.isFinite(a.effAvgNum) ? a.effAvgNum : Number.NEGATIVE_INFINITY;
    const eb = Number.isFinite(b.effAvgNum) ? b.effAvgNum : Number.NEGATIVE_INFINITY;
    if (eb !== ea) return eb - ea;

    return a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' });
  });

  const tr = rows.map((r, i) => {
    const pos = i + 1;
    const achBadges = renderAchievementBadgesInline(r.nameNorm, achIndex);
    const displayName = r.nameOrig || r.nameNorm || '';

    const pnameHtml = PLAYER_STATS_ENABLED
      ? `<a href="#" class="player-name player-link qj-accent fw-semibold js-player-stat"
              data-player="${escapeAttr(displayName)}">${escapeHtml(displayName)}</a>`
      : `<span class="player-name qj-accent fw-semibold">${escapeHtml(displayName)}</span>`;

    return `
      <tr>
        <td class="small text-muted">${pos}.</td>
        <td>
          ${pnameHtml}
          ${achBadges ? `<span class="ms-2">${achBadges}</span>` : ''}
        </td>
        <td class="text-end small">${Number.isFinite(r.pts) ? r.pts : ''}</td>
        <td class="text-end small">${r.frags}</td>
        <td class="text-end small">${r.kills}</td>
        <td class="text-end small">${r.effAvg}</td>
        <td class="text-end small">${r.fphAvg}</td>
        <td class="text-end small">${r.dgiv}</td>
        <td class="text-end small">${r.drec}</td>
      </tr>
    `;
  }).join('');

  const openAttr = collapsedByDefault ? '' : ' open';
  const idAttr = sectionId ? ` id="${escapeHtml(sectionId)}"` : '';

  return `
    <section class="mb-4">
      <details class="stage-collapse"${openAttr}${idAttr}>
        <summary class="qj-toggle">
          <span class="section-title">${escapeHtml(title)}</span>
          <a href="#${escapeHtml(sectionId)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
        </summary>
        <div class="mt-2">
          <div class="table-responsive">
            <table class="table table-hover align-middle rating-table qj-table js-sortable-table">
              <thead>
                <tr>
                  <th class="pos small text-secondary" style="width:64px;">№</th>
                  <th class="small text-secondary" data-sort-type="string">Игрок</th>
                  <th class="small text-secondary text-end" style="width:120px;" data-sort-type="number">Очки</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Frags</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Deaths</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Eff (avg)</th>
                  <th class="small text-secondary text-end" data-sort-type="number">FPH (avg)</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Dgiv</th>
                  <th class="small text-secondary text-end" data-sort-type="number">Drec</th>
                </tr>
              </thead>
              <tbody>${tr}</tbody>
            </table>
          </div>
        </div>
      </details>
    </section>
  `;
}



// UPDATED: современный визуал (градиентные заголовки по типам секций, «стекло»-карточки, улучшенные акценты)
// ВАЖНО: эта версия рассчитана на обычный режим (без forceQuake2ComRuCSS=1); ретро‑тема не затрагивается.
function renderPage({
  tournament, groups, finals, superfinals,
  groupScreens, finalScreens, superScreens,
  groupPtsMap, finalPtsMap, superFinalPtsMap,
  tournamentNews, groupsNews, finalsNews, superNews,
  useQ2Css = false,
  collapseAll = false,
  definedGroupRating = null,
  definedFinalRating = null,
  definedSuperFinalRating = null,  // новый параметр с дефолтом
  customGroups = [],
  customPointsByGroup = new Map(),
  customScreens = new Map(),
  achievementsAch = [],
  achievementsPerc = [],
  achievementsIndex = new Map(),
  statsBaseUrl = '',
  mapsList = [],
  sectionOrder = [],
  // НОВОЕ:
  tournamentsMeta = [],           // [{id, name}], для селектора
  selectedChatId = null,          // текущий выбранный chatId
  // НОВОЕ: селектор подтурниров
  subTournamentsMeta = null,      // [{ id, name, code }]
  subRootChatId = null,           // chatId корневого турнира
  subRootAlias = null,            // алиас корневого турнира (T)
  // НОВОЕ: результаты карт по стадиям (Map<groupId, Array<result>>)
  groupResultsByGroup = new Map(),
  finalResultsByGroup = new Map(),
  superResultsByGroup = new Map(),
  feedbackEntries = [],
  // НОВОЕ:
  users = [],
  teams = [],
  registrationSettings = null,
  signups = [],
}) {
  const logoUrl = tournament.logo?.relPath ? `/media/${relToUrl(tournament.logo.relPath)}` : null;
  const logoMime = tournament.logo?.mime || 'image/png';

  const tournamentBackUrl = tournament.back?.relPath ? `/media/${relToUrl(tournament.back.relPath)}` : SITE_BG_IMAGE;

  const editTimeout = parseInt(process.env.SECTIONS_EDIT_TIMEOUT_SECONDS || '3600', 10);

  // НОВОЕ: построить индексы стран игроков для флагов
  initPlayerCountryIndexes(signups, users);

  const faviconLink = logoUrl
    ? `<link rel="icon" type="${escapeHtml(logoMime)}" href="${logoUrl}">`
    : `<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='30' fill='%23007bff'/%3E%3Ctext x='32' y='39' font-family='Arial' font-size='28' text-anchor='middle' fill='white'%3EQ%3C/text%3E%3C/svg%3E'>`;

  const logoBlock = logoUrl ? `<img src="${logoUrl}" alt="Логотип турнира" class="hero-logo me-3" />` : '';
  const siteLink = tournament.site
    ? `<a href="${escapeHtml(tournament.site)}" target="_blank" rel="noopener" class="small text-muted text-decoration-none" title="Этот сайт">${escapeHtml('🏠Этот сайт')}</a>`
    : '';
  const siteWiki = tournament.wiki
    ? `<a href="${escapeHtml(tournament.wiki)}" target="_blank" rel="noopener" class="text-decoration-none" title="WiKi турнира">${escapeHtml('📖WiKi')}</a>`
    : '';

  const newsChannelLink = tournament.newsChannel
    ? (() => {
      const h = tournament.newsChannel.trim();                 // может быть с @
      const handle = h.replace(/^@/, '');
      const href = `https://t.me/${encodeURIComponent(handle)}`;
      // оборачиваем так же, как siteLink (под твой стиль ссылок)
      //return `<a href="${href}" target="_blank" rel="noopener" class="link-success link-underline-opacity-0 link-underline-opacity-50-hover">${escapeHtml(h)}</a>`;
      return `<a href="${href}" target="_blank" rel="noopener" class="link-success link-underline-opacity-0 link-underline-opacity-50-hover" title="Новости в Telegram">${escapeHtml('✈️Telegram')}</a>`;
    })()
    : '';

  // NEW: ссылка на аналитику для текущего selectedChatId
  const analyticsUrl = (selectedChatId != null)
    ? `/analytics?${encodeURIComponent(TOURNAMENT_QUERY_PARAM)}=${encodeURIComponent(String(selectedChatId))}`
    : '';

  // Бейдж "A" для мобильной шапки
  const analyticsBadgeMobile = analyticsUrl
    ? `<a href="${escapeAttr(analyticsUrl)}" class="qj-badge-circle qj-badge-analytics" title="Аналитика по турниру">📊</a>`
    : '';

  const containerClass = useQ2Css ? 'container-fluid px-0' : 'container';

  // НОВОЕ: селектор турниров (показываем только если турниров > 1)
  const tournamentSelectHtml = Array.isArray(tournamentsMeta) && tournamentsMeta.length > 1
    ? (() => {
      // Если есть корневой турнир для текущего подтурнира — считаем выбранным его.
      const effectiveSelectedId = (subRootChatId != null && !Number.isNaN(Number(subRootChatId)))
        ? Number(subRootChatId)
        : (selectedChatId != null ? Number(selectedChatId) : null);

      const opts = tournamentsMeta.map(t => {
        const tIdNum = Number(t.id);
        const isSelected = (effectiveSelectedId != null && tIdNum === effectiveSelectedId);

        const alias = getSiteNameForChatId(t.id);
        const value = alias || String(t.id);              // то, что пойдёт в ?T=
        const label = t.name || alias || `Чат ${t.id}`;   // текст для пользователя
        const sel = isSelected ? ' selected' : '';
        return `<option value="${escapeAttr(value)}" data-chat-id="${escapeAttr(String(t.id))}"${sel}>${escapeHtml(label)}</option>`;
      }).join('');

      return `
          <div class="d-flex align-items-center gap-2 qj-tournament-select-wrapper">
            <span class="small text-secondary"></span>
            <select class="form-select form-select-sm js-tournament-select" style="min-width: 240px;">
              ${opts}
            </select>
          </div>
        `;
    })()
    : '';


  // НОВОЕ: селектор подтурнира (subTournaments)
  let subTournamentSelectHtml = '';
  if (Array.isArray(subTournamentsMeta) && subTournamentsMeta.length > 0) {
    const options = subTournamentsMeta.map(item => {
      const id = Number(item.id);
      const selected = (Number(selectedChatId) === id) ? ' selected' : '';
      const name = String(item.name || '').trim() || `Турнир ${id}`;
      const code = (item.code || '').toString();
      const valueAttr = escapeAttr(code);
      return `<option value="${valueAttr}"${selected}>${escapeHtml(name)}</option>`;
    }).join('');

    const rootAliasSafe = subRootAlias ? escapeAttr(subRootAlias) : '';
    const rootIdSafe = Number.isFinite(Number(subRootChatId)) ? Number(subRootChatId) : '';

    subTournamentSelectHtml = `
        <div class="qj-subtournament-select-wrapper mt-2">
          <label class="form-label mb-1 small text-muted">Подтурнир</label>
          <select class="form-select form-select-sm js-subtournament-select"
                  ${rootAliasSafe ? `data-root-alias="${rootAliasSafe}"` : ''}
                  ${rootIdSafe ? `data-root-chat-id="${rootIdSafe}"` : ''}>
            ${options}
          </select>
        </div>
      `;
  }

  // Верхние отдельные секции
  const serversSec = renderServersSection(tournament, containerClass, collapseAll);
  const packSec = renderPackSection(tournament, containerClass, collapseAll);
  const mapsListSec = renderMapsListSection(mapsList, containerClass, collapseAll);

  const descSection = renderTournamentDescSection(tournament, containerClass, collapseAll);

  const groupsCards = renderSection('🎯 Квалификации', groups, 'group', groupScreens, groupPtsMap, collapseAll, achievementsIndex, groupResultsByGroup);
  const finalsCards = renderSection('🏆 Финальный раунд', finals, 'final', finalScreens, finalPtsMap, collapseAll, achievementsIndex, finalResultsByGroup);
  const superCards = renderSection('👑 Суперфинал', superfinals, 'superfinal', superScreens, superFinalPtsMap, collapseAll, achievementsIndex, superResultsByGroup);

  const groupsMapsRatingSec = renderMapsPopularityTable('maps-groups', groups, collapseAll);
  const finalsMapsRatingSec = renderMapsPopularityTable('maps-finals', finals, collapseAll);
  const superMapsRatingSec = renderMapsPopularityTable('maps-superfinals', superfinals, collapseAll);

  const groupsNewsSec = renderNewsList('📰 Новости квалификаций', groupsNews, collapseAll, 'section-news-groups');
  const finalsNewsSec = renderNewsList('📰 Новости финального раунда', finalsNews, collapseAll, 'section-news-finals');
  const superNewsSec = renderNewsList('📰 Новости суперфинала', superNews, collapseAll, 'section-news-super');

  const groupsRatingSec = renderStageRating(
    '📋 Результаты квалификации',
    groups, groupPtsMap, 'rating-groups', collapseAll, achievementsIndex, groupResultsByGroup
  );
  const finalsRatingSec = renderStageRating(
    '📋 Результаты финального раунда',
    finals, finalPtsMap, 'rating-finals', collapseAll, achievementsIndex, finalResultsByGroup
  );
  const superRatingSec = renderStageRating(
    '📋 Результаты суперфинала',
    superfinals, superFinalPtsMap, 'rating-superfinals', collapseAll, achievementsIndex, superResultsByGroup
  );

  const groupsDefinedRatingSec = renderDefinedRating(
    '🥇 Рейтинг квалификации',
    definedGroupRating, 'rating-groups-defined', collapseAll, achievementsIndex
  );
  const finalsDefinedRatingSec = renderDefinedRating(
    '🥇 Рейтинг финального раунда',
    definedFinalRating, 'rating-finals-defined', collapseAll, achievementsIndex
  );

  // Новый блок: окончательный рейтинг суперфинала
  const superfinalsDefinedRatingSec = renderDefinedRating(
    '🥇 Рейтинг суперфинала',
    definedSuperFinalRating, 'rating-superfinals-defined', collapseAll, achievementsIndex
  );

  const customCards = renderCustomSection(customGroups, customPointsByGroup, customScreens, collapseAll, achievementsIndex);
  const openAttr = collapseAll ? '' : ' open';
  const customWholeSec = (customGroups && customGroups.length) ? `
    <section class="mb-5">
      <details id="section-custom" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">Дополнительные группы</span>
          <a href="#section-custom" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${customGroups.length}</span>
        </summary>
        <div class="mt-2">
          ${customCards}
        </div>
      </details>
    </section>
  ` : '';

  const achievementsAchSec = renderAchievementsSectionTitled('🏅 Ачивки (оплачиваемые достижения)', 'section-achievements', achievementsAch, collapseAll);
  const perksSec = renderAchievementsSectionTitled('⚡ Перки', 'section-perks', achievementsPerc, collapseAll);

  const tournamentNewsSecHtml = renderNewsList('📰 Новости турнира', tournamentNews, collapseAll, 'section-news-tournament');

  // Статистика (если включена и есть URL)
  const statsBaseNorm = (PLAYER_STATS_ENABLED && statsBaseUrl) ? statsBaseUrl : '';
  const tournamentStatsSec = renderTournamentStatsSection(statsBaseNorm, containerClass, true);

  // Бейдж "C" (Статистика) для мобильной шапки — по тем же условиям, что и меню "Статистика"
  const statsBadgeMobile = statsBaseNorm
    ? `<a href="#section-stats" class="qj-badge-circle qj-badge-stats" title="Статистика турнира">📈</a>`
    : '';

  // Стримеры
  const hasStreams = tournamentHasStreams(tournament);
  const streamsSec = hasStreams
    ? renderStreamsSection(tournament, containerClass, collapseAll)
    : '';

  //const streamsSec = renderStreamsSection(tournament, containerClass, collapseAll);

  const feedbackSec = renderFeedbackSection(feedbackEntries, containerClass, true);

  // Карта секций
  const sectionsMap = new Map([
    ['news-tournament', tournamentNewsSecHtml],
    ['superfinals', `
      <section class="mb-5">
        <details id="section-superfinals" class="stage-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">👑 Суперфинал 👑</span>
            <a href="#section-superfinals" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
            <span class="qj-badge ms-auto">${superfinals?.length || 0}</span>
          </summary>
          <div class="mt-2">
            ${superCards}
            ${superMapsRatingSec}
            ${superNewsSec}
            ${superRatingSec}
            ${superfinalsDefinedRatingSec}  <!-- новый блок рейтинга суперфинала -->
          </div>
        </details>
      </section>
    `],
    ['finals', `
      <section class="mb-5">
        <details id="section-finals" class="stage-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">🏆 Финальный раунд 🏆</span>
            <a href="#section-finals" class="qj-anchor ms-2 text-secondary text-decoration:none" aria-label="Ссылка на раздел">#</a>
            <span class="qj-badge ms-auto">${finals?.length || 0}</span>
          </summary>
          <div class="mt-2">
            ${finalsCards}
            ${finalsMapsRatingSec}
            ${finalsNewsSec}
            ${finalsRatingSec}
            ${finalsDefinedRatingSec}
          </div>
        </details>
      </section>
    `],
    ['groups', `
      <section class="mb-5">
        <details id="section-groups" class="stage-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">🎯 Квалификации 🎯</span>
            <a href="#section-groups" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
            <span class="qj-badge ms-auto">${groups?.length || 0}</span>
          </summary>
          <div class="mt-2">
            ${groupsCards}
            ${groupsMapsRatingSec}
            ${groupsNewsSec}
            ${groupsRatingSec}
            ${groupsDefinedRatingSec}
          </div>
        </details>
      </section>
    `],
    ['stats', tournamentStatsSec],
    ['custom', customWholeSec],
    ['servers', serversSec],
    ['pack', packSec],
    ['maps-list', mapsListSec],
    ['desc', descSection],
    ['achievements', achievementsAchSec],
    ['perks', perksSec],
    ['streams', streamsSec],
    ['feedback', feedbackSec],
  ]);

  // НОВОЕ: контент модалок
  //const playersModalBody = renderUsersModalBody(users);
  //const teamsModalBody = renderTeamsModalBody(teams);
  //const signupsModalBody = renderSignupsModalBody(registrationSettings, signups);

  // Порядок секций
  const hasNews = !!(tournamentNews?.length);
  const hasSuper = !!(superfinals?.length);
  const hasFinals = !!(finals?.length);
  const hasGroups = !!(groups?.length);
  const hasStats = !!statsBaseNorm;

  const defaultOrder = [];
  if (hasNews) defaultOrder.push('news-tournament');
  if (hasSuper) defaultOrder.push('superfinals');
  if (hasFinals) defaultOrder.push('finals');
  if (hasGroups && !hasFinals) defaultOrder.push('groups');
  if (hasStats) defaultOrder.push('stats');
  if (hasGroups && hasFinals) defaultOrder.push('groups');
  defaultOrder.push('custom', 'servers', 'pack', 'maps-list', 'desc', 'achievements', 'perks', 'streams', 'feedback');

  const seen = new Set();
  const order = [];
  for (const id of sectionOrder || []) {
    if (!sectionsMap.has(id)) continue;
    const html = sectionsMap.get(id);
    if (!html) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    order.push(id);
  }
  for (const id of defaultOrder) {
    if (seen.has(id)) continue;
    const html = sectionsMap.get(id);
    if (!html) continue;
    seen.add(id);
    order.push(id);
  }

  const sectionWrappers = order.map(id => {
    const inner = sectionsMap.get(id);
    if (!inner) return '';
    return `
      <div class="qj-section js-draggable-section" data-section-id="${escapeHtml(id)}">
        ${inner}
      </div>
    `;
  }).join('');

  const hasFeedback = !!(feedbackEntries?.length);

  // Меню (для десктопа — чипы; для мобилок — скрываем чипы и даём компактную кнопку)
  const topMenuHtml = renderTopMenu({
    tournament,
    tournamentNews,
    groups,
    finals,
    superfinals,
    achievementsAch,
    achievementsPerc,
    showStats: Boolean(statsBaseNorm),
    showFeedback: hasFeedback,
    analyticsUrl,   // NEW
  });

  // Стили
  const baseUiCss = `
    html, body { max-width: 100%; }
    body {
      background: #f8f9fa;
      overflow-x: hidden;
      padding-top: var(--qj-mobile-header-offset, 0px);
    }
    header.hero { background: #ffffff; border-bottom: 1px solid rgba(0,0,0,0.06); }
    .hero .title { font-weight: 800; letter-spacing: .2px; }

    body:not(.q2css-active) {
      background-color: #0b0d10 !important;
      background-image: url('${escapeHtml(tournamentBackUrl)}') !important; /* новый фон */
      background-position: center center;
      background-repeat: no-repeat;
      background-size: cover;
      background-attachment: fixed;
    }    


    /* Sticky header: только для десктопа и только в modern-режиме (не Q2CSS) */
    @media (min-width: 768px) {
      body:not(.q2css-active) .hero--sticky {
        position: sticky;
        top: 0;
        z-index: 1050;
        background: rgba(255,255,255,0.9);
        backdrop-filter: saturate(120%) blur(8px);
        -webkit-backdrop-filter: saturate(120%) blur(8px);
        border-bottom: 1px solid rgba(0,0,0,0.06);
        box-shadow: 0 6px 18px rgba(16,24,40,.12);
      }
    }

    /* NEW: возможность откреплять липкую шапку кнопкой-скрепкой (только desktop) */
    @media (min-width: 768px) {
      body:not(.q2css-active) .hero--sticky.is-unpinned {
        position: static !important;
        backdrop-filter: none !important;
        -webkit-backdrop-filter: none !important;
        box-shadow: none !important;
      }
    }
    
    /* NEW: компактная кнопка-скрепка */
    .qj-pin-btn { line-height: 1; }

    .news-meta { white-space: normal; }
    @media (min-width: 768px) { .news-meta { white-space: nowrap; } }

    .hero-logo { max-height: 140px; width: auto; border-radius: 14px; border: 1px solid rgba(0,0,0,0.1); box-shadow: 0 6px 18px rgba(16,24,40,.06); height: auto; }
    @media (max-width: 576px) { .hero-logo { max-height: 90px; } }
    @media (min-width: 1400px) { .hero-logo { max-height: 180px; } }

    .card { border-radius: 12px; }

    .cards-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1.25rem; }
    @media (min-width: 1200px) { .cards-grid { grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); } }
    @media (min-width: 1920px) { .cards-grid { gap: 1.5rem; } }

        /* Селекторы турниров и подтурниров под шапкой */
    .qj-tournament-select-wrapper,
    .qj-subtournament-select-wrapper {
      width: 100%;
      max-width: 360px;
    }

    .qj-subtournament-select-wrapper .form-label {
      font-size: .8rem;
      font-weight: 500;
      color: rgba(71,85,105,.95);
    }

    .qj-subtournament-select-wrapper .form-select,
    .qj-tournament-select-wrapper .form-select {
      font-size: .86rem;
      padding-top: .25rem;
      padding-bottom: .25rem;
    }

    /* Для стадий (квалификации/финалы/суперфинал): не больше двух карточек в ряд на десктопе */
    .cards-grid--stage {
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }

    @media (min-width: 992px) {
      .cards-grid--stage {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    /* Суперфинал: одна карточка в ряд на десктопе */
    .cards-grid--stage.cards-grid--super {
        grid-template-columns: 1fr;
      }
    @media (min-width: 992px) {
      .cards-grid--stage.cards-grid--super {
        grid-template-columns: 1fr;
      }
    }

    .cards-grid.cards-grid--ach { grid-template-columns: 1fr; }
    @media (min-width: 768px) { .cards-grid.cards-grid--ach { grid-template-columns: repeat(2, minmax(0, 1fr)); } }

        /* Сортируемые таблицы в секции "Подробнее" */
    .js-sortable-table th {
      cursor: pointer;
      user-select: none;
      position: relative;
      white-space: nowrap;
    }

    .js-sortable-table th::after {
      content: '';
      margin-left: .25rem;
      font-size: .7em;
      opacity: .4;
    }

    .js-sortable-table th[data-sort-dir="asc"]::after {
      content: '▲';
      opacity: .8;
    }

    .js-sortable-table th[data-sort-dir="desc"]::after {
      content: '▼';
      opacity: .8;
    }

    .maps { border-top: 1px dashed rgba(0,0,0,0.06); padding-top: .5rem; }
    .demos { border-top: 1px dashed rgba(0,0,0,0.06); padding-top: .5rem; }

    .rating-table th, .rating-table td { vertical-align: middle; }
    .rating-table .pos { width: 64px; }
    .rating-table .pts { width: 120px; text-align: right; }

    .qj-shot-btn { display: inline-block; border: 0; background: transparent; padding: 0; margin: 0; cursor: zoom-in; line-height: 0; }

    .lightbox { position: fixed; inset: 0; display: grid; place-items: center; background: rgba(0,0,0,0.5); opacity: 0; pointer-events: none; transition: opacity .2s ease-out; z-index: 9999; }
    .lightbox.is-open { opacity: 1; pointer-events: auto; }
    .lightbox-img { max-width: 92vw; max-height: 92vh; box-shadow: 0 20px 60px rgba(0,0,0,0.4); border-radius: 12px; transform: scale(.97); transition: transform .2s ease-out, opacity .2s ease-out; opacity: 0; cursor: zoom-out; }
    .lightbox.is-open .lightbox-img { transform: scale(1); opacity: 1; }
    .lightbox-backdrop { position: absolute; inset: 0; }
    .no-scroll { overflow: hidden !important; }

    .player-modal { position: fixed; inset: 0; z-index: 1060; display: none; }
    .player-modal.is-open { display: block; }

    .player-modal-backdrop {
      position: absolute;
      inset: 0;
      /* полупрозрачный тёмный фон, через который видно сайт */
      background:
        radial-gradient(circle at top, rgba(30,64,175,.35), transparent 55%),
        rgba(15,23,42,.55);
      backdrop-filter: blur(4px);
    }

    .player-modal-dialog {
      position: relative;
      margin: 6vh auto;
      max-width: min(960px, calc(100vw - 24px));
      max-height: 88vh;
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 18px 60px rgba(15,23,42,.25);
      background: linear-gradient(180deg, #ffffff, #f7f9fc);
      border: 1px solid rgba(15,23,42,.08);
      color: #111827;
    }

    .player-modal-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: .85rem 1.1rem;
      border-bottom: 1px solid rgba(15,23,42,.06);
      background: linear-gradient(90deg, #ffffff, #e5edff);
      color: #111827;
      position: sticky;
      top: 0;
      z-index: 1;
    }

    .player-modal-title {
      font-weight: 600;
      font-size: .98rem;
      letter-spacing: .03em;
      text-transform: uppercase;
    }

    .player-modal-body {
      height: calc(88vh - 54px);
      overflow: auto;
      padding: .75rem 1rem 1rem;
      background: linear-gradient(180deg, #f8fafc, #eef2ff);
    }

    .player-modal-body iframe {
      display: block;
      width: 100%;
      height: 100%;
      border: 0;
      border-radius: 12px;
      background: #020617;
    }


    /* Velvet-стиль для модалок "Игроки / Команды / Заявки" — светлая тема с чёткой сеткой */

    #playersModal .player-modal-title,
    #teamsModal .player-modal-title,
    #signupsModal .player-modal-title,
    #demosModal .player-modal-title {
      font-size: 1rem;
    }

    #playersModal .qj-modal-meta,
    #teamsModal .qj-modal-meta,
    #signupsModal .qj-modal-meta,
    #demosModal .qj-modal-meta {
      color: rgba(71,85,105,.9);
    }

    #playersModal .qj-modal-shell,
    #teamsModal .qj-modal-shell,
    #signupsModal .qj-modal-shell,
    #demosModal .qj-modal-shell {
      background: #ffffff;
      border-radius: 16px;
      border: 1px solid rgba(148,163,184,.5);
      box-shadow: 0 16px 40px rgba(15,23,42,.12);
      padding: .5rem;
    }

    #playersModal .qj-modal-table,
    #teamsModal .qj-modal-table,
    #signupsModal .qj-modal-table,
    #demosModal .qj-modal-table {
      margin-bottom: 0;
      border-collapse: separate;
      border-spacing: 0;
    }

    #playersModal .qj-modal-table thead tr,
    #teamsModal .qj-modal-table thead tr,
    #signupsModal .qj-modal-table thead tr,
    #demosModal .qj-modal-table thead tr {
      background: linear-gradient(90deg, #e3edff, #d7e3ff);
      color: #111827;
    }

    #playersModal .qj-modal-table thead th,
    #teamsModal .qj-modal-table thead th,
    #signupsModal .qj-modal-table thead th,
    #demosModal .qj-modal-table thead th {
      border-bottom: 2px solid rgba(148,163,184,.8);
      font-weight: 600;
    }

    #playersModal .qj-modal-table tbody tr,
    #teamsModal .qj-modal-table tbody tr,
    #signupsModal .qj-modal-table tbody tr,
    #demosModal .qj-modal-table tbody tr {
      background: #ffffff;
      border-bottom: 1px solid rgba(226,232,240,1);
      transition: background .16s ease-out, transform .16s ease-out, box-shadow .16s ease-out;
    }

    #playersModal .qj-modal-table tbody tr:nth-child(2n),
    #teamsModal .qj-modal-table tbody tr:nth-child(2n),
    #signupsModal .qj-modal-table tbody tr:nth-child(2n),
    #demosModal .qj-modal-table tbody tr:nth-child(2n) {
      background: #f8fafc;
    }

    #playersModal .qj-modal-table tbody tr:hover,
    #teamsModal .qj-modal-table tbody tr:hover,
    #signupsModal .qj-modal-table tbody tr:hover,
    #demosModal .qj-modal-table tbody tr:hover {
      background: #edf2ff;
      box-shadow: 0 6px 18px rgba(15,23,42,.12);
      transform: translateY(-1px);
    }

    #playersModal .qj-modal-table th,
    #playersModal .qj-modal-table td,
    #teamsModal .qj-modal-table th,
    #teamsModal .qj-modal-table td,
    #signupsModal .qj-modal-table th,
    #signupsModal .qj-modal-table td
    #demosModal .qj-modal-table th,
    #demosModal .qj-modal-table td {
      border-color: transparent;
      vertical-align: middle;
      font-size: .8rem;
    }


    /* Специальный светлый стиль для модалки статистики игрока */
    #playerModal .player-modal-dialog {
      background: #ffffff;
      border-color: rgba(0,0,0,.08);
      color: #111827;
    }

    #playerModal .player-modal-header {
      background: #ffffff;
      border-bottom: 1px solid rgba(0,0,0,.08);
      color: #111827;
    }

    #playerModal .player-modal-body {
      background: #ffffff;
    }

    #playerModal .player-modal-body iframe {
      background: transparent;
    }

    .qj-accent { color: var(--bs-primary); }
    .qj-muted { color: var(--bs-secondary); }
    .qj-pts { color: var(--bs-danger-text-emphasis); }
    .qj-badge { display: inline-block; padding: .35em .6em; font-size: .75rem;
      background-color: var(--bs-secondary-bg-subtle); color: var(--bs-secondary-text-emphasis); border-radius: .375rem; }
    .qj-tag { display: inline-block; padding: .25rem .5rem; background-color: var(--bs-secondary-bg-subtle);
      color: var(--bs-secondary-text-emphasis); border-radius: 10rem; font-size: .8rem; border: 1px solid rgba(0,0,0,.05); }

    /* Меню (верхние ссылки-чипы) */
    .qj-menu { width: 100%; }
    .qj-menu-scroll { display: flex; flex-wrap: wrap; gap: .35rem; overflow-x: auto; padding-bottom: .125rem; }
    .qj-menu-scroll::-webkit-scrollbar { height: 6px; }
    .qj-menu-scroll::-webkit-scrollbar-thumb { background: rgba(0,0,0,.15); border-radius: 999px; }
    .qj-chip {
      display: inline-block;
      padding: .38rem .8rem;
      border-radius: 9999px;
      text-decoration: none;
      font-weight: 600;
      font-size: .9rem;
      line-height: 1;
      background: linear-gradient(180deg, #eef4ff, #e6efff);
      color: #2b4c7e;
      border: 1px solid rgba(60,100,170,.25);
      box-shadow: 0 2px 6px rgba(16,24,40,.05);
      transition: transform .1s ease, background .15s ease, box-shadow .15s ease;
      white-space: nowrap;
    }
    .qj-chip:hover { background: linear-gradient(180deg, #e9f1ff, #e0ebff); transform: translateY(-1px); box-shadow: 0 4px 12px rgba(16,24,40,.08); }

    /* Заголовки секций + DnD хват */
    details > summary.qj-toggle {
      display: flex; align-items: center; gap: .5rem;
      padding: .7rem .95rem;
      border: 1px solid rgba(0,0,0,0.06);
      background: linear-gradient(180deg, #ffffff, #f7f9fb);
      border-radius: 14px;
      box-shadow: 0 2px 8px rgba(16,24,40,.04);
      cursor: default; user-select: none;
      transition: background .2s ease, box-shadow .2s ease, border-color .2s ease, transform .1s ease;
      touch-action: pan-y; /* важно: не блокируем вертикальную прокрутку пальцем */
    }
    body.dnd-enabled details > summary.qj-toggle { cursor: grab; }
    body.dnd-enabled details > summary.qj-toggle:active { cursor: grabbing; }

    details > summary.qj-toggle:hover {
      background: linear-gradient(180deg, #ffffff, #f4f7fa);
      box-shadow: 0 6px 18px rgba(16,24,40,.08);
      border-color: rgba(0,0,0,.1);
      transform: translateY(-1px);
    }
    details > summary.qj-toggle .section-title { font-weight: 700; color: var(--bs-emphasis-color); margin: 0; letter-spacing: .2px; }
    details > summary.qj-toggle::after { content: '›'; margin-left: .5rem; transform: rotate(0deg);
      transition: transform .2s ease; color: var(--bs-secondary); font-size: 1.05rem; }
    details[open] > summary.qj-toggle::after { transform: rotate(90deg); }
    details > summary.qj-toggle::-webkit-details-marker { display: none; }

    .qj-anchor { opacity: .7; transition: opacity .15s ease; color: inherit; }
    summary.qj-toggle:hover .qj-anchor { opacity: 1; }
    .news-collapse summary, .stage-collapse summary, .sub-collapse summary { margin-bottom: .25rem; }

    /* Мини‑иконки */
    .ach-badges { display: inline-flex; align-items: center; gap: .25rem; }
    .ach-badge-img { width: 55px; height: 55px; object-fit: contain; border-radius: 6px; border: 1px solid rgba(0,0,0,.15); vertical-align: middle; }
    .perc-badge-img { width: 55px; height: 55px; object-fit: contain; border-radius: 50%; border: 1px solid rgba(0,0,0,.15); vertical-align: middle; }

    .ach-thumb { width: 200px; height: 200px; object-fit: contain; border-radius: 6px; border: 1px solid rgba(0,0,0,.1); box-shadow: 0 6px 18px rgba(16,24,40,.06); }

    /* Превью поверх всего — чтобы hover-увеличение не обрезалось таблицами */
    .ach-preview {
      position: fixed;
      z-index: 200000;
      pointer-events: none;
      box-shadow: 0 12px 36px rgba(0,0,0,.35);
    }
    .ach-preview img {
      display: block;
      width: var(--ach-preview-w, 220px);
      height: auto;
      border-radius: var(--ach-preview-br, 0); /* NEW: копируем скругление источника */
    }

    .players { margin: 0; padding: 0; }
    .players li { display: flex; align-items: center; gap: .5rem; padding: .35rem 0; margin: 0; line-height: 1.25; }
    .players li + li { border-top: 1px solid rgba(0,0,0,.08); }
    .player-pos { display: inline-block; min-width: 1.75rem; text-align: right; font-variant-numeric: tabular-nums; color: var(--bs-secondary-color); }
    .player-link { text-decoration: none; }
    .player-link:hover { text-decoration: underline; }
    .player-meta { display: inline-flex; align-items: center; gap: .35rem; }

    .player-pts {
      display: inline-flex; align-items: center; justify-content: center;
      min-width: 1.6rem; height: 1.35rem; padding: 0 .5rem;
      border-radius: .75rem;
      background: linear-gradient(135deg, rgba(255,99,132,.08), rgba(255,159,64,.08));
      color: #b42318;
      border: 1px solid rgba(244,63,94,.2);
      font-weight: 800;
      font-variant-numeric: tabular-nums;
      line-height: 1;
      vertical-align: middle;
      box-shadow: inset 0 1px 0 rgba(255,255,255,.5);
    }

        /* Подсветка топ-3 игроков в суперфинале (анимированные ореолы вокруг ника) */
    .player-name.qj-halo {
      position: relative;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 0 .35rem;
      border-radius: 999px;
      isolation: isolate;
    }

    .qj-halo::before {
      content: '';
      position: absolute;
      inset: -0.25rem;
      border-radius: inherit;
      opacity: .9;
      filter: blur(.3px);
      z-index: -1;
      animation: qj-halo-pulse 1.8s ease-in-out infinite alternate;
    }

    .qj-halo-gold::before {
      background: radial-gradient(circle at 50% 0%, rgba(255,215,0,.95), rgba(255,215,0,.28) 40%, transparent 70%);
      box-shadow: 0 0 10px rgba(255,215,0,.7);
    }

    .qj-halo-silver::before {
      background: radial-gradient(circle at 50% 0%, rgba(192,192,192,.95), rgba(192,192,192,.28) 40%, transparent 70%);
      box-shadow: 0 0 9px rgba(192,192,192,.7);
    }

    .qj-halo-bronze::before {
      background: radial-gradient(circle at 50% 0%, rgba(205,127,50,.95), rgba(205,127,50,.28) 40%, transparent 70%);
      box-shadow: 0 0 9px rgba(205,127,50,.7);
    }

    @keyframes qj-halo-pulse {
      0%   { transform: scale(1); opacity: .7; }
      100% { transform: scale(1.06); opacity: 1; }
    }


    .qj-section { margin-bottom: 2rem; }
    .qj-sections-root .qj-section.dragging { opacity: .6; }

    /* Отступ якорям под липкую шапку */
    [id^="section-"], [id^="news-"], [id^="group-"], [id^="final-"], [id^="super-"], [id^="custom-"], [id^="ach-"] {
      scroll-margin-top: var(--qj-sticky-offset, 0px);
    }

    /* FIX: выравнивание кнопок в qj-controls (desktop + mobile) */
    .qj-controls {
      align-items: center !important;
    }
    .qj-controls > * {
      height: auto !important;
      line-height: 1 !important;
    }

    /* Мобильные улучшения */
    @media (max-width: 767.98px) {
      html, body { overflow-x: hidden; }
      .qj-controls { flex-wrap: wrap; gap: .5rem; }
      .qj-controls > * { flex: 0 0 auto; }
      .qj-menu { display: none; } /* скрываем чипы меню на мобильном */
      .hero-logo { max-width: 28vw; height: auto; }
      .news-text { overflow-wrap: anywhere; word-break: break-word; }
    }

    /* Мобильная версия: в модалках Игроки/Команды прячем даты создания/обновления */
    @media (max-width: 767.98px) {
      #playersModal .qj-col-created,
      #playersModal .qj-col-updated,
      #teamsModal .qj-col-created,
      #teamsModal .qj-col-updated {
        display: none;
      }
    }

    /* Авто-появление/скрытие шапки на мобильных при скролле */
    @media (max-width: 767.98px) {
      body:not(.q2css-active) header.hero.hero--sticky {
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        z-index: 1050;
        transform: translateY(0);
        transition: transform .2s ease-in-out;
      }
      body:not(.q2css-active) header.hero.hero--sticky.hero--hidden-mobile {
        transform: translateY(-100%);
      }
    }

    /* Кнопка мобильного меню всегда видима в мобильной шапке */
    .mobile-menu-trigger { display: inline-flex; align-items: center; justify-content: center; padding: .25rem .6rem; min-height: 30px; }

    .qj-badge-circle {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 1.9rem;
      height: 1.9rem;
      border-radius: 999px;
      font-size: 0.95rem;
      font-weight: 700;
      text-decoration: none !important;
      line-height: 1;
      padding: 0;
      margin: 0;
      box-sizing: border-box;
    }

    .qj-badge-analytics {
      color: #ffffff;
      background-color: #0d6efd;
      border: 2px solid #0b5ed7;
      box-shadow: 0 0 0 1px rgba(13, 110, 253, 0.2);
    }

    .qj-badge-stats {
      color: #ffffff;
      background-color: #198754;
      border: 2px solid #157347;
      box-shadow: 0 0 0 1px rgba(25, 135, 84, 0.2);
    }

    .qj-badge-circle:hover {
      filter: brightness(1.08);
      text-decoration: none !important;
    }

    /* Мобильное меню (оверлей) */
    .qj-mm { position: fixed; inset: 0; z-index: 1080; display: none; }
    .qj-mm.is-open { display: block; }
    .qj-mm-backdrop { position: absolute; inset: 0; background: rgba(0,0,0,.5); }
    .qj-mm-panel { position: absolute; left: 0; right: 0; top: 0; background: #fff; border-radius: 0 0 12px 12px; box-shadow: 0 12px 24px rgba(0,0,0,.25); transform: translateY(-100%); transition: transform .2s ease-out; }
    .qj-mm.is-open .qj-mm-panel { transform: translateY(0); }
    .qj-mm-header { display: flex; align-items: center; justify-content: space-between; padding: .75rem 1rem; border-bottom: 1px solid rgba(0,0,0,.08); }
    .qj-mm-title { font-weight: 600; }
    .qj-mm-body { max-height: 70vh; overflow: auto; padding: .5rem; }
    .qj-mm-body a { display: block; padding: .6rem .75rem; border-radius: .5rem; text-decoration: none; color: var(--bs-emphasis-color); }
    .qj-mm-body a:hover { background: rgba(0,0,0,.04); }
  `;

  const modernUiCss = !useQ2Css ? `
    :root {
      --tone-tournament: linear-gradient(180deg, #f6faff 0%, #eef5ff 100%);
      --tone-tournament-edge: rgba(37,99,235,.18);
      --tone-qual: linear-gradient(180deg, #f5fff7 0%, #ecfbf0 100%);
      --tone-qual-edge: rgba(22,163,74,.18);
      --tone-final: linear-gradient(180deg, #fbf7ff 0%, #f6effe 100%);
      --tone-final-edge: rgba(124,58,237,.18);
      --tone-super: linear-gradient(180deg, #fff7f5 0%, #ffefeb 100%);
      --tone-super-edge: rgba(239,68,68,.18);
      --tone-custom: linear-gradient(180deg, #f5fffd 0%, #ebfcf7 100%);
      --tone-custom-edge: rgba(14,165,233,.18);
      --tone-ach: linear-gradient(180deg, #fffaf2 0%, #fff3e0 100%);
      --tone-ach-edge: rgba(245,158,11,.18);
      --tone-perk: linear-gradient(180deg, #f6f8ff 0%, #eef3ff 100%);
      --tone-perk-edge: rgba(79,70,229,.18);
    }

    body:not(.q2css-active) .card {
      border-radius: 16px;
      background: rgba(255,255,255,0.95);
      border: 1px solid rgba(16,24,40,0.05);
      backdrop-filter: saturate(120%) blur(6px);
      box-shadow: 0 10px 24px rgba(16,24,40,.08);
      transition: transform .15s ease, box-shadow .2s ease, border-color .2s ease;
    }
    body:not(.q2css-active) .card:hover {
      transform: translateY(-1px);
      box-shadow: 0 14px 34px rgba(16,24,40,.12);
      border-color: rgba(16,24,40,0.08);
    }

    body:not(.q2css-active) #section-desc > summary.qj-toggle,
    body:not(.q2css-active) #section-servers > summary.qj-toggle,
    body:not(.q2css-active) #section-pack > summary.qj-toggle,
    body:not(.q2css-active) #section-maps-list > summary.qj-toggle,
    body:not(.q2css-active) #section-news-tournament > summary.qj-toggle {
      background: var(--tone-tournament);
      border-left: 4px solid var(--tone-tournament-edge);
    }
    body:not(.q2css-active) #section-groups > summary.qj-toggle,
    body:not(.q2css-active) #section-news-groups > summary.qj-toggle {
      background: var(--tone-qual);
      border-left: 4px solid var(--tone-qual-edge);
    }
    body:not(.q2css-active) #section-finals > summary.qj-toggle,
    body:not(.q2css-active) #section-news-finals > summary.qj-toggle {
      background: var(--tone-final);
      border-left: 4px solid var(--tone-final-edge);
    }
    body:not(.q2css-active) #section-superfinals > summary.qj-toggle,
    body:not(.q2css-active) #section-news-super > summary.qj-toggle {
      background: var(--tone-super);
      border-left: 4px solid var(--tone-super-edge);
    }
    body:not(.q2css-active) #section-custom > summary.qj-toggle {
      background: var(--tone-custom);
      border-left: 4px solid var(--tone-custom-edge);
    }
    body:not(.q2css-active) #section-achievements > summary.qj-toggle {
      background: var(--tone-ach);
      border-left: 4px solid var(--tone-ach-edge);
    }
    body:not(.q2css-active) #section-perks > summary.qj-toggle {
      background: var(--tone-perk);
      border-left: 4px solid var(--tone-perk-edge);
    }

    body:not(.q2css-active) { background: linear-gradient(180deg, #f7f9fc, #f3f6fb); }
    body:not(.q2css-active) header.hero { background: transparent; border-bottom: 0; }
  ` : '';

  const q2OverridesCss = `
    body.q2css-active details > summary.qj-toggle {
      border-radius: 0 !important; background: #FAD3BC !important; border: 1px solid #000 !important;
      box-shadow: none !important; padding: 6px 8px !important; cursor: default;
    }
    body.q2css-active.dnd-enabled details > summary.qj-toggle { cursor: grab; }
    body.q2css-active.dnd-enabled details > summary.qj-toggle:active { cursor: grabbing; }

    body.q2css-active details > summary.qj-toggle .section-title {
      color: #000 !important; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 12px;
    }
    body.q2css-active details > summary.qj-toggle::after { color: #3D3D3D !important; }
    body.q2css-active .qj-badge { background: #FEF1DE; color: #000; border: 1px solid #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
    body.q2css-active .qj-tag { background: #FEF1DE; color: #A22C21; border: 1px solid #000; border-radius: 0; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }

    /* Меню в Q2CSS стиле */
    body.q2css-active .qj-menu-scroll { gap: .35rem; }
    body.q2css-active .qj-chip {
      background: #FEF1DE !important;
      color: #A22C21 !important;
      border: 1px solid #000 !important;
      border-radius: 0 !important;
      font-family: Verdana, Geneva, Arial, Helvetica, sans-serif !important;
      font-size: 11px !important;
      padding: 2px 6px !important;
      box-shadow: none !important;
    }

    body.q2css-active { background-color: #FEF1DE !important; }
    body.q2css-active header.hero { background-color: #FEF1DE !important; border-bottom: 0 !important; }
    html.q2css-active, body.q2css-active { overflow-x: hidden !important; }
    body.q2css-active .container, body.q2css-active .container-fluid { max-width: 100% !important; padding-left: 0 !important; padding-right: 0 !important; }
    body.q2css-active main { padding-left: 8px !important; padding-right: 8px !important; }
    body.q2css-active .row { margin-left: 0 !important; margin-right: 0 !important; }
    body.q2css-active .row > [class^="col-"] { padding-left: 8px; padding-right: 8px; }

    body.q2css-active .qj-accent { color: #A22C21 !important; }
    body.q2css-active .qj-muted { color: #3D3D3D !important; }
    body.q2css-active .qj-pts { color: #A22C21 !important; }

    body.q2css-active .card { border-radius: 0 !important; border: 1px solid #000 !important; background: #FEF1DE !important; box-shadow: none !important; }
    body.q2css-active .card-title { background: #A22C21; color: #fff !important; margin: -12px -12px 8px -12px; padding: 6px 10px; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 12px; }
    body.q2css-active .maps, body.q2css-active .demos { border-top: 1px solid #000 !important; }
    body.q2css-active .players li { font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }
    body.q2css-active .players li + li { border-top: 1px solid #000 !important; }
    body.q2css-active .player-pos { color: #000 !important; }
    body.q2css-active .list-group-item { border-color: #000; background: #FEF1DE; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }

    body.q2css-active .rating-table.qj-table,
    body.q2css-active .maps-rating-table.qj-table {
      background: #FEF1DE; border: 1px solid #000; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px;
    }
    body.q2css-active .rating-table.qj-table thead tr,
    body.q2css-active .maps-rating-table.qj-table thead tr { background: #FAD3BC; }
    body.q2css-active .rating-table.qj-table th,
    body.q2css-active .rating-table.qj-table td,
    body.q2css-active .maps-rating-table.qj-table th,
    body.q2css-active .maps-rating-table.qj-table td {
      border-bottom: 1px solid #000; color: #000 !important;
    }

    body.q2css-active .qj-quote { font-style: italic; font-size: .95em; background: #FEECD3; border-left: 4px solid #A22C21; border-radius: 0; color: #000; }

    body.q2css-active .ach-badge-img { border: 1px solid #000; border-radius: 0; width: 55px; height: 55px; object-fit: contain; }
    body.q2css-active .perc-badge-img { border: 1px solid #000; border-radius: 50%; width: 55px; height: 55px; object-fit: contain; }
    body.q2css-active .ach-thumb { border: 1px solid #000; border-radius: 0; width: 200px; height: 200px; }
  `;

  const animatedBgCss = !useQ2Css ? `
    body:not(.q2css-active) {
      background-color: #0b0d10 !important;
      background-image: url('${escapeHtml(tournamentBackUrl)}') !important;
      background-position: center center;
      background-repeat: no-repeat;
      background-size: cover;
      background-attachment: fixed;
    }
    @media (pointer: coarse) {
      body:not(.q2css-active) { background-attachment: scroll; }
    }
    body:not(.q2css-active) header.hero { background: transparent !important; border-bottom: 0 !important; }
    @media (prefers-reduced-motion: reduce) {
      body:not(.q2css-active) { background-image: none !important; background-color: #f5f7fa !important; }
    }
  ` : '';

  const q2BtnClass = useQ2Css ? 'btn btn-sm btn-primary' : 'btn btn-sm btn-outline-primary';
  const collBtnClass = collapseAll ? 'btn btn-sm btn-primary' : 'btn btn-sm btn-outline-primary';
  const resetBtnClass = 'btn btn-sm btn-outline-secondary';

  return `<!doctype html>
<html lang="ру" data-bs-theme="auto" class="${useQ2Css ? 'q2css-active' : ''}">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>${escapeHtml(tournament.name || 'Турнир')}</title>
  ${faviconLink}
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    ${baseUiCss}
    ${modernUiCss}
    ${animatedBgCss}
  </style>
  ${useQ2Css ? `<style id="quake2-com-ru-css">${QUAKE2_COM_RU_CSS}</style>` : ''}
  ${useQ2Css ? `<style id="q2-overrides-css">${q2OverridesCss}</style>` : ''}
</head>
<body class="${useQ2Css ? 'q2css-active' : ''}" data-chat-id="${selectedChatId != null ? String(selectedChatId) : ''}">
  <header class="hero py-3 ${useQ2Css ? 'head_image' : 'hero--sticky'}">
    <div class="${containerClass}">
      <!-- Мобильная шапка: логотип + название + ссылки + селектор турнира + кнопки -->
      <div class="d-flex d-md-none flex-column">
        <div class="d-flex align-items-start">
          ${logoBlock}
          <div class="ms-3 flex-grow-1">
              <h1 class="title h5 my-0">${escapeHtml(tournament.name || 'Турнир')}</h1>
              <div class="site-link mt-1">
                ${siteLink ? `${siteLink}` : ''} ${siteWiki ? `${siteWiki} ` : ''} 
                ${newsChannelLink ? `<span class="me-1">${newsChannelLink} </span>` : ''}
                ${analyticsBadgeMobile ? `${analyticsBadgeMobile} ` : ''} 
                ${statsBadgeMobile ? `${statsBadgeMobile}` : ''} 
              </div>  
          </div>
        </div>
        ${tournamentSelectHtml
      ? `<div class="mt-2 w-100">${tournamentSelectHtml}</div>`
      : ''}

       ${subTournamentSelectHtml
      ? `<div class="mt-2">${subTournamentSelectHtml}</div>`
      : ''
    }

        <!-- Кнопки: Меню + Q2CSS + Свернуть все -->
        <div class="qj-controls mt-2">
          <div class="d-flex justify-content-start gap-2">
            <button type="button" class="mobile-menu-trigger btn btn-sm btn-secondary" title="Меню">≡ Меню</button>
            <button type="button" class="js-btn-toggle-q2 ${q2BtnClass}" title="Переключить Q2CSS">Q2CSS</button>
            <button type="button" class="js-btn-toggle-collapse ${collBtnClass}" title="Свернуть/раскрыть все">🔽 Свернуть все</button>
          </div>
        </div>

        <!-- Чипы меню (скрыты на мобиле визуально, но используются для заполнения мобильного меню) -->
        ${topMenuHtml || ''}
      </div>


      <!-- Десктопная шапка (липкая) -->
      <div class="d-none d-md-flex align-items-start">
        ${logoBlock}
        <div class="flex-grow-1">
          <div class="d-flex justify-content-end gap-2 mb-2 qj-controls">
            ${tournamentSelectHtml}
            <button type="button" class="js-btn-toggle-q2 ${q2BtnClass}" title="Переключить Q2CSS">Q2CSS</button>
            <button type="button" class="js-btn-toggle-collapse ${collBtnClass}" title="Свернуть/раскрыть все">🔽 Свернуть все</button>
            <button type="button" class="js-btn-reset-sections ${resetBtnClass}" title="Вернуть порядок разделов по умолчанию">↩️ Вернуть порядок</button>
            <button type="button" class="js-btn-toggle-dnd btn btn-sm btn-outline-warning" title="Включить/выключить редактирование разделов">✏️ Редактировать разделы</button>
          </div>
          <div class="d-flex flex-column align-items-start">
            <h1 class="title h3 my-0">${escapeHtml(tournament.name || 'Турнир')}</h1>
            <div class="site-link mt-1">
                ${siteLink ? `${siteLink}` : ''} ${siteWiki ? `${siteWiki} ` : ''} 
                ${newsChannelLink ? `<span class="me-1">${newsChannelLink} </span>` : ''}
            </div> 

            ${subTournamentSelectHtml
      ? `<div class="mt-2 w-100" style="max-width:360px;">${subTournamentSelectHtml}</div>`
      : ''
    }

            ${topMenuHtml || ''}
          </div>
        </div>
      </div>
    </div>
  </header>

  <main class="${containerClass} py-4">
    <div id="sections-root" class="qj-sections-root">
      ${sectionWrappers}
    </div>
  </main>

  <div id="lightbox" class="lightbox" aria-hidden="true" role="dialog" aria-label="Просмотр скриншота">
    <div class="lightbox-backdrop"></div>
    <img class="lightbox-img" alt="screenshot" />
  </div>

  <div id="playerModal" class="player-modal" aria-hidden="true" role="dialog" aria-label="Статистика игрока">
    <div class="player-modal-backdrop"></div>
    <div class="player-modal-dialog" role="document" aria-modal="true">
      <div class="player-modal-header">
        <div class="player-modal-title">Статистика: <span id="playerModalName"></span></div>
        <button type="button" class="btn-close" aria-label="Закрыть" id="playerModalClose"></button>
      </div>
      <div class="player-modal-body">
        <iframe id="playerModalFrame" src="about:blank" loading="lazy" title="Player stats preview"></iframe>
      </div>
    </div>
  </div>

  <!-- Модалка "Игроки" -->
  <div id="playersModal" class="player-modal" aria-hidden="true" role="dialog" aria-label="Список игроков">
    <div class="player-modal-backdrop"></div>
    <div class="player-modal-dialog" role="document" aria-modal="true">
      <div class="player-modal-header">
        <div class="player-modal-title">Игроки</div>
        <button type="button" class="btn-close js-close-modal" data-target="playersModal" aria-label="Закрыть"></button>
      </div>
      <div class="player-modal-body">
        <div class="small text-muted">Загрузка...</div>
      </div>
    </div>
  </div>

  <!-- Модалка "Команды" -->
  <div id="teamsModal" class="player-modal" aria-hidden="true" role="dialog" aria-label="Список команд">
    <div class="player-modal-backdrop"></div>
    <div class="player-modal-dialog" role="document" aria-modal="true">
      <div class="player-modal-header">
        <div class="player-modal-title">Команды</div>
        <button type="button" class="btn-close js-close-modal" data-target="teamsModal" aria-label="Закрыть"></button>
      </div>
      <div class="player-modal-body">
        <div class="small text-muted">Загрузка...</div>
      </div>
    </div>
  </div>

  <!-- Модалка "Заявки" -->
  <div id="signupsModal" class="player-modal" aria-hidden="true" role="dialog" aria-label="Заявки на турнир">
    <div class="player-modal-backdrop"></div>
    <div class="player-modal-dialog" role="document" aria-modal="true">
      <div class="player-modal-header">
        <div class="player-modal-title">Заявки</div>
        <button type="button" class="btn-close js-close-modal" data-target="signupsModal" aria-label="Закрыть"></button>
      </div>
      <div class="player-modal-body">
        <div class="small text-muted">Загрузка...</div>
      </div>
    </div>
  </div>

  <!-- Модалка "Демки" -->
  <div id="demosModal" class="player-modal" aria-hidden="true" role="dialog" aria-label="Демки">
    <div class="player-modal-backdrop"></div>
    <div class="player-modal-dialog" role="document" aria-modal="true">
      <div class="player-modal-header">
        <div class="player-modal-title">Демки</div>
        <button type="button" class="btn-close js-close-modal" data-target="demosModal" aria-label="Закрыть"></button>
      </div>
      <div class="player-modal-body">
        <div class="small text-muted">Загрузка...</div>
      </div>
    </div>
  </div>

  <!-- Мобильное меню -->
  <div id="mobileMenu" class="qj-mm" aria-hidden="true">
    <div class="qj-mm-backdrop"></div>
    <div class="qj-mm-panel">
      <div class="qj-mm-header">
        <div class="qj-mm-title">Меню</div>
        <button type="button" class="btn-close qj-mm-close" aria-label="Закрыть"></button>
      </div>
      <div class="qj-mm-body">
        <nav id="mobileMenuList"></nav>
      </div>
    </div>
  </div>

  <footer class="py-4">
    <div class="${containerClass} text-center text-muted small">
      Работает на QuakeJourney Tournament Bot — ${new Date().getFullYear()}
      <br>
      Developed by ly
      <br>
      https://github.com/Quake-Journey/Tournament
    </div>
  </footer>

  <script>
    (function(){

      // Анти-скролл на первом заходе
      (function initialAntiAnchoring(){
        const style = document.createElement('style');
        style.id = 'qj-anti-anch';
        style.textContent = 'html,body{overflow-anchor:none !important;}';
        document.head.appendChild(style);

        let firstNavigate = !location.hash;
        try {
          const nav = (performance.getEntriesByType && performance.getEntriesByType('navigation') || [])[0];
          if (nav) firstNavigate = (!location.hash && nav.type === 'navigate');
        } catch(_) {}

        if (firstNavigate) {
          try { history.scrollRestoration = 'manual'; } catch(_) {}
          window.scrollTo(0, 0);
          requestAnimationFrame(() => window.scrollTo(0, 0));
        }

        window.addEventListener('load', () => {
          setTimeout(() => {
            const el = document.getElementById('qj-anti-anch');
            if (el) el.remove();
            try { history.scrollRestoration = 'auto'; } catch(_) {}
          }, 800);
        });
      })();

      // --- СОРТИРОВКА ТАБЛИЦ В "ПОДРОБНЕЕ" (group/final/superfinal) ---
      function initSortableTables() {
        const tables = document.querySelectorAll('table.js-sortable-table');
        if (!tables.length) return;

        tables.forEach(table => {
          const thead = table.tHead;
          if (!thead) return;

          const headers = Array.from(thead.querySelectorAll('th'));
          const tbody = table.tBodies[0];
          if (!tbody) return;

          headers.forEach((th, colIndex) => {
            const sortType = th.getAttribute('data-sort-type');
            // если тип сортировки не задан или явно "none" — не делаем этот столбец кликабельным
            if (!sortType || sortType === 'none') return;

            th.addEventListener('click', function () {
              const type = sortType; // "string" | "number"

              const currentDir = th.getAttribute('data-sort-dir');
              const nextDir = currentDir === 'asc' ? 'desc' : 'asc';

              headers.forEach(h => h.removeAttribute('data-sort-dir'));
              th.setAttribute('data-sort-dir', nextDir);

              const rows = Array.from(tbody.rows);

              rows.sort((rowA, rowB) => {
                const cellA = rowA.cells[colIndex];
                const cellB = rowB.cells[colIndex];
                const aText = (cellA ? cellA.textContent : '').trim();
                const bText = (cellB ? cellB.textContent : '').trim();

                let cmp = 0;

                if (type === 'number') {
                  const a = parseFloat(aText.replace(',', '.')) || 0;
                  const b = parseFloat(bText.replace(',', '.')) || 0;
                  cmp = a - b;
                } else {
                  cmp = aText.localeCompare(bText, 'ru', { sensitivity: 'base' });
                }

                return nextDir === 'asc' ? cmp : -cmp;
              });

              rows.forEach(row => tbody.appendChild(row));
            });
          });
        });
      }

      
      // Обновление CSS-переменной для отступа якорей под липкую шапку
      // Обновление CSS-переменной для отступа якорей под липкую шапку (UPDATED)
      function updateStickyOffset() {
        const stickyHeader = document.querySelector('header.hero.hero--sticky');
        const isDesktop = window.matchMedia('(min-width: 768px)').matches;
        const stickyActive = !!stickyHeader
          && isDesktop
          && !document.body.classList.contains('q2css-active')
          && !stickyHeader.classList.contains('is-unpinned'); // NEW: отключаем offset, если шапка откреплена
        const h = stickyActive ? Math.ceil(stickyHeader.getBoundingClientRect().height) : 0;
        document.documentElement.style.setProperty('--qj-sticky-offset', (h + 8) + 'px');
      }

      // NEW: кнопка-скрепка для закрепления/открепления липкой шапки (desktop only)
      (function initStickyPinToggle(){
        const COOKIE = 'qj_pin';                 // 1 = прикреплена (по умолчанию), 0 = откреплена
        const COOKIE_MAX_AGE = 60*60*24*365;     // 1 год

        const header = document.querySelector('header.hero.hero--sticky');
        const desktopControls = document.querySelector('header.hero .d-none.d-md-flex .qj-controls');

        // Не показываем скрепку в Q2CSS-режиме и при отсутствии нужных узлов
        if (!header || !desktopControls || document.body.classList.contains('q2css-active')) return;

        function readCookie(name){
          const pair = document.cookie.split(';').map(s=>s.trim()).find(s => s.startsWith(encodeURIComponent(name)+'='));
          if (!pair) return null;
          try { return decodeURIComponent(pair.split('=').slice(1).join('=')); } catch(_) { return null; }
        }
        function writeCookie(name, value, maxAge){
          document.cookie = encodeURIComponent(name) + '=' + encodeURIComponent(String(value)) +
            '; Max-Age=' + (maxAge||0) + '; Path=/; SameSite=Lax';
        }

        // Инициализация состояния из cookie (по умолчанию прикреплена)
        const initialPinned = readCookie(COOKIE) !== '0';
        if (!initialPinned) header.classList.add('is-unpinned');

        // Создаём кнопку 📎
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'js-btn-toggle-sticky btn btn-sm btn-outline-secondary qj-pin-btn';
        btn.innerText = '📎';
        btn.title = initialPinned ? 'Открепить шапку' : 'Прикрепить шапку';
        btn.setAttribute('aria-pressed', initialPinned ? 'true' : 'false');

        function setPinned(pinned){
          header.classList.toggle('is-unpinned', !pinned);
          writeCookie(COOKIE, pinned ? '1' : '0', COOKIE_MAX_AGE);
          btn.title = pinned ? 'Открепить шапку' : 'Прикрепить шапку';
          btn.setAttribute('aria-pressed', pinned ? 'true' : 'false');
          try { updateStickyOffset(); } catch(_) {}
        }

        btn.addEventListener('click', function(){
          const currentlyPinned = !header.classList.contains('is-unpinned');
          setPinned(!currentlyPinned);
        });

        // Добавляем кнопку в начало панели управления (desktop)
        desktopControls.prepend(btn);

        // Пересчитываем отступ якорей на всякий случай
        try { updateStickyOffset(); } catch(_) {}
      })();

      window.addEventListener('load', updateStickyOffset);
      window.addEventListener('resize', () => requestAnimationFrame(updateStickyOffset));

      // Вспомогательное: раскрыть все вложенные <details> внутри основной секции
      function openAllInnerDetails(root) {
        if (!root) return;
        const list = root.querySelectorAll('details');
        list.forEach(d => { if (d !== root) d.open = true; });
      }

      // Автораскрытие по якорю + раскрытие дочерних, если это основная секция
      function openDetailsForHash(){
        const raw = location.hash.slice(1);
        if (!raw) return;
        const id = decodeURIComponent(raw);
        const el = document.getElementById(id);
        if (!el) return;

        if (el.tagName?.toLowerCase() === 'details') el.open = true;

        if (!el.matches('details')) {
          const innerDetails = el.querySelector('details');
          if (innerDetails) {
            innerDetails.open = true;
            let p = innerDetails.parentElement?.closest?.('details');
            while (p) { p.open = true; p = p.parentElement?.closest?.('details'); }
          }
        }

        let parentDetails = el.closest('details');
        while (parentDetails) { parentDetails.open = true; parentDetails = parentDetails.parentElement?.closest?.('details'); }

        const main = el.matches('.stage-collapse') ? el : el.closest('.stage-collapse');
        if (main) openAllInnerDetails(main);

        setTimeout(() => { 
            document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
            // Убираем хеш из URL без перезагрузки:
            history.replaceState(null, '', window.location.href.split('#')[0]);
          }, 0
        );
      }
      openDetailsForHash();
      window.addEventListener('hashchange', openDetailsForHash);

      // При ручном открытии основной секции — открыть дочерние
      document.addEventListener('toggle', function(e){
        const t = e.target;
        if (t && t.matches && t.matches('.stage-collapse') && t.open) {
          openAllInnerDetails(t);
        }
      }, true);

      // Lightbox
      const lb = document.getElementById('lightbox');
      const img = lb.querySelector('.lightbox-img');
      const backdrop = lb.querySelector('.lightbox-backdrop');
      let lbOpen = false, lastCloseAt = 0;

      function resetImgInlineStyles() {
        img.style.opacity = ''; img.style.left = ''; img.style.top = '';
        img.style.width = ''; img.style.height = ''; img.style.position = '';
      }
      function openLb(src) {
        if (!src || lbOpen) return;
        resetImgInlineStyles();
        img.removeAttribute('width'); img.removeAttribute('height');
        img.src = src;
        lb.classList.add('is-open');
        document.body.classList.add('no-scroll');
        lb.setAttribute('aria-hidden', 'false');
        lbOpen = true;
      }
      function closeLb() {
        if (!lbOpen) return;
        lb.classList.remove('is-open');
        document.body.classList.remove('no-scroll');
        lb.setAttribute('aria-hidden', 'true');
        resetImgInlineStyles();
        lastCloseAt = Date.now();
        setTimeout(() => { img.removeAttribute('src'); }, 200);
        lbOpen = false;
      }
      document.addEventListener('click', function(e){
        const trg = e.target.closest('.js-shot');
        if (!trg) return;
        if (Date.now() - lastCloseAt < 250) { e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.(); return; }
        e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.();
        openLb(trg.getAttribute('data-src'));
      });
      backdrop.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); closeLb(); });
      img.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); closeLb(); });
      document.addEventListener('keydown', (e) => { if (lbOpen && (e.key === 'Escape' || e.key === 'Esc')) closeLb(); });

      // Twitch embeds (parent=hostname)
      const hostname = location.hostname;
      document.querySelectorAll('iframe.js-twitch-embed[data-channel]').forEach(ifr => {
        if (ifr.classList.contains('js-video-iframe')) return;
        const ch = ifr.getAttribute('data-channel'); if (!ch) return;
        const src = 'https://player.twitch.tv/?channel=' + encodeURIComponent(ch) + '&parent=' + encodeURIComponent(hostname) + '&muted=true';
        ifr.src = src; ifr.setAttribute('allow', 'autoplay; picture-in-picture; fullscreen');
      });

      // Ленивая гидратация видео iframe
      (function hydrateVideoIframes(){
        const setSrc = (el) => {
          if (!el) return;
          if (el.classList.contains('js-twitch-embed')) {
            const ch = el.getAttribute('data-channel');
            if (!ch) return;
            const src = 'https://player.twitch.tv/?channel=' + encodeURIComponent(ch) +
                        '&parent=' + encodeURIComponent(location.hostname) + '&muted=true';
            el.src = src;
            el.setAttribute('allow', 'autoplay; picture-in-picture; fullscreen');
            el.removeAttribute('data-channel');
          } else {
            const s = el.getAttribute('data-src');
            if (!s) return;
            el.src = s;
            el.removeAttribute('data-src');
          }
          el.setAttribute('tabindex', '-1');
          try { if (document.activeElement === el) el.blur(); } catch(_) {}
        };

        const iframes = Array.from(document.querySelectorAll('iframe.js-video-iframe'));
        if (!iframes.length) return;

        if ('IntersectionObserver' in window) {
          const io = new IntersectionObserver((entries, obs) => {
            entries.forEach(en => {
              if (en.isIntersecting && en.intersectionRatio > 0) {
                setSrc(en.target);
                obs.unobserve(en.target);
              }
            });
          }, { root: null, rootMargin: '0px', threshold: 0.01 });
          iframes.forEach(el => io.observe(el));
        } else {
          iframes.forEach(setSrc);
        }

        document.addEventListener('focusin', (e) => {
          const el = e.target;
          if (el && el.tagName === 'IFRAME' && el.classList.contains('js-video-iframe') &&
              (el.hasAttribute('data-src') || el.classList.contains('js-twitch-embed'))) {
            try { el.blur(); } catch(_) {}
          }
        }, true);
      })();

      // Player stats modal
      const statsEnabled = ${PLAYER_STATS_ENABLED ? 'true' : 'false'};
      const statsBase = ${JSON.stringify(statsBaseNorm || '')};
      const pm = document.getElementById('playerModal');
      const pmBackdrop = pm?.querySelector('.player-modal-backdrop');
      const pmClose = pm?.querySelector('#playerModalClose');
      const pmName = pm?.querySelector('#playerModalName');
      const pmFrame = pm?.querySelector('#playerModalFrame');
      function openPlayerModal(playerName) {
        if (!pm || !statsEnabled || !statsBase) return;
        const sep = statsBase.includes('?') ? '&' : '?';
        const url = statsBase + sep + 'player=' + encodeURIComponent(playerName || '');
        pmName.textContent = playerName || '';
        pmFrame.src = url;
        pm.classList.add('is-open');
        document.body.classList.add('no-scroll');
        pm.setAttribute('aria-hidden', 'false');
      }
      function closePlayerModal() {
        if (!pm) return;
        pm.classList.remove('is-open');
        document.body.classList.remove('no-scroll');
        pm.setAttribute('aria-hidden', 'true');
        setTimeout(() => { if (pmFrame) pmFrame.src = 'about:blank'; }, 150);
      }
      if (statsEnabled && statsBase) {
        document.addEventListener('click', function(e){
          const a = e.target.closest('.js-player-stat');
          if (a) { e.preventDefault(); openPlayerModal(a.getAttribute('data-player') || ''); }
        });
        pmBackdrop?.addEventListener('click', closePlayerModal);
        pmClose?.addEventListener('click', closePlayerModal);
        document.addEventListener('keydown', function(e){
          if (pm?.classList.contains('is-open') && (e.key === 'Escape' || e.key === 'Esc')) closePlayerModal();
        });
      }

      // --- НОВОЕ: простые модальные окна "Игроки / Команды / Заявки" с ленивой загрузкой ---
      (function initSimpleInfoModals(){
        const MODAL_IDS = ['playersModal', 'teamsModal', 'signupsModal', 'demosModal'];

        function getChatId() {
          return document.body.getAttribute('data-chat-id') || '';
        }

        function loadModalContent(modalId) {
          const modal = document.getElementById(modalId);
          if (!modal) return;

          const body = modal.querySelector('.player-modal-body');
          if (!body) return;

          let url;
          const chatId = getChatId();
          switch (modalId) {
            case 'playersModal':
              url = '/api/players-modal';
              break;
            case 'teamsModal':
              url = '/api/teams-modal';
              break;
            case 'signupsModal':
              if (!chatId) {
                body.innerHTML = '<div class="text-danger small">Не указан турнир (chatId).</div>';
                return;
              }
              url = '/api/signups-modal?chatId=' + encodeURIComponent(chatId);
              break;
            case 'demosModal':
              if (!chatId) {
                body.innerHTML = '<div class="text-danger small">Не указан турнир (chatId).</div>';
                return;
              }
              url = '/api/demos-modal?chatId=' + encodeURIComponent(chatId);
              break;
            default:
              return;
          }

          body.innerHTML = '<div class="small text-muted">Загрузка...</div>';

          fetch(url, { headers: { 'X-Requested-With': 'XMLHttpRequest' } })
            .then(resp => {
              if (!resp.ok) throw new Error('HTTP ' + resp.status);
              return resp.text();
            })
            .then(html => {
              body.innerHTML = html;
            })
            .catch(err => {
              console.error('Error loading modal', modalId, err);
              body.innerHTML = '<div class="text-danger small">Не удалось загрузить данные. Попробуйте позже.</div>';
            });
        }

        function openModal(id) {
          loadModalContent(id);

          const el = document.getElementById(id);
          if (!el) return;
          el.classList.add('is-open');
          document.body.classList.add('no-scroll');
          el.setAttribute('aria-hidden', 'false');
        }

        function closeModal(id) {
          const el = document.getElementById(id);
          if (!el) return;
          el.classList.remove('is-open');
          document.body.classList.remove('no-scroll');
          el.setAttribute('aria-hidden', 'true');
        }

        // клики по фону и крестику
        MODAL_IDS.forEach(id => {
          const el = document.getElementById(id);
          if (!el) return;
          const backdrop = el.querySelector('.player-modal-backdrop');
          const closeBtn = el.querySelector('.js-close-modal');

          backdrop && backdrop.addEventListener('click', () => closeModal(id));
          closeBtn && closeBtn.addEventListener('click', () => closeModal(id));
        });

        // клики по ссылкам меню (десктоп и мобильный)
        document.addEventListener('click', function(e){
          const link = e.target.closest('a[href="#players-modal"], a[href="#teams-modal"], a[href="#signups-modal"], a[href="#demos-modal"]');
          if (!link) return;

          e.preventDefault();
          const href = link.getAttribute('href') || '';
          if (href === '#players-modal') openModal('playersModal');
          else if (href === '#teams-modal') openModal('teamsModal');
          else if (href === '#signups-modal') openModal('signupsModal');
          else if (href === '#demos-modal') openModal('demosModal');
        });

        // Esc закрывает любые открытые модалки
        document.addEventListener('keydown', function(e){
          if (e.key !== 'Escape' && e.key !== 'Esc') return;
          MODAL_IDS.forEach(id => closeModal(id));
        });
      })();

      // Переключатели Q2CSS и CollapseAll (UPDATED)
      const isQ2Css = ${useQ2Css ? 'true' : 'false'};
      const isCollapsedInitial = ${collapseAll ? 'true' : 'false'};
      const Q2_PARAM = ${JSON.stringify(FORCE_Q2CSS_PARAM)};
      const COLLAPSE_PARAM = ${JSON.stringify(COLLAPSE_ALL_PARAM)};
      const COLLAPSE_COOKIE = ${JSON.stringify(COLLAPSE_COOKIE)};
      const COOKIE_MAX_AGE = 60 * 60 * 24 * 365; // 1 год

      // НОВОЕ: параметр выбора турнира по alias-имени (?T=OpenFFA2025)
      const TOURN_PARAM = ${JSON.stringify(TOURNAMENT_NAME_PARAM)};
      const SUB_PARAM = ${JSON.stringify(SUB_TOURNAMENT_PARAM)};

      function toggleParam(name, current) {
        const url = new URL(location.href);
        url.searchParams.set(name, current ? '0' : '1');
        location.href = url.toString();
      }

      function isCollapsedNow() {
        try {
          const url = new URL(location.href);
          const raw = url.searchParams.get(COLLAPSE_PARAM);
          if (raw == null) return isCollapsedInitial;
          return /^(1|true|yes|on)$/i.test(String(raw));
        } catch(_) {
          return isCollapsedInitial;
        }
      }

      function collapseAllClientSide() {
        document.querySelectorAll('details').forEach(d => { d.open = false; });
      }

      function applyCollapsedUrlState() {
        const url = new URL(location.href);
        url.searchParams.set(COLLAPSE_PARAM, '1');
        const newUrl = url.toString().split('#')[0];
        history.replaceState(null, '', newUrl);
        document.cookie = encodeURIComponent(COLLAPSE_COOKIE) + '=1; Max-Age=' + COOKIE_MAX_AGE + '; Path=/; SameSite=Lax';
      }

      function scrollToFirstMainSection() {
        try { updateStickyOffset(); } catch(_) {}
        const firstSection = document.querySelector('.stage-collapse');
        if (firstSection) {
          firstSection.scrollIntoView({ behavior: 'auto', block: 'start' });
        } else {
          window.scrollTo({ top: 0, behavior: 'auto' });
        }
      }

      document.querySelectorAll('.js-btn-toggle-q2').forEach(btn =>
        btn.addEventListener('click', () => toggleParam(Q2_PARAM, isQ2Css))
      );

      document.querySelectorAll('.js-btn-toggle-collapse').forEach(btn =>
        btn.addEventListener('click', () => {
          const collapsed = isCollapsedNow();
          if (!collapsed) {
            applyCollapsedUrlState();
            collapseAllClientSide();
            scrollToFirstMainSection();
          } else {
            toggleParam(COLLAPSE_PARAM, true);
          }
        })
      );

      // НОВОЕ: обработчик выбора турнира
      document.querySelectorAll('.js-tournament-select').forEach(sel => {
        sel.addEventListener('change', () => {
          const id = sel.value || '';
          const url = new URL(location.href);
          if (id) url.searchParams.set(TOURN_PARAM, id);
          else url.searchParams.delete(TOURN_PARAM);
          // при смене турнира сбрасываем выбранный подтурнир
          url.searchParams.delete(SUB_PARAM);
          location.href = url.toString();
        });
      });

      // Обработчик выбора подтурнира
      document.querySelectorAll('.js-subtournament-select').forEach(sel => {
        sel.addEventListener('change', () => {
          const code = sel.value || '';
          const url = new URL(location.href);

          const rootAlias = sel.getAttribute('data-root-alias') || '';
          const rootChatId = sel.getAttribute('data-root-chat-id') || '';

          // Привязываемся к корневому турниру:
          if (rootAlias) {
            url.searchParams.set(TOURN_PARAM, rootAlias);
            url.searchParams.delete(${JSON.stringify(TOURNAMENT_QUERY_PARAM)});
          } else if (rootChatId) {
            url.searchParams.set(${JSON.stringify(TOURNAMENT_QUERY_PARAM)}, rootChatId);
          }

          if (code) {
            url.searchParams.set(SUB_PARAM, code);
          } else {
            url.searchParams.delete(SUB_PARAM);
          }

          location.href = url.toString();
        });
      });

      // Сброс порядка секций
      (function(){
        const COOKIE_NAME = ${JSON.stringify(SECTIONS_COOKIE)};
        function resetSectionsOrder() {
          document.cookie = encodeURIComponent(COOKIE_NAME) + '=; Max-Age=0; Path=/; SameSite=Lax';
          location.reload();
        }
        document.querySelectorAll('.js-btn-reset-sections').forEach(btn => {
          btn.addEventListener('click', (e) => { e.preventDefault(); resetSectionsOrder(); });
        });
      })();

      // DnD reorder главных секций
      (function(){
        const root = document.getElementById('sections-root');
        if (!root) return;
        const COOKIE_NAME = ${JSON.stringify(SECTIONS_COOKIE)};
        const ONE_YEAR = 60*60*24*365;
        const STORAGE_KEY = 'qj_dnd_enabled';
        const STORAGE_LAST_ON_KEY = 'qj_dnd_last_on'; // когда последний раз включили редактирование
        const AUTO_DISABLE_SECONDS = ${editTimeout}; 
        const isTouch = window.matchMedia?.('(pointer: coarse)').matches || 'ontouchstart' in window;

        function nowSeconds() {
          return Math.floor(Date.now() / 1000);
        }

        function readLastOn() {
          const v = parseInt(localStorage.getItem(STORAGE_LAST_ON_KEY) || '0', 10);
          return Number.isFinite(v) ? v : 0;
        }

        let dndEnabled;
        const stored = localStorage.getItem(STORAGE_KEY);
        const lastOn = readLastOn();

        if (stored == null) {
          // Первичная инициализация: как и раньше
          // по умолчанию включаем DnD на десктопе и выключаем на touch
          dndEnabled = !isTouch;
          localStorage.setItem(STORAGE_KEY, dndEnabled ? '1' : '0');
          if (dndEnabled) {
            localStorage.setItem(STORAGE_LAST_ON_KEY, String(nowSeconds()));
          } else {
            localStorage.removeItem(STORAGE_LAST_ON_KEY);
          }
        } else {
          dndEnabled = stored === '1';
          // Если режим редактирования завис включённым слишком долго — сразу выключаем
          if (dndEnabled && lastOn && nowSeconds() - lastOn > AUTO_DISABLE_SECONDS) {
            dndEnabled = false;
            localStorage.setItem(STORAGE_KEY, '0');
            localStorage.removeItem(STORAGE_LAST_ON_KEY);
          }
        }

        function saveOrder() {
          const ids = Array.from(root.querySelectorAll('.js-draggable-section'))
            .map(el => el.getAttribute('data-section-id'))
            .filter(Boolean);
          document.cookie = encodeURIComponent(COOKIE_NAME) + '=' + encodeURIComponent(ids.join(','))
            + '; Max-Age=' + ONE_YEAR + '; Path=/; SameSite=Lax';
        }

        function applyDndState() {
          document.body.classList.toggle('dnd-enabled', dndEnabled);
          root.querySelectorAll('.js-draggable-section').forEach(el => { el.draggable = dndEnabled; });
          document.querySelectorAll('.js-btn-toggle-dnd').forEach(btn => {
            btn.classList.toggle('btn-warning', dndEnabled);
            btn.classList.toggle('btn-outline-warning', !dndEnabled);
            btn.textContent = dndEnabled
              ? '✔️ Готово (закончить редактирование)'
              : '✏️ Редактировать разделы';
          });
        }

        let autoDisableTimer = null;
        function setupAutoDisableTimer() {
          if (autoDisableTimer) {
            clearTimeout(autoDisableTimer);
            autoDisableTimer = null;
          }
          if (!dndEnabled) return;

          const last = readLastOn() || nowSeconds();
          const elapsed = nowSeconds() - last;
          const remaining = AUTO_DISABLE_SECONDS - elapsed;

          if (remaining <= 0) {
            // На всякий случай сразу выключим
            dndEnabled = false;
            localStorage.setItem(STORAGE_KEY, '0');
            localStorage.removeItem(STORAGE_LAST_ON_KEY);
            applyDndState();
            return;
          }

          autoDisableTimer = setTimeout(() => {
            const storedVal = localStorage.getItem(STORAGE_KEY);
            const last2 = readLastOn();
            if (storedVal === '1' && last2 && nowSeconds() - last2 >= AUTO_DISABLE_SECONDS) {
              dndEnabled = false;
              localStorage.setItem(STORAGE_KEY, '0');
              localStorage.removeItem(STORAGE_LAST_ON_KEY);
              applyDndState();
            }
          }, remaining * 1000);
        }

        applyDndState();
        setupAutoDisableTimer();

        document.querySelectorAll('.js-btn-toggle-dnd').forEach(btn => {
          btn.addEventListener('click', (e) => {
            e.preventDefault();
            dndEnabled = !dndEnabled;
            localStorage.setItem(STORAGE_KEY, dndEnabled ? '1' : '0');
            if (dndEnabled) {
              localStorage.setItem(STORAGE_LAST_ON_KEY, String(nowSeconds()));
            } else {
              localStorage.removeItem(STORAGE_LAST_ON_KEY);
            }
            applyDndState();
            setupAutoDisableTimer();
          });
        });

        // Desktop DnD
        let dragging = null;
        function getDragAfterElement(container, y) {
          const els = [...container.querySelectorAll('.js-draggable-section:not(.dragging)')];
          let closest = null, closestOffset = Number.NEGATIVE_INFINITY;
          for (const el of els) {
            const box = el.getBoundingClientRect();
            const offset = y - box.top - box.height / 2;
            if (offset < 0 && offset > closestOffset) { closestOffset = offset; closest = el; }
          }
          return closest;
        }

        root.addEventListener('dragstart', (e) => {
          if (!dndEnabled) return;
          const sec = e.target.closest('.js-draggable-section'); if (!sec) return;
          dragging = sec; sec.classList.add('dragging');
          try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', sec.dataset.sectionId || ''); } catch(_) {}
        });
        root.addEventListener('dragover', (e) => {
          if (!dndEnabled || !dragging) return; e.preventDefault();
          const afterEl = getDragAfterElement(root, e.clientY);
          if (afterEl == null) root.appendChild(dragging);
          else root.insertBefore(dragging, afterEl);
        });
        root.addEventListener('drop', (e) => { if (!dndEnabled || !dragging) return; e.preventDefault(); dragging.classList.remove('dragging'); dragging = null; saveOrder(); });
        root.addEventListener('dragend', () => { if (!dndEnabled || !dragging) return; dragging.classList.remove('dragging'); dragging = null; saveOrder(); });

        // Mobile Pointer DnD (по умолчанию выключено на touch)
        let pDragging = null, started = false, startY = 0, suppressClick = false;

        root.addEventListener('pointerdown', (e) => {
          if (!dndEnabled) return;
          const sum = e.target.closest('summary.qj-toggle'); if (!sum) return;
          const sec = sum.closest('.js-draggable-section'); if (!sec) return;
          pDragging = sec; startY = e.clientY; started = false; suppressClick = false;
        }, { passive: true });

        root.addEventListener('pointermove', (e) => {
          if (!dndEnabled) return;
          if (!pDragging) return;
          const dy = Math.abs(e.clientY - startY);
          if (!started && dy >= 5) {
            started = true;
            pDragging.classList.add('dragging');
            suppressClick = true;
          }
          if (!started) return;
          const el = document.elementFromPoint(e.clientX, e.clientY);
          const targetSec = el ? el.closest('.js-draggable-section') : null;
          if (!targetSec || targetSec === pDragging) return;
          const box = targetSec.getBoundingClientRect();
          const before = e.clientY < box.top + box.height / 2;
          if (before) root.insertBefore(pDragging, targetSec);
          else root.insertBefore(pDragging, targetSec.nextSibling);
          e.preventDefault();
        });

        function endPointerDrag() {
          if (!pDragging) return;
          if (started) saveOrder();
          pDragging.classList.remove('dragging');
          pDragging = null; started = false;
          setTimeout(() => { suppressClick = false; }, 0);
        }
        root.addEventListener('pointerup', endPointerDrag);
        root.addEventListener('pointercancel', endPointerDrag);

        root.addEventListener('click', (e) => {
          if (!dndEnabled) return;
          if (!suppressClick) return;
          if (e.target.closest('summary.qj-toggle')) {
            e.preventDefault(); e.stopPropagation();
          }
        }, true);
      })();

      // Мобильное меню (≡ Меню -> список пунктов)
      (function(){
        const btn = document.querySelector('.mobile-menu-trigger');
        const modal = document.getElementById('mobileMenu');
        const closeBtn = modal?.querySelector('.qj-mm-close');
        const backdrop = modal?.querySelector('.qj-mm-backdrop');
        const list = document.getElementById('mobileMenuList');

        function fillMenu() {
          if (!list) return;
          list.innerHTML = '';

          // 1) Пытаемся взять пункты с чипов верхнего меню (как на десктопе)
          const chipLinks = Array.from(document.querySelectorAll('.qj-menu a.qj-chip'));
          if (chipLinks.length) {
            chipLinks.forEach(a => {
              const href = a.getAttribute('href') || '#';
              const text = (a.textContent || href).trim();
              const item = document.createElement('a');
              item.href = href;
              item.textContent = text;
              item.addEventListener('click', () => closeMenu());
              list.appendChild(item);
            });
          }

          // 2) Резервный вариант: формируем меню по наличию основных секций
          if (!chipLinks.length) {
            const candidates = [
              { id: 'section-news-tournament', label: 'Новости' },
              { id: 'section-desc', label: 'Информация' },
              { id: 'section-groups', label: 'Квалификации' },
              { id: 'section-finals', label: 'Финалы' },
              { id: 'section-superfinals', label: 'Суперфинал' },
              { id: 'section-stats', label: 'Статистика' },
              { id: 'section-achievements', label: 'Ачивки' },
              { id: 'section-perks', label: 'Перки' },
              { id: 'section-servers', label: 'Сервера' },
              { id: 'section-streams', label: 'Стримы' },
              { id: 'section-feedback', label: 'Отзывы' },
            ];
            candidates.forEach(c => {
              if (document.getElementById(c.id)) {
                const a = document.createElement('a');
                a.href = '#' + c.id;
                a.textContent = c.label;
                a.addEventListener('click', () => closeMenu());
                list.appendChild(a);
              }
            });
          }

          // 3) Дополнительные действия: Вернуть порядок / Редактировать разделы
          const actionsRoot = document.createElement('div');
          actionsRoot.className = 'mt-3 pt-2 border-top';

          // Кнопка "Вернуть порядок"
          const mainResetBtn = document.querySelector('header.hero .d-none.d-md-flex .js-btn-reset-sections');
          const resetBtn = document.createElement('button');
          resetBtn.type = 'button';
          resetBtn.className = 'btn btn-sm btn-outline-secondary w-100';
          resetBtn.textContent = mainResetBtn?.textContent?.trim() || '↩️ Вернуть порядок разделов по умолчанию';
          resetBtn.addEventListener('click', (e) => {
            e.preventDefault();
            if (mainResetBtn) mainResetBtn.click();
            closeMenu();
          });
          actionsRoot.appendChild(resetBtn);

          // Кнопка "Редактировать разделы"
          const mainDndBtn = document.querySelector('header.hero .d-none.d-md-flex .js-btn-toggle-dnd');
          const dndBtn = document.createElement('button');
          dndBtn.type = 'button';
          dndBtn.className = 'btn btn-sm w-100 mt-2';
          if (mainDndBtn?.classList.contains('btn-warning')) {
            dndBtn.classList.add('btn-warning');
          } else {
            dndBtn.classList.add('btn-outline-warning');
          }
          dndBtn.textContent = mainDndBtn?.textContent?.trim() || 'Редактировать разделы';
          dndBtn.addEventListener('click', (e) => {
            e.preventDefault();
            if (mainDndBtn) mainDndBtn.click();
            closeMenu();
          });
          actionsRoot.appendChild(dndBtn);

          list.appendChild(actionsRoot);
        }


        function openMenu() {
          fillMenu();
          modal?.classList.add('is-open');
          modal?.setAttribute('aria-hidden', 'false');
          document.body.classList.add('no-scroll');
        }
        function closeMenu() {
          modal?.classList.remove('is-open');
          modal?.setAttribute('aria-hidden', 'true');
          document.body.classList.remove('no-scroll');
        }

        btn?.addEventListener('click', (e) => { e.preventDefault(); openMenu(); });
        closeBtn?.addEventListener('click', (e) => { e.preventDefault(); closeMenu(); });
        backdrop?.addEventListener('click', (e) => { e.preventDefault(); closeMenu(); });
        document.addEventListener('keydown', (e) => {
          if (modal?.classList.contains('is-open') && (e.key === 'Escape' || e.key === 'Esc')) closeMenu();
        });
      })();

      // Авто-скрытие/показ шапки на мобильных при скролле
      (function initMobileHeaderAutoHide(){
        const header = document.querySelector('header.hero.hero--sticky');
        if (!header) return;
        if (document.body.classList.contains('q2css-active')) return;

        const mqMobile = window.matchMedia('(max-width: 767.98px)');
        let lastY = window.scrollY || 0;
        let isHidden = false;
        let ticking = false;

        // Направление скролла:
        //  1  — вниз
        // -1  — вверх
        //  0  — неизвестно/нет движения
        let lastDirection = 0;
        // Точка, с которой начали скроллить вверх (для порога в высоту экрана)
        let upStartY = lastY;

        function isMobile() {
          return mqMobile.matches;
        }

        function updateMobileHeaderOffset() {
          if (!isMobile()) {
            document.documentElement.style.setProperty('--qj-mobile-header-offset', '0px');
            header.classList.remove('hero--hidden-mobile');
            isHidden = false;
            return;
          }
          const h = header.getBoundingClientRect().height || 0;
          document.documentElement.style.setProperty('--qj-mobile-header-offset', h + 'px');
        }

        function handleScroll() {
          if (!isMobile()) return;
          const y = window.scrollY || 0;
          const dy = y - lastY;

          // лёгкий "дедзон", чтобы не дёргалось
          if (Math.abs(dy) < 4) {
            lastY = y;
            return;
          }

          const dir = dy > 0 ? 1 : -1; // 1 вниз, -1 вверх

          // смена направления: если только что начали скроллить вверх — запоминаем точку старта
          if (dir !== lastDirection) {
            if (dir < 0) {
              upStartY = y; // отсюда считаем пройденное вверх расстояние
            }
            lastDirection = dir;
          }

          const viewportH = window.innerHeight || 400;
          const scrolledUp = upStartY - y; // сколько пикселей реально проскроллили вверх

          if (dir > 0 && y > 20) {
            // Скролл вниз — прячем шапку
            if (!isHidden) {
              header.classList.add('hero--hidden-mobile');
              isHidden = true;
            }
          } else if (dir < 0) {
            // Скролл вверх — два варианта:
            // 1) Если мы "возле верха" (не дальше двух экранов) — показываем шапку сразу.
            // 2) Если далеко внизу — показываем только когда проскроллили вверх >= 1 экран.
            const nearTopThreshold = viewportH * 2; // "в районе двух экранов" от верха
            if (y <= nearTopThreshold) {
              if (isHidden) {
                header.classList.remove('hero--hidden-mobile');
                isHidden = false;
              }
            } else {
              if (scrolledUp >= viewportH) {
                if (isHidden) {
                  header.classList.remove('hero--hidden-mobile');
                  isHidden = false;
                }
              }
            }
          }

          lastY = y;
        }

        window.addEventListener('scroll', () => {
          if (!ticking) {
            window.requestAnimationFrame(() => {
              handleScroll();
              ticking = false;
            });
            ticking = true;
          }
        });

        window.addEventListener('resize', () => {
          updateMobileHeaderOffset();
        });

        window.addEventListener('load', () => {
          updateMobileHeaderOffset();
        });

        updateMobileHeaderOffset();
      })();



      // Инициализация сортируемых таблиц
      try { initSortableTables(); } catch (_) {}

      // === FIX: превью миниатюр вне потока, чтобы их не обрезали таблицы (v2) ===
      // В ЭТОЙ ВЕРСИИ:
      // - превью-«портал» работает ТОЛЬКО внутри .table-responsive (группы/финалы/суперфинал);
      // - локальное увеличение (ваше) там же отключаем, чтобы не было «двойного» эффекта;
      // - круглая форма перков сохраняется (копируем border-radius из исходной миниатюры).
      // === FIX (v3): глобальное превью значков ачивок/перков через портал поверх всего ===
      (function initAchBadgeHoverPreview(){
        if (!window.matchMedia || !window.matchMedia('(pointer: fine)').matches) return;

        // Работает везде, не только внутри таблиц
        const SELECTOR = '.ach-badge-link, .perc-badge-link';
        let currentAnchor = null;
        let preview = null;

        // Сносим локальные inline-обработчики и стили масштабирования, которые ставятся при рендере
        function disableLocalScale(root){
          (root || document).querySelectorAll(SELECTOR).forEach(a => {
            a.onmouseenter = a.onmouseleave = a.onfocus = a.onblur = null;
            a.removeAttribute('onmouseenter');
            a.removeAttribute('onmouseleave');
            a.removeAttribute('onfocus');
            a.removeAttribute('onblur');
            const img = a.firstElementChild;
            if (img && img.style) {
              img.style.removeProperty('transform');
              img.style.removeProperty('box-shadow');
              img.style.removeProperty('position');
              img.style.removeProperty('z-index');
            }
          });
        }
        disableLocalScale(document);

        // Если узлы подгружаются — чистим и на них тоже
        const mo = new MutationObserver(muts => {
          for (const m of muts) {
            if (m.addedNodes && m.addedNodes.length) {
              m.addedNodes.forEach(n => { if (n.nodeType === 1) disableLocalScale(n); });
            }
          }
        });
        mo.observe(document.body, { childList: true, subtree: true });

        function cleanup() {
          if (preview) { preview.remove(); preview = null; }
          document.removeEventListener('scroll', cleanup, true);
          window.removeEventListener('resize', cleanup, true);
        }

        function showPreviewFor(anchor) {
          const img = anchor.querySelector('img');
          if (!img) return;

          const rect = img.getBoundingClientRect();
          const scale = 4; // x4 как и раньше
          const w = Math.round(rect.width * scale);
          const h = Math.round(rect.height * scale);

          // Портал-превью
          preview = document.createElement('div');
          preview.className = 'ach-preview';
          preview.style.setProperty('--ach-preview-w', w + 'px');

          // Сохраняем форму (круг/скругления) — берём border-radius источника
          const br = getComputedStyle(img).borderRadius || '0';
          preview.style.setProperty('--ach-preview-br', br);

          const big = new Image();
          big.src = img.currentSrc || img.src;
          big.alt = img.alt || '';
          big.style.borderRadius = 'inherit';
          preview.appendChild(big);
          document.body.appendChild(preview);

          // Позиция: справа от значка, если влазит; иначе — слева. По Y — по верхнему краю с врезкой.
          const margin = 8;
          const vw = document.documentElement.clientWidth;
          const vh = document.documentElement.clientHeight;

          let left = rect.right + margin;
          if (left + w > vw) left = Math.max(margin, rect.left - w - margin);

          let top = rect.top;
          if (top + h > vh - margin) top = Math.max(margin, vh - h - margin);

          preview.style.left = left + 'px';
          preview.style.top  = top  + 'px';

          document.addEventListener('scroll', cleanup, true);
          window.addEventListener('resize', cleanup, true);
        }

        // Делегирование событий на документ
        document.addEventListener('mouseover', function(e){
          const a = e.target.closest(SELECTOR);
          if (a && a !== currentAnchor) {
            currentAnchor = a;
            cleanup();
            showPreviewFor(a);
          }
        }, true);

        document.addEventListener('mouseout', function(e){
          if (!currentAnchor) return;
          const to = e.relatedTarget;
          if (e.target.closest && e.target.closest(SELECTOR) === currentAnchor && (!to || !currentAnchor.contains(to))) {
            currentAnchor = null;
            cleanup();
          }
        }, true);
      })();


    })();
  </script>
</body>
</html>`;
}


async function main() {
  const client = new MongoClient(MONGODB_URI, { ignoreUndefined: true });
  await client.connect();
  db = client.db();

  colChats = db.collection('chats');
  colGameGroups = db.collection('game_groups');
  colFinalGroups = db.collection('final_groups');
  colSuperFinalGroups = db.collection('super_final_groups');
  colScreenshots = db.collection('screenshots');
  colGroupPoints = db.collection('group_points');
  colFinalPoints = db.collection('final_points');
  colSuperFinalPoints = db.collection('super_final_points');
  colNews = db.collection('news');
  colPlayerRatings = db.collection('player_ratings');   // NEW
  colFinalRatings = db.collection('final_ratings');     // NEW
  colCustomGroups = db.collection('custom_groups');     // NEW
  colCustomPoints = db.collection('custom_points');     // NEW
  colAchievements = db.collection('achievements');      // NEW
  colMaps = db.collection('maps');                      // NEW
  colGroupResults = db.collection('group_results');
  colFinalResults = db.collection('final_results');
  colSuperFinalRatings = db.collection('super_final_ratings');   // NEW
  colSuperFinalResults = db.collection('superfinal_results');
  colFeedback = db.collection('feedback');    // NEW: коллекция отзывов
  // НОВОЕ: таблицы регистрации и справочники игроков/команд
  colUsers = db.collection('users');
  colTeams = db.collection('teams');
  colRegistrationSettings = db.collection('registration_settings');
  colSignups = db.collection('signups');

  const app = express();

  // <<< ВОТ ЗДЕСЬ ПОДКЛЮЧАЕМ АНАЛИТИКУ >>>
  const { attachAnalyticsRoutes } = require('./analytics');
  attachAnalyticsRoutes(app);

  // Медиа (скриншоты)
  app.use('/media', express.static(SCREENSHOTS_DIR, {
    fallthrough: true,
    maxAge: '1h',
    immutable: false,
    setHeaders(res) {
      res.setHeader('Cache-Control', 'public, max-age=3600');
    }
  }));

  // Изображения для стилей q2
  app.use('/images', express.static(path.resolve(process.cwd(), 'public', 'images'), {
    fallthrough: true,
    maxAge: '1h',
  }));

  // Главная
  app.get('/', async (req, res) => {
    try {
      // 1) Читаем query-флаги и cookies
      const q2ParamDefined = Object.prototype.hasOwnProperty.call(req.query || {}, FORCE_Q2CSS_PARAM);
      const collParamDefined = Object.prototype.hasOwnProperty.call(req.query || {}, COLLAPSE_ALL_PARAM);

      const useQ2Css = q2ParamDefined
        ? getBoolQuery(req, FORCE_Q2CSS_PARAM, false)
        : getBoolCookie(req, Q2CSS_COOKIE, false);

      const collapseAll = collParamDefined
        ? getBoolQuery(req, COLLAPSE_ALL_PARAM, false)
        : getBoolCookie(req, COLLAPSE_COOKIE, false);

      // порядок секций из cookie
      const sectionsOrder = parseSectionsOrderCookie(req);

      // 2) Если пришли query — обновим cookies (1 год)
      const cookiesToSet = [];
      const maxAge = 60 * 60 * 24 * 365; // 1 год

      if (q2ParamDefined) {
        cookiesToSet.push(`${Q2CSS_COOKIE}=${useQ2Css ? '1' : '0'}; Max-Age=${maxAge}; Path=/; SameSite=Lax`);
      }
      if (collParamDefined) {
        cookiesToSet.push(`${COLLAPSE_COOKIE}=${collapseAll ? '1' : '0'}; Max-Age=${maxAge}; Path=/; SameSite=Lax`);
      }

      // 3) Определяем выбранный турнир
      // Сначала пробуем новый alias-параметр ?T=OpenFFA2025 (SITE_NAMES)
      const rawNameParam = req.query?.[TOURNAMENT_NAME_PARAM];
      let selectedChatId = null;

      if (typeof rawNameParam === 'string' && rawNameParam.trim()) {
        const idFromName = getChatIdForSiteName(rawNameParam.trim());
        if (idFromName && ALLOWED_CHAT_IDS.includes(idFromName)) {
          selectedChatId = idFromName;
        }
      }

      // 3.1) Подтурниры: обрабатываем параметр ?Sub=... и связи subTournaments
      const rawSubParam = req.query?.[SUB_TOURNAMENT_PARAM];
      const subCtx = await buildSubTournamentsContext(selectedChatId, rawSubParam);

      let effectiveChatId = subCtx.effectiveChatId;
      if (!ALLOWED_CHAT_IDS.includes(effectiveChatId)) {
        // на всякий случай, если вдруг дочерний турнир не входит в ALLOWED_CHAT_IDS
        effectiveChatId = selectedChatId;
      }
      selectedChatId = effectiveChatId;

      // Если alias не передан или невалиден — читаем старый numeric-параметр ?tournamentId=...
      if (selectedChatId == null) {
        const rawParamId = req.query?.[TOURNAMENT_QUERY_PARAM];
        const parsedId = Number(rawParamId);
        if (Number.isFinite(parsedId) && ALLOWED_CHAT_IDS.includes(parsedId)) {
          selectedChatId = parsedId;
        } else {
          selectedChatId = DEFAULT_CHAT_ID;
        }
      }

      // 4) Метаданные для селектора турниров
      const tournamentsMeta = await getTournamentsMeta(ALLOWED_CHAT_IDS);

      // 4.1) Подготовим данные для селектора подтурниров
      const subTournamentsMeta = Array.isArray(subCtx.items) && subCtx.items.length > 0
        ? subCtx.items
        : null;
      const subRootChatId = subCtx.rootChatId || null;
      const subRootAlias = subCtx.rootAlias || null;

      // 5) Загружаем данные по выбранному турниру
      const [
        tournament, groups, finals, superfinals,
        groupPtsMap, finalPtsMap, superFinalPtsMap,
        // НОВОЕ:
        groupResultsByGroup,
        finalResultsByGroup,
        superResultsByGroup,
        // НОВОЕ:
        //users,
        //teams,
        //registrationSettings,
        //signups,
      ] = await Promise.all([
        getTournament(selectedChatId),
        getGroups(selectedChatId),
        getFinals(selectedChatId),
        getSuperfinals(selectedChatId),
        getGroupPointsMap(selectedChatId),
        getFinalPointsMap(selectedChatId),
        getSuperFinalPointsMap(selectedChatId),
        // НОВОЕ:
        getGroupResultsMap(selectedChatId),
        getFinalResultsMap(selectedChatId),
        getSuperFinalResultsMap(selectedChatId),
        // НОВОЕ: глобальные игроки/команды и настройки регистрации по текущему турниру
        //Теперь это не нужно, т.к. данные будем тянуть по API при открытии модалок.
        //colUsers.find({}).sort({ nickNorm: 1, nick: 1 }).toArray(),
        //colTeams.find({}).sort({ nameNorm: 1, name: 1 }).toArray(),
        //colRegistrationSettings.findOne({ chatId: selectedChatId }),
        //colSignups.find({ chatId: selectedChatId }).sort({ createdAt: 1 }).toArray(),
      ]);

      //--------------------------------------------------------------------
      // SUB-TOURNAMENT HANDLING (правильный selectedChatId)
      //--------------------------------------------------------------------
      const subCode = req.query?.Sub ? String(req.query.Sub).trim() : null;

      if (subCode) {
        // ищем турнир с соответствующим tournamentSubCode
        const subTournament = await colChats.findOne({ tournamentSubCode: subCode });

        // подменяем selectedChatId, ЕСЛИ такой подтурнир реально есть
        if (subTournament && subTournament.chatId) {
          selectedChatId = subTournament.chatId;

          // заново загружаем tournament уже дочерний
          const newTournament = await getTournament(selectedChatId);
          if (newTournament) {
            Object.assign(tournament, newTournament);
          }
        }
      }


      PLAYER_STATS_ENABLED = tournament.tournamentStatsEnabled;
      PLAYER_STATS_URL = tournament.tournamentStatsUrl;

      const [groupScreens, finalScreens, superScreens] = await Promise.all([
        getScreensForScope(selectedChatId, 'group', groups),
        getScreensForScope(selectedChatId, 'final', finals),
        getScreensForScope(selectedChatId, 'superfinal', superfinals),
      ]);

      const [groupRunId, finalRunId, superRunId] = await Promise.all([
        findLatestRunIdForScope(selectedChatId, 'group'),
        findLatestRunIdForScope(selectedChatId, 'final'),
        findLatestRunIdForScope(selectedChatId, 'superfinal'),
      ]);

      const [tournamentNews, groupsNews, finalsNews, superNews] = await Promise.all([
        listNews(selectedChatId, 'tournament', null),
        groupRunId ? listNews(selectedChatId, 'group', groupRunId) : Promise.resolve([]),
        finalRunId ? listNews(selectedChatId, 'final', finalRunId) : Promise.resolve([]),
        superRunId ? listNews(selectedChatId, 'superfinal', superRunId) : Promise.resolve([]),
      ]);

      const [definedGroupRating, definedFinalRating, definedSuperFinalRating] = await Promise.all([
        getDefinedGroupRating(selectedChatId),
        getDefinedFinalRating(selectedChatId),
        getDefinedSuperFinalRating(selectedChatId),  // Новый вызов для рейтинга суперфинала
      ]);

      const [customGroups, customPointsByGroup] = await Promise.all([
        getCustomGroups(selectedChatId),
        getCustomPointsByGroup(selectedChatId),
      ]);
      const customScreens = await getScreensForScope(selectedChatId, 'custom', customGroups);

      const achievements = await getAchievements(selectedChatId);
      const achievementsAch = achievements.filter(a => String(a?.type || 'achievement').toLowerCase() === 'achievement');
      const achievementsPerc = achievements.filter(a => String(a?.type || 'achievement').toLowerCase() === 'perc');
      const achievementsIndex = buildAchievementsIndex(achievements);

      const mapsList = await getMaps(selectedChatId);
      const feedbackEntries = await getFeedback(selectedChatId);

      // ... после загрузки groups, finals, superfinals и рейтингов:
      const usersList = await colUsers.find({}).toArray();
      const userMap = new Map();
      usersList.forEach(u => {
        // Нормализация ключа: убрать регистр и спецсимволы
        const norm = (u.nickNorm || u.nick || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
        if (norm) {
          userMap.set(norm, u.country?.toLowerCase() || '');
        }
      });

      // Привязка country к каждому игроку в группах
      for (const g of groups) {
        if (Array.isArray(g.players)) {
          g.players.forEach(p => {
            const key = (p.nameNorm || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
            p.country = userMap.get(key) || '';
          });
        }
      }
      for (const g of finals) {
        if (Array.isArray(g.players)) {
          g.players.forEach(p => {
            const key = (p.nameNorm || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
            p.country = userMap.get(key) || '';
          });
        }
      }
      for (const g of superfinals) {
        if (Array.isArray(g.players)) {
          g.players.forEach(p => {
            const key = (p.nameNorm || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
            p.country = userMap.get(key) || '';
          });
        }
      }
      // То же для кастомных групп, если используется:
      for (const g of customGroups) {
        if (Array.isArray(g.players)) {
          g.players.forEach(p => {
            const key = (p.nameNorm || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
            p.country = userMap.get(key) || '';
          });
        }
      }

      // Привязка country к игрокам в определенных рейтингах (если они есть)
      for (const rating of [definedGroupRating, definedFinalRating, definedSuperFinalRating]) {
        if (rating && Array.isArray(rating.players)) {
          rating.players.forEach(p => {
            const key = (p.nameNorm || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
            p.country = userMap.get(key) || '';
          });
        }
      }

      const html = renderPage({
        tournament, groups, finals, superfinals,
        groupScreens, finalScreens, superScreens,
        groupPtsMap, finalPtsMap, superFinalPtsMap,
        tournamentNews, groupsNews, finalsNews, superNews,
        useQ2Css,
        collapseAll,
        definedGroupRating,
        definedFinalRating,
        definedSuperFinalRating, // передаем данные рейтинга суперфинала
        customGroups,
        customPointsByGroup,
        customScreens,
        achievementsAch,
        achievementsPerc,
        achievementsIndex,
        statsBaseUrl: PLAYER_STATS_URL,
        mapsList,
        sectionOrder: sectionsOrder,
        // Новое поле отзывов:
        feedbackEntries,
        // НОВОЕ:
        tournamentsMeta,
        selectedChatId,
        // НОВОЕ: селектор подтурниров
        subTournamentsMeta,
        subRootChatId,
        subRootAlias,
        // НОВОЕ:
        groupResultsByGroup,
        finalResultsByGroup,
        superResultsByGroup,
        // НОВОЕ:
        //users,
        //teams,
        //registrationSettings,
        //signups,
      });

      if (cookiesToSet.length) {
        res.setHeader('Set-Cookie', cookiesToSet);
      }
      res.status(200).send(html);
    } catch (e) {
      console.error('Error rendering page:', e);
      res.status(500).send('Internal Server Error');
    }
  });

  // --- API для модальных окон Игроки / Команды / Заявки ---

  app.get('/api/players-modal', async (req, res) => {
    try {
      const users = await colUsers.find({}).sort({ nickNorm: 1, nick: 1 }).toArray();
      const html = renderUsersModalBody(users);
      res.type('text/html').send(html);
    } catch (err) {
      console.error('Error in /api/players-modal:', err);
      res.status(500).type('text/plain').send('Ошибка загрузки игроков');
    }
  });

  app.get('/api/teams-modal', async (req, res) => {
    try {
      const teams = await colTeams.find({}).sort({ nameNorm: 1, name: 1 }).toArray();
      const html = renderTeamsModalBody(teams);
      res.type('text/html').send(html);
    } catch (err) {
      console.error('Error in /api/teams-modal:', err);
      res.status(500).type('text/plain').send('Ошибка загрузки команд');
    }
  });

  app.get('/api/signups-modal', async (req, res) => {
    try {
      const chatIdRaw = req.query.chatId;
      const chatId = chatIdRaw ? Number(chatIdRaw) : null;
      if (!chatId) {
        return res.status(400).type('text/plain').send('chatId не указан');
      }

      const registrationSettings = await colRegistrationSettings.findOne({ chatId });
      //const signups = await colSignups.find({ chatId }).sort({ createdAt: 1 }).toArray();

      const allUsers = await colUsers.find({}).toArray();
      const idMap = new Map();
      const nameMap = new Map();
      // Подготовим поиск по userId и по nickNorm
      allUsers.forEach(u => {
        idMap.set(String(u._id), u.country?.toLowerCase() || '');
        const norm = (u.nickNorm || u.nick || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
        if (norm) nameMap.set(norm, u.country?.toLowerCase() || '');
      });

      const signups = await colSignups.find({ chatId }).sort({ createdAt: 1 }).toArray();
      // Для каждой заявки определим страну:
      signups.forEach(s => {
        let country = '';
        if (s.userId && idMap.has(String(s.userId))) {
          country = idMap.get(String(s.userId));
        }
        if (!country) {
          const nameKey = (s.playerNick || s.teamName || '').toString().trim().toLowerCase().replace(/[^a-z0-9а-яё]/gi, '');
          country = nameMap.get(nameKey) || '';
        }
        s.country = country;
      });

      const html = renderSignupsModalBody(registrationSettings, signups);

      res.type('text/html').send(html);
    } catch (err) {
      console.error('Error in /api/signups-modal:', err);
      res.status(500).type('text/plain').send('Ошибка загрузки заявок');
    }
  });

  app.get('/api/demos-modal', async (req, res) => {
    try {
      const rawChatId = req.query.chatId;
      const chatId = Number(rawChatId);

      if (!Number.isFinite(chatId)) {
        res.status(400).type('text/plain; charset=utf-8').send('chatId is required');
        return;
      }

      const [groups, finals, superfinals] = await Promise.all([
        getGroups(chatId),
        getFinals(chatId),
        getSuperfinals(chatId),
      ]);

      // helper: получить список имён игроков для группы/финала/суперфинала
      function getPlayerNamesFromGroupDoc(g) {
        const playersArr = Array.isArray(g?.players) ? g.players : [];
        const names = [];

        for (const p of playersArr) {
          if (!p) continue;
          const name = (p.nameOrig || p.nameNorm || '').trim();
          if (!name) continue;
          names.push(name);
        }

        // на всякий случай уберём дубли
        const uniq = [];
        const seen = new Set();
        for (const n of names) {
          const key = n.toLowerCase();
          if (seen.has(key)) continue;
          seen.add(key);
          uniq.push(n);
        }
        return uniq;
      }

      const rows = [];

      function collect(stageLabel, items) {
        if (!Array.isArray(items)) return;
        for (const g of items) {
          const groupId = g.groupId != null ? Number(g.groupId) : null;
          const urls = Array.isArray(g?.demos)
            ? g.demos.map(u => String(u || '').trim()).filter(Boolean)
            : [];

          const players = getPlayerNamesFromGroupDoc(g);

          for (const url of urls) {
            rows.push({ stage: stageLabel, groupId, url, players });
          }
        }
      }

      collect('Квалификация', groups);
      collect('Финал', finals);
      collect('Суперфинал', superfinals);

      const stageOrder = { 'Квалификация': 1, 'Финал': 2, 'Суперфинал': 3 };
      rows.sort((a, b) => {
        const sa = stageOrder[a.stage] || 99;
        const sb = stageOrder[b.stage] || 99;
        if (sa !== sb) return sa - sb;
        const ga = a.groupId ?? 0;
        const gb = b.groupId ?? 0;
        return ga - gb;
      });

      const html = renderDemosModalBody(rows);

      res
        .status(200)
        .type('text/html; charset=utf-8')
        .send(html);
    } catch (err) {
      console.error('Error in /api/demos-modal', err);
      res
        .status(500)
        .type('text/plain; charset=utf-8')
        .send('INTERNAL_ERROR');
    }
  });


  // Healthcheck
  const server = app.listen(PORT, () => {
    console.log(`Site started on http://localhost:${PORT}`);
    if (ALLOWED_CHAT_IDS.length === 1) {
      console.log(`Tournament chatId=${DEFAULT_CHAT_ID}`);
    } else {
      console.log(`Allowed tournaments: ${ALLOWED_CHAT_IDS.join(', ')} (default=${DEFAULT_CHAT_ID})`);
      console.log(`URL param for selection: ?${TOURNAMENT_QUERY_PARAM}=<chatId>`);
    }
    console.log(`Optional CSS param: ?${FORCE_Q2CSS_PARAM}=1`);
  });

  server.on('connection', (socket) => {
    socket.setMaxListeners(30);
  });
}



main().catch(err => {
  console.error('Startup error:', err);
  process.exit(1);
});
