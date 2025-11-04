// site.js
require('dotenv').config();
const express = require('express');
const { MongoClient } = require('mongodb');
const path = require('path');

const MONGODB_URI = process.env.MONGODB_URI;
const SITE_CHAT_ID = process.env.SITE_CHAT_ID; // ID чата/группы/канала, для которого показываем сайт
const PORT = Number(process.env.SITE_PORT || 3000);
const SCREENSHOTS_DIR = process.env.SCREENSHOTS_DIR || path.resolve(process.cwd(), 'screenshots');
const PLAYER_STATS_URL = process.env.PLAYER_STATS_URL || ''; // https://q2.agly.eu/?lang=ru&r=r_6901e479cced6
const PLAYER_STATS_ENABLED = /^(1|true|yes)$/i.test(String(process.env.PLAYER_STATS_ENABLED || ''));

// Параметр для принудительного подключения CSS quake2.com.ru
const FORCE_Q2CSS_PARAM = 'forceQuake2ComRuCSS';
// Параметр для сворачивания всех новостных секций по умолчанию
const COLLAPSE_ALL_PARAM = 'CollapseAll';
// Cookies для сохранения пользовательских предпочтений
const Q2CSS_COOKIE = 'qj_q2css';
const COLLAPSE_COOKIE = 'qj_collapse';
const SECTIONS_COOKIE = 'qj_sections'; // порядок главных секций

const SITE_BG_IMAGE = process.env.SITE_BG_IMAGE || '/images/fon1.png';

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
let colPlayerRatings, colFinalRatings;
let colCustomGroups, colCustomPoints;   // NEW: кастомные группы/очки
let colAchievements;                    // NEW: ачивки
let colMaps;                            // NEW: карты

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
  // Сначала экранируем весь текст
  const escaped = escapeHtml(String(text || ''));

  // Единый проход: http/https ИЛИ внутренняя #якорь-ссылка
  const re = /(\bhttps?:\/\/[^\s<>"']+)|(^|[\s(])#([A-Za-z][\w-]{0,100})/g;

  return escaped.replace(re, (m, url, pre, anchor) => {
    if (url) {
      // Внешние ссылки — как раньше, в новой вкладке
      return `<a href="${url}" target="_blank" rel="noopener">${url}</a>`;
    }
    if (anchor) {
      // Внутренние якоря — в той же вкладке (для плавного скролла и авто-раскрытия секций)
      return `${pre}<a href="#${anchor}">#${anchor}</a>`;
    }
    return m;
  });
}

function renderServersSection(tournament, containerClass, collapsedByDefault = false) {
  const hasServers = Array.isArray(tournament.servers) && tournament.servers.length > 0;
  if (!hasServers) return '';
  const openAttr = collapsedByDefault ? '' : ' open';
  return `
    <section class="mb-4">
      <details id="section-servers" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">Список серверов</span>
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
          <span class="section-title">Архив с картами</span>
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
          <span class="section-title">Список карт</span>
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
  // Поддержка markdown‑подобных цитат: строки, начинающиеся с "> " или ">"
  const src = String(text || '').replace(/\r\n?/g, '\n');
  const lines = src.split('\n');

  const blocks = [];
  let buf = [];
  let inQuote = false;

  function pushBlock() {
    if (!buf.length) return;
    const raw = buf.join('\n');
    // linkify уже экранирует текст и превращает URL/якоря в ссылки
    const innerHtml = linkify(raw);
    blocks.push(inQuote
      ? `<blockquote class="qj-quote">${innerHtml}</blockquote>`
      : `<div class="qj-paragraph">${innerHtml}</div>`);
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

async function getTournament(chatId) {
  const doc = await colChats.findOne({ chatId });
  return {
    name: doc?.tournamentName || '',
    site: doc?.tournamentSite || '',
    desc: doc?.tournamentDesc || '',
    logo: doc?.tournamentLogo || null, // { relPath, ... }

    // Новые поля для верхнего блока
    servers: Array.isArray(doc?.tournamentServers) ? doc.tournamentServers : [],
    pack: doc?.tournamentPack || '',
    streams: Array.isArray(doc?.tournamentStreams) ? doc.tournamentStreams : [],
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
    const shots = renderScreenshots(files);

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

// Мини-иконки ачивок рядом с именем игрока
// Мини-иконки ачивок/перков рядом с именем игрока
// Мини-иконки ачивок/перков рядом с именем игрока
// Мини-иконки ачивок/перков рядом с именем игрока (с подсветкой контуров)
function renderAchievementBadgesInline(nameNorm, achIndex) {
  const key = String(nameNorm || '').trim().toLowerCase();
  if (!key || !achIndex || !achIndex.has(key)) return '';
  const pack = achIndex.get(key) || { achs: [], percs: [] };
  const achs = pack.achs || [];
  const percs = pack.percs || [];
  if (!achs.length && !percs.length) return '';

  const renderItem = (ai, cls, kind) => {
    const href = `#${escapeHtml(ai.id)}`;
    const linkCls = kind === 'perc' ? 'perc-badge-link' : 'ach-badge-link';
    if (ai.url) {
      const alt = escapeHtml(ai.title || 'ach');
      return `<a href="${href}" class="me-1 align-middle ${linkCls}" title="${alt}">
        <img src="${escapeHtml(ai.url)}" alt="${alt}" class="${cls}" loading="lazy" />
      </a>`;
    }
    return `<a href="${href}" class="me-1 align-middle ${linkCls}" title="${escapeHtml(ai.title || 'ach')}">
      <span class="ach-badge-fallback">🏆</span>
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
    const who = n.authorUsername ? `@${n.authorUsername}` : (n.authorId ? `#${n.authorId}` : '');
    const nid = (n && n._id && typeof n._id.toString === 'function') ? n._id.toString() : String(n?._id || '');
    const idAttr = nid ? ` id="news-${escapeHtml(nid)}"` : '';
    const selfLink = nid ? `<a href="#news-${escapeHtml(nid)}" class="ms-2 text-decoration-none" aria-label="Ссылка на новость">#</a>` : '';

    // 1) Рендерим rich-text (экранирование + автоссылки)
    const baseHtml = renderNewsRichText(n.text || '');
    // 2) Инлайн-вставляем плееры на место «голых» URL
    const textWithEmbeds = injectEmbedsIntoNewsHtml(baseHtml).trim();

    return `
      <li class="list-group-item"${idAttr}>
        <div class="d-flex flex-column flex-md-row">
          <div class="flex-grow-1">
            <div class="news-text" style="white-space: pre-wrap;">${textWithEmbeds}</div>
          </div>
          <div class="news-meta small text-muted mt-2 mt-md-0 ms-0 ms-md-3">
            ${escapeHtml(ts)}${who ? ` (${escapeHtml(who)})` : ''}${selfLink}
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


function renderPlayers(players = [], ptsMap = null, achIndex = null) {
  if (!players?.length) return '<div class="text-muted small">(пусто)</div>';

  const arr = players.slice();
  const hasPtsFlag = ptsMap && arr.some(p => ptsMap.has(p.nameNorm));

  if (hasPtsFlag) {
    arr.sort((a, b) => {
      const aHas = ptsMap.has(a.nameNorm);
      const bHas = ptsMap.has(b.nameNorm);
      if (aHas && bHas) {
        const ap = Number(ptsMap.get(a.nameNorm));
        const bp = Number(ptsMap.get(b.nameNorm));
        if (ap !== bp) return ap - bp;
        return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
      }
      if (aHas !== bHas) return aHas ? -1 : 1;
      return (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' });
    });
  } else {
    arr.sort((a, b) => (a.nameOrig || '').localeCompare(b.nameOrig || '', undefined, { sensitivity: 'base' }));
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

      const pnameHtml = PLAYER_STATS_ENABLED
        ? `<a href="#" class="player-name player-link qj-accent fw-semibold js-player-stat"
              data-player="${escapeAttr(displayName)}">${escapeHtml(displayName)}</a>`
        : `<span class="player-name qj-accent fw-semibold">${escapeHtml(displayName)}</span>`;

      return `<li>
        ${posHtml}
        ${pnameHtml}
        ${metaHtml}
      </li>`;
    }).join('')}
  </ul>`;
}


function renderScreenshots(files = []) {
  if (!files?.length) {
    return '<div class="text-muted small">Скриншоты отсутствуют</div>';
  }
  const thumbs = files.map(f => {
    const url = '/media/' + relToUrl(f.relPath || '');
    const alt = escapeHtml(f.mime || 'image');
    return `
      <button type="button" class="js-shot qj-shot-btn me-1 mb-1" data-src="${url}" aria-label="Открыть скриншот">
        <img src="${url}" alt="${alt}" loading="lazy"
             style="max-width: 120px; max-height: 90px; object-fit: cover; border-radius: 6px; border: 1px solid rgba(0,0,0,0.08);" />
      </button>`;
  }).join('');
  return `<div class="mt-1">${thumbs}</div>`;
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
        <iframe class="ratio ratio-16x9"
                src="${yt}"
                title="Видео YouTube"
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                allowfullscreen
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // Twitch (канал)
  const twitchChan = parseTwitchChannel(url);
  if (twitchChan) {
    return `
      <div class="stream-embed mb-2">
        <iframe class="js-twitch-embed"
                data-channel="${escapeHtml(twitchChan)}"
                title="Видео Twitch"
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
        <iframe src="${escapeHtml(vkVideo)}"
                title="Видео VK"
                allowfullscreen
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // RuTube
  const rutube = toRutubeEmbed(url);
  if (rutube) {
    return `
      <div class="stream-embed mb-2">
        <iframe src="${escapeHtml(rutube)}"
                title="Видео RuTube"
                allowfullscreen
                style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
      </div>
    `;
  }

  // VK Play
  const vk = toVkPlayEmbed(url);
  if (vk) {
    return `
      <div class="stream-embed mb-2">
        <iframe src="${escapeHtml(vk)}"
                title="Видео"
                allowfullscreen
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
    // ВАЖНО: не экранируем здесь src — sanitizeAchievementHtml сделает это один раз корректно
    return `<iframe src="${yt}" title="Видео YouTube" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // Twitch
  const twitchChan = parseTwitchChannel(url);
  if (twitchChan) {
    return `<iframe class="js-twitch-embed" data-channel="${escapeHtml(twitchChan)}" title="Видео Twitch" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // VK Video
  const vkVideo = toVkVideoEmbed(url);
  if (vkVideo) {
    return `<iframe src="${vkVideo}" title="Видео VK" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // RuTube
  const rutube = toRutubeEmbed(url);
  if (rutube) {
    return `<iframe src="${rutube}" title="Видео RuTube" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
  }

  // VK Play
  const vk = toVkPlayEmbed(url);
  if (vk) {
    return `<iframe src="${vk}" title="Видео" allowfullscreen style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>`;
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
          <span class="section-title">Частота назначений карт</span>
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
  const descHtml = linkify(tournament.desc);

  return `
    <section class="mb-4">
      <details id="section-desc" class="stage-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">Описание турнира</span>
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


// Только блок «Стримеры» — для обычного режима, выводим в самом конце страницы
function renderStreamsOnly(tournament, containerClass) {
  const hasStreams = Array.isArray(tournament.streams) && tournament.streams.length > 0;
  if (!hasStreams) return '';

  const embItems = tournament.streams.map(raw => {
    const url = String(raw || '').trim();
    const safe = escapeHtml(url);
    const yt = toYouTubeEmbed(url);
    const twitchChan = parseTwitchChannel(url);
    const vk = toVkPlayEmbed(url);

    if (yt) {
      return `
        <div class="stream-embed mb-2">
          <iframe class="ratio ratio-16x9"
                  src="${yt}"
                  title="Трансляция YouTube"
                  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                  allowfullscreen
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>
      `;
    }
    if (twitchChan) {
      return `
        <div class="stream-embed mb-2">
          <iframe class="js-twitch-embed"
                  data-channel="${escapeHtml(twitchChan)}"
                  title="Трансляция Twitch"
                  allowfullscreen
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>
      `;
    }
    if (vk) {
      return `
        <div class="stream-embed mb-2">
          <iframe src="${escapeHtml(vk)}"
                  title="Трансляция"
                  allowfullscreen
                  style="width:100%; aspect-ratio:16/9; border:0; border-radius:10px;"></iframe>
          <div class="small mt-1"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>
        </div>
      `;
    }
    return `<div class="mb-2"><a href="${safe}" target="_blank" rel="noopener">${safe}</a></div>`;
  }).join('');

  return `
    <section class="mb-5">
      <div class="${containerClass}">
        <div class="row g-3">
          <div class="col-12">
            <div class="card shadow-sm h-100">
              <div class="card-body">
                <h5 class="card-title mb-2">Стримеры</h5>
                ${embItems || '<div class="text-muted small">(нет)</div>'}
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  `;
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
          <span class="section-title">Статистика турнира</span>
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


function renderSection(title, items, scope, screensMap, ptsMap = null, collapsedByDefault = false, achIndex = null) {
  if (!items?.length) return '<div class="text-muted">Нет данных</div>';

  const label = (scope === 'group') ? 'Квалификация' : (scope === 'final') ? 'Финал' : 'Суперфинал';
  const openAttr = collapsedByDefault ? '' : ' open';

  const cells = items.map(g => {
    const id = `${scope}-${g.groupId}`;
    const players = renderPlayers(g.players || [], ptsMap, achIndex);
    const maps = renderMaps(g.maps || []);
    const demos = renderDemos(Array.isArray(g.demos) ? g.demos : []);
    const files = screensMap.get(Number(g.groupId)) || [];
    const shots = renderScreenshots(files);
    const timeLine = renderTimeStr(g.time);

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
              </div>
            </div>
          </div>
        </details>
      </div>
    `;
  }).join('');

  return `<div class="cards-grid">${cells}</div>`;
}

function renderStageRating(title, items, ptsMap, sectionId, collapsedByDefault = false, achIndex = null) {
  if (!items?.length || !ptsMap || ptsMap.size === 0) return '';

  const seen = new Set();
  const rows = [];
  for (const g of items) {
    for (const p of (g.players || [])) {
      if (!p?.nameNorm) continue;
      if (seen.has(p.nameNorm)) continue;
      if (!ptsMap.has(p.nameNorm)) continue;
      seen.add(p.nameNorm);
      rows.push({ nameOrig: p.nameOrig, nameNorm: p.nameNorm, pts: Number(ptsMap.get(p.nameNorm)) });
    }
  }
  if (!rows.length) return '';

  rows.sort((a, b) => a.pts - b.pts || a.nameOrig.localeCompare(b.nameOrig, undefined, { sensitivity: 'base' }));

  const tr = rows.map((r, i) => {
    const badges = renderAchievementBadgesInline(r.nameNorm, achIndex);
    const pnameHtml = PLAYER_STATS_ENABLED
      ? `<a href="#" class="player-link qj-accent fw-semibold js-player-stat"
             data-player="${escapeAttr(r.nameOrig)}">${escapeHtml(r.nameOrig)}</a>`
      : `<span class="qj-accent fw-semibold">${escapeHtml(r.nameOrig)}</span>`;
    return `
      <tr>
        <td class="pos text-muted">${i + 1}</td>
        <td class="pname">${pnameHtml}${badges}</td>
        <td class="pts qj-pts fw-semibold">${r.pts}</td>
      </tr>
    `;
  }).join('');

  const openAttr = collapsedByDefault ? '' : ' open';

  return `
    <section class="mb-4">
      <details id="${escapeHtml(sectionId)}" class="sub-collapse"${openAttr}>
        <summary class="qj-toggle">
          <span class="section-title">${escapeHtml(title)}</span>
          <a href="#${escapeHtml(sectionId)}" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
          <span class="qj-badge ms-auto">${rows.length}</span>
        </summary>
        <div class="mt-2">
          <div class="table-responsive">
            <table class="table table-hover align-middle rating-table qj-table">
              <thead>
                <tr>
                  <th class="pos small text-secondary" style="width:64px;">№</th>
                  <th class="small text-secondary">Игрок</th>
                  <th class="small text-secondary text-end" style="width:120px;">Количество очков</th>
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
  customGroups = [],
  customPointsByGroup = new Map(),
  customScreens = new Map(),
  achievementsAch = [],
  achievementsPerc = [],
  achievementsIndex = new Map(),
  statsBaseUrl = '',
  mapsList = [],
  sectionOrder = [], // порядок главных секций из cookie
}) {
  const logoUrl = tournament.logo?.relPath ? `/media/${relToUrl(tournament.logo.relPath)}` : null;
  const logoMime = tournament.logo?.mime || 'image/png';

  const faviconLink = logoUrl
    ? `<link rel="icon" type="${escapeHtml(logoMime)}" href="${logoUrl}">`
    : `<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'%3E%3Ccircle cx='32' cy='32' r='30' fill='%23007bff'/%3E%3Ctext x='32' y='39' font-family='Arial' font-size='28' text-anchor='middle' fill='white'%3EQ%3C/text%3E%3C/svg%3E'>`;

  const logoBlock = logoUrl ? `<img src="${logoUrl}" alt="Логотип турнира" class="hero-logo me-3" />` : '';
  const siteLink = tournament.site
    ? `<a href="${escapeHtml(tournament.site)}" target="_blank" rel="noopener" class="small text-muted text-decoration-none">${escapeHtml(tournament.site)}</a>`
    : '';

  const containerClass = useQ2Css ? 'container-fluid px-0' : 'container';

  const extrasTopSec = useQ2Css
    ? renderTournamentExtrasFull(tournament, containerClass, true /* linksOnly */, collapseAll, mapsList)
    : renderTournamentExtrasTopOnly(tournament, containerClass, collapseAll, mapsList);

  const descSection = renderTournamentDescSection(tournament, containerClass, collapseAll);

  const groupsCards = renderSection('Квалификации', groups, 'group', groupScreens, groupPtsMap, collapseAll, achievementsIndex);
  const finalsCards = renderSection('Финальный раунд', finals, 'final', finalScreens, finalPtsMap, collapseAll, achievementsIndex);
  const superCards = renderSection('Суперфинал', superfinals, 'superfinal', superScreens, superFinalPtsMap, collapseAll, achievementsIndex);

  const groupsMapsRatingSec = renderMapsPopularityTable('maps-groups', groups, collapseAll);
  const finalsMapsRatingSec = renderMapsPopularityTable('maps-finals', finals, collapseAll);
  const superMapsRatingSec = renderMapsPopularityTable('maps-superfinals', superfinals, collapseAll);

  const groupsNewsSec = renderNewsList('Новости квалификаций', groupsNews, collapseAll, 'section-news-groups');
  const finalsNewsSec = renderNewsList('Новости финального раунда', finalsNews, collapseAll, 'section-news-finals');
  const superNewsSec = renderNewsList('Новости суперфинала', superNews, collapseAll, 'section-news-super');

  const groupsRatingSec = renderStageRating(
    'Общий рейтинг квалификации (по количеству очков, по возрастанию)',
    groups, groupPtsMap, 'rating-groups', collapseAll, achievementsIndex
  );
  const finalsRatingSec = renderStageRating(
    'Рейтинг финального раунда (по количеству очков, по возрастанию)',
    finals, finalPtsMap, 'rating-finals', collapseAll, achievementsIndex
  );
  const superRatingSec = renderStageRating(
    'Рейтинг суперфинала (по количеству очков, по возрастанию)',
    superfinals, superFinalPtsMap, 'rating-superfinals', collapseAll, achievementsIndex
  );

  const groupsDefinedRatingSec = renderDefinedRating(
    'Рейтинг для выхода в финалы (установленный)',
    definedGroupRating, 'rating-groups-defined', collapseAll, achievementsIndex
  );
  const finalsDefinedRatingSec = renderDefinedRating(
    'Установленный рейтинг финального раунда',
    definedFinalRating, 'rating-finals-defined', collapseAll, achievementsIndex
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

  const achievementsAchSec = renderAchievementsSectionTitled('Ачивки', 'section-achievements', achievementsAch, collapseAll);
  const perksSec = renderAchievementsSectionTitled('Перки', 'section-perks', achievementsPerc, collapseAll);

  const tournamentNewsSecHtml = renderNewsList('Новости турнира', tournamentNews, collapseAll, 'section-news-tournament');

  // Новая секция «Статистика турнира» (по умолчанию свернута всегда, показывается если задан URL)
  const tournamentStatsSec = renderTournamentStatsSection(statsBaseUrl, containerClass, true);

  const streamsBottomSec = useQ2Css ? '' : renderStreamsOnly(tournament, containerClass);

  // Собираем главные секции в карту: id -> html (пустые не включаем при рендере)
  const sectionsMap = new Map([
    ['desc', descSection],
    ['extras', extrasTopSec],
    ['news-tournament', tournamentNewsSecHtml],
    ['groups', `
      <section class="mb-5">
        <details id="section-groups" class="stage-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">Квалификации</span>
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
    ['finals', `
      <section class="mb-5">
        <details id="section-finals" class="stage-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">Финальный раунд</span>
            <a href="#section-finals" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
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
    ['superfinals', `
      <section class="mb-5">
        <details id="section-superfinals" class="stage-collapse"${openAttr}>
          <summary class="qj-toggle">
            <span class="section-title">Суперфинал</span>
            <a href="#section-superfinals" class="qj-anchor ms-2 text-secondary text-decoration-none" aria-label="Ссылка на раздел">#</a>
            <span class="qj-badge ms-auto">${superfinals?.length || 0}</span>
          </summary>
          <div class="mt-2">
            ${superCards}
            ${superMapsRatingSec}
            ${superNewsSec}
            ${superRatingSec}
          </div>
        </details>
      </section>
    `],
    ['custom', customWholeSec],
    ['achievements', achievementsAchSec],
    ['perks', perksSec],
    ['stats', tournamentStatsSec],
    ['streams', streamsBottomSec],
  ]);

  const defaultOrder = [
    'desc', 'extras', 'news-tournament',
    'groups', 'finals', 'superfinals',
    'custom', 'achievements', 'perks',
    'stats', 'streams'
  ];

  // Итоговый порядок: сначала порядок из cookie, затем оставшиеся по умолчанию
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

  // Оборачиваем секции в draggable-контейнеры
  const sectionWrappers = order.map(id => {
    const inner = sectionsMap.get(id);
    if (!inner) return '';
    return `
      <div class="qj-section js-draggable-section" data-section-id="${escapeHtml(id)}" draggable="true">
        ${inner}
      </div>
    `;
  }).join('');

  // Базовые стили (включая мини‑иконки ачивок/перков и DnD)
  const baseUiCss = `
    body { background: #f8f9fa; }
    header.hero { background: #ffffff; border-bottom: 1px solid rgba(0,0,0,0.06); }
    .hero .title { font-weight: 800; letter-spacing: .2px; }

    .news-meta { white-space: normal; }
    @media (min-width: 768px) { .news-meta { white-space: nowrap; } }

    .hero-logo { max-height: 140px; width: auto; border-radius: 14px; border: 1px solid rgba(0,0,0,0.1); box-shadow: 0 6px 18px rgba(16,24,40,.06); }
    @media (max-width: 576px) { .hero-logo { max-height: 90px; } }
    @media (min-width: 1400px) { .hero-logo { max-height: 180px; } }

    .card { border-radius: 12px; }

    .cards-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 1.25rem; }
    @media (min-width: 1200px) { .cards-grid { grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); } }
    @media (min-width: 1920px) { .cards-grid { gap: 1.5rem; } }

    .cards-grid.cards-grid--ach { grid-template-columns: 1fr; }
    @media (min-width: 768px) { .cards-grid.cards-grid--ach { grid-template-columns: repeat(2, minmax(0, 1fr)); } }

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
    .player-modal-backdrop { position: absolute; inset: 0; background: rgba(0,0,0,.55); }
    .player-modal-dialog { position: relative; margin: 6vh auto; background: #fff; color: var(--bs-body-color); width: min(920px, calc(100vw - 24px)); max-height: 88vh; border-radius: 12px; overflow: hidden; box-shadow: 0 20px 60px rgba(0,0,0,.35); }
    .player-modal-header { display: flex; align-items: center; justify-content: space-between; padding: .75rem 1rem; border-bottom: 1px solid rgba(0,0,0,.08); position: sticky; top: 0; background: inherit; z-index: 1; }
    .player-modal-title { font-weight: 600; }
    .player-modal-body { height: calc(88vh - 52px); overflow: auto; }
    .player-modal-body iframe { display: block; width: 100%; height: 100%; border: 0; }

    .qj-accent { color: var(--bs-primary); }
    .qj-muted { color: var(--bs-secondary); }
    .qj-pts { color: var(--bs-danger-text-emphasis); }
    .qj-badge { display: inline-block; padding: .35em .6em; font-size: .75rem;
      background-color: var(--bs-secondary-bg-subtle); color: var(--bs-secondary-text-emphasis); border-radius: .375rem; }
    .qj-tag { display: inline-block; padding: .25rem .5rem; background-color: var(--bs-secondary-bg-subtle);
      color: var(--bs-secondary-text-emphasis); border-radius: 10rem; font-size: .8rem; border: 1px solid rgba(0,0,0,.05); }

    /* Заголовки секций + DnD хват */
    details > summary.qj-toggle {
      display: flex; align-items: center; gap: .5rem;
      padding: .7rem .95rem;
      border: 1px solid rgba(0,0,0,0.06);
      background: linear-gradient(180deg, #ffffff, #f7f9fb);
      border-radius: 14px;
      box-shadow: 0 2px 8px rgba(16,24,40,.04);
      cursor: grab; user-select: none;
      transition: background .2s ease, box-shadow .2s ease, border-color .2s ease, transform .1s ease;
      touch-action: none;
    }
    details > summary.qj-toggle:active { cursor: grabbing; }
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

    /* Мини‑иконки ачивок/перков рядом с именами */
    .ach-badges { display: inline-flex; align-items: center; gap: .25rem; }
    .ach-badge-img {
      width: 42px; height: 42px; object-fit: contain;
      border-radius: 6px; border: 1px solid rgba(0,0,0,.15);
      vertical-align: middle;
    }
    .perc-badge-img {
      width: 42px; height: 42px; object-fit: contain;
      border-radius: 50%; border: 1px solid rgba(0,0,0,.15);
      vertical-align: middle;
    }
    .ach-badge-link, .perc-badge-link { position: relative; display: inline-block; }
    .ach-badge-link { border-radius: 6px; filter: drop-shadow(0 0 2px rgba(255,77,109,.55)) drop-shadow(0 0 6px rgba(255,153,172,.35)); }
    .perc-badge-link { border-radius: 50%; filter: drop-shadow(0 0 2px rgba(96,165,250,.55)) drop-shadow(0 0 6px rgba(34,211,238,.35)); }
    .ach-badge-link::before, .perc-badge-link::before {
      content: ""; position: absolute; inset: -1.25px; padding: 1.25px; border-radius: inherit; pointer-events: none; opacity: .85;
      -webkit-mask: linear-gradient(#000 0 0) content-box, linear-gradient(#000 0 0);
      -webkit-mask-composite: xor; mask-composite: exclude; animation: glowPulse 2.2s ease-in-out infinite;
    }
    .ach-badge-link::before { background: conic-gradient(#ff4d6d, #ff99ac, #ff4d6d); }
    .perc-badge-link::before { background: conic-gradient(#60a5fa, #22d3ee, #60a5fa); }
    .ach-badges img { transition: transform .15s ease, box-shadow .15s ease; transform-origin: center center; }
    .ach-badges img:hover { transform: scale(4); z-index: 10; position: relative; box-shadow: 0 6px 18px rgba(0,0,0,.2); }
    .ach-badge-fallback { font-size: .9em; }
    @keyframes glowPulse { 0%,100%{opacity:.65;} 50%{opacity:1;} }

    /* Крупные превью ачивок внутри секций */
    .ach-thumb {
      width: 165px; height: 165px; object-fit: contain;
      border-radius: 6px; border: 1px solid rgba(0,0,0,.1);
      box-shadow: 0 6px 18px rgba(16,24,40,.06);
    }

    /* Списки игроков */
    .players { margin: 0; padding: 0; }
    .players li { display: flex; align-items: center; gap: .5rem; padding: .35rem 0; margin: 0; line-height: 1.25; }
    .players li + li { border-top: 1px solid rgba(0,0,0,.08); }
    .player-pos { display: inline-block; min-width: 1.75rem; text-align: right; font-variant-numeric: tabular-nums; color: var(--bs-secondary-color); }
    .player-name { letter-spacing: .2px; }
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

    /* DnD секции */
    .qj-section { margin-bottom: 2rem; }
    .qj-sections-root .qj-section.dragging { opacity: .6; }
  `;

  // Современная тема (градиенты по типам секций)
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
      border-left: 4px solid var(--tone-perк-edge);
    }

    body:not(.q2css-active) { background: linear-gradient(180deg, #f7f9fc, #f3f6fb); }
    body:not(.q2css-active) header.hero { background: transparent; border-bottom: 0; }
  ` : '';

  // Ретро‑режим (Q2CSS) — оверрайды
  const q2OverridesCss = `
    body.q2css-active details > summary.qj-toggle {
      border-radius: 0 !important; background: #FAD3BC !important; border: 1px solid #000 !important;
      box-shadow: none !important; padding: 6px 8px !important; cursor: grab;
    }
    body.q2css-active details > summary.qj-toggle .section-title {
      color: #000 !important; font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 12px;
    }
    body.q2css-active details > summary.qj-toggle::after { color: #3D3D3D !important; }
    body.q2css-active .qj-badge {
      background: #FEF1DE; color: #000; border: 1px solid #000; border-radius: 0;
      font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px;
    }
    body.q2css-active .qj-tag { background: #FEF1DE; color: #A22C21; border: 1px solid #000; border-radius: 0;
      font-family: Verdana, Geneva, Arial, Helvetica, sans-serif; font-size: 11px; }

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

    body.q2css-active .ach-badge-img { border: 1px solid #000; border-radius: 0; width: 42px; height: 42px; object-fit: contain; }
    body.q2css-active .perc-badge-img { border: 1px solid #000; border-radius: 50%; width: 42px; height: 42px; object-fit: contain; }
    body.q2css-active .ach-thumb { border: 1px solid #000; border-radius: 0; }
    body.q2css-active .ach-badges img:hover { box-shadow: none; }

    html.q2css-active, body.q2css-active { font-size: 14px !important; }
    body.q2css-active * { font-size: inherit !important; }
  `;

  // Фон modern-темы (SITE_BG_IMAGE)
  const animatedBgCss = !useQ2Css ? `
    body:not(.q2css-active) {
      background-color: #0b0d10 !important;
      background-image: url('${escapeHtml(SITE_BG_IMAGE)}') !important;
      background-position: center center;
      background-repeat: no-repeat;
      background-size: cover;
      background-attachment: fixed;
    }
    body:not(.q2css-active) header.hero {
      background-color: transparent !important;
      background: transparent !important;
      border-bottom: 0 !important;
    }
    @media (prefers-reduced-motion: reduce) {
      body:not(.q2css-active) {
        background-image: none !important;
        background-color: #f5f7fa !important;
      }
    }
  ` : '';

  const q2BtnClass = useQ2Css ? 'btn btn-sm btn-primary' : 'btn btn-sm btn-outline-primary';
  const collBtnClass = collapseAll ? 'btn btn-sm btn-primary' : 'btn btn-sm btn-outline-primary';
  const resetBtnClass = 'btn btn-sm btn-outline-secondary';

  return `<!doctype html>
<html lang="ru" data-bs-theme="auto" class="${useQ2Css ? 'q2css-active' : ''}">
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
<body class="${useQ2Css ? 'q2css-active' : ''}">
  <header class="hero py-3 ${useQ2Css ? 'head_image' : ''}">
    <div class="${containerClass}">
      <!-- Мобильная шапка -->
      <div class="d-flex d-md-none align-items-start">
        ${logoBlock}
        <div class="ms-3 flex-grow-1">
          <div class="d-flex justify-content-end gap-2 mb-2">
            <button type="button" class="js-btn-toggle-q2 ${q2BtnClass}" title="Переключить Q2CSS">Q2CSS</button>
            <button type="button" class="js-btn-toggle-collapse ${collBtnClass}" title="Свернуть/раскрыть все">Свернуть все</button>
            <button type="button" class="js-btn-reset-sections ${resetBtnClass}" title="Вернуть порядок разделов по умолчанию">Вернуть порядок</button>
          </div>
          <div class="d-flex flex-column align-items-start">
            <h1 class="title h5 my-0">${escapeHtml(tournament.name || 'Турнир')}</h1>
            ${siteLink ? `<div class="site-link mt-1">${siteLink}</div>` : ''}
          </div>
        </div>
      </div>

      <!-- Десктопная шапка -->
      <div class="d-none d-md-flex align-items-start">
        ${logoBlock}
        <div class="flex-grow-1">
          <div class="d-flex justify-content-end gap-2 mb-2">
            <button type="button" class="js-btn-toggle-q2 ${q2BtnClass}" title="Переключить Q2CSS">Q2CSS</button>
            <button type="button" class="js-btn-toggle-collapse ${collBtnClass}" title="Свернуть/раскрыть все">Свернуть все</button>
            <button type="button" class="js-btn-reset-sections ${resetBtnClass}" title="Вернуть порядок разделов по умолчанию">Вернуть порядок</button>
          </div>
          <div class="d-flex flex-column align-items-start">
            <h1 class="title h3 my-0">${escapeHtml(tournament.name || 'Турнир')}</h1>
            ${siteLink ? `<div class="site-link mt-1">${siteLink}</div>` : ''}
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

  <footer class="py-4">
    <div class="${containerClass} text-center text-muted small">
      Работает на QuakeJourney Bot — ${new Date().getFullYear()}
    </div>
  </footer>

  <script>
    (function(){
      // Lightbox
      const lb = document.getElementById('lightbox');
      const img = lb.querySelector('.lightbox-img');
      const backdrop = lb.querySelector('.lightbox-backdrop');
      let lbOpen = false, lastCloseAt = 0;

      function resetImgInlineStyles() {
        img.style.opacity = '';
        img.style.left = '';
        img.style.top = '';
        img.style.width = '';
        img.style.height = '';
        img.style.position = '';
      }
      function openLb(src) {
        if (!src || lbOpen) return;
        resetImgInlineStyles();
        img.removeAttribute('width');
        img.removeAttribute('height');
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
        if (Date.now() - lastCloseAt < 250) {
          e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.(); return;
        }
        e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation?.();
        openLb(trg.getAttribute('data-src'));
      });
      backdrop.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); closeLb(); });
      img.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); closeLb(); });
      document.addEventListener('keydown', (e) => { if (lbOpen && (e.key === 'Escape' || e.key === 'Esc')) closeLb(); });

      // Twitch embeds (parent=hostname)
      const hostname = location.hostname;
      document.querySelectorAll('iframe.js-twitch-embed[data-channel]').forEach(ifr => {
        const ch = ifr.getAttribute('data-channel'); if (!ch) return;
        const src = 'https://player.twitch.tv/?channel=' + encodeURIComponent(ch) + '&parent=' + encodeURIComponent(hostname) + '&muted=true';
        ifr.src = src; ifr.setAttribute('allow', 'autoplay; picture-in-picture; fullscreen');
      });

      // Авто-раскрытие по якорю
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
        setTimeout(() => { document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' }); }, 0);
      }
      openDetailsForHash();
      window.addEventListener('hashchange', openDetailsForHash);

      // Player stats modal
      const statsEnabled = ${PLAYER_STATS_ENABLED ? 'true' : 'false'};
      const statsBase = ${JSON.stringify(statsBaseUrl || '')};
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

      // Переключатели Q2CSS и CollapseAll (сервер выставит cookies)
      const isQ2Css = ${useQ2Css ? 'true' : 'false'};
      const isCollapsed = ${collapseAll ? 'true' : 'false'};
      const Q2_PARAM = ${JSON.stringify(FORCE_Q2CSS_PARAM)};
      const COLLAPSE_PARAM = ${JSON.stringify(COLLAPSE_ALL_PARAM)};
      function toggleParam(name, current) {
        const url = new URL(location.href);
        url.searchParams.set(name, current ? '0' : '1');
        location.href = url.toString();
      }
      document.querySelectorAll('.js-btn-toggle-q2').forEach(btn => btn.addEventListener('click', () => toggleParam(Q2_PARAM, isQ2Css)));
      document.querySelectorAll('.js-btn-toggle-collapse').forEach(btn => btn.addEventListener('click', () => toggleParam(COLLAPSE_PARAM, isCollapsed)));

      // Сброс порядка секций к умолчанию (удалить cookie и перезагрузить)
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

      // DnD reorder главных секций (desktop HTML5 DnD + mobile Pointer Events)
      (function(){
        const root = document.getElementById('sections-root');
        if (!root) return;
        const COOKIE_NAME = ${JSON.stringify(SECTIONS_COOKIE)};
        const ONE_YEAR = 60*60*24*365;

        function saveOrder() {
          const ids = Array.from(root.querySelectorAll('.js-draggable-section'))
            .map(el => el.getAttribute('data-section-id'))
            .filter(Boolean);
          document.cookie = encodeURIComponent(COOKIE_NAME) + '=' + encodeURIComponent(ids.join(','))
            + '; Max-Age=' + ONE_YEAR + '; Path=/; SameSite=Lax';
        }

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
          const sec = e.target.closest('.js-draggable-section'); if (!sec) return;
          dragging = sec; sec.classList.add('dragging');
          try { e.dataTransfer.effectAllowed = 'move'; e.dataTransfer.setData('text/plain', sec.dataset.sectionId || ''); } catch(_) {}
        });
        root.addEventListener('dragover', (e) => {
          if (!dragging) return; e.preventDefault();
          const afterEl = getDragAfterElement(root, e.clientY);
          if (afterEl == null) root.appendChild(dragging);
          else root.insertBefore(dragging, afterEl);
        });
        root.addEventListener('drop', (e) => { if (!dragging) return; e.preventDefault(); dragging.classList.remove('dragging'); dragging = null; saveOrder(); });
        root.addEventListener('dragend', () => { if (dragging) dragging.classList.remove('dragging'); dragging = null; saveOrder(); });

        // Mobile Pointer DnD — перетаскиваем за summary.qj-toggle
        let pDragging = null, started = false, startY = 0, suppressClick = false;

        root.addEventListener('pointerdown', (e) => {
          const sum = e.target.closest('summary.qj-toggle'); if (!sum) return;
          const sec = sum.closest('.js-draggable-section'); if (!sec) return;
          pDragging = sec; startY = e.clientY; started = false; suppressClick = false;
        }, { passive: true });

        root.addEventListener('pointermove', (e) => {
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

        // Глушим «клик» по summary после drag (чтобы не сворачивалось/раскрывалось)
        root.addEventListener('click', (e) => {
          if (!suppressClick) return;
          if (e.target.closest('summary.qj-toggle')) {
            e.preventDefault(); e.stopPropagation();
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

  const app = express();

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
  
      const [
        tournament, groups, finals, superfinals,
        groupPtsMap, finalPtsMap, superFinalPtsMap
      ] = await Promise.all([
        getTournament(CHAT_ID),
        getGroups(CHAT_ID),
        getFinals(CHAT_ID),
        getSuperfinals(CHAT_ID),
        getGroupPointsMap(CHAT_ID),
        getFinalPointsMap(CHAT_ID),
        getSuperFinalPointsMap(CHAT_ID),
      ]);
  
      const [groupScreens, finalScreens, superScreens] = await Promise.all([
        getScreensForScope(CHAT_ID, 'group', groups),
        getScreensForScope(CHAT_ID, 'final', finals),
        getScreensForScope(CHAT_ID, 'superfinal', superfinals),
      ]);
  
      const [groupRunId, finalRunId, superRunId] = await Promise.all([
        findLatestRunIdForScope(CHAT_ID, 'group'),
        findLatestRunIdForScope(CHAT_ID, 'final'),
        findLatestRunIdForScope(CHAT_ID, 'superfinal'),
      ]);
  
      const [tournamentNews, groupsNews, finalsNews, superNews] = await Promise.all([
        listNews(CHAT_ID, 'tournament', null),
        groupRunId ? listNews(CHAT_ID, 'group', groupRunId) : Promise.resolve([]),
        finalRunId ? listNews(CHAT_ID, 'final', finalRunId) : Promise.resolve([]),
        superRunId ? listNews(CHAT_ID, 'superfinal', superRunId) : Promise.resolve([]),
      ]);
  
      const [definedGroupRating, definedFinalRating] = await Promise.all([
        getDefinedGroupRating(CHAT_ID),
        getDefinedFinalRating(CHAT_ID),
      ]);
  
      const [customGroups, customPointsByGroup] = await Promise.all([
        getCustomGroups(CHAT_ID),
        getCustomPointsByGroup(CHAT_ID),
      ]);
      const customScreens = await getScreensForScope(CHAT_ID, 'custom', customGroups);
  
      const achievements = await getAchievements(CHAT_ID);
      const achievementsAch = achievements.filter(a => String(a?.type || 'achievement').toLowerCase() === 'achievement');
      const achievementsPerc = achievements.filter(a => String(a?.type || 'achievement').toLowerCase() === 'perc');
      const achievementsIndex = buildAchievementsIndex(achievements);
  
      const mapsList = await getMaps(CHAT_ID);
  
      const html = renderPage({
        tournament, groups, finals, superfinals,
        groupScreens, finalScreens, superScreens,
        groupPtsMap, finalPtsMap, superFinalPtsMap,
        tournamentNews, groupsNews, finalsNews, superNews,
        useQ2Css,
        collapseAll,
        definedGroupRating,
        definedFinalRating,
        customGroups,
        customPointsByGroup,
        customScreens,
        achievementsAch,
        achievementsPerc,
        achievementsIndex,
        statsBaseUrl: PLAYER_STATS_URL,
        mapsList,
        sectionOrder: sectionsOrder, // НОВОЕ
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
  
  
  // Healthcheck
  const server = app.listen(PORT, () => {
    console.log(`Site started on http://localhost:${PORT} (chatId=${CHAT_ID})`);
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
