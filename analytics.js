// analytics.js
require('dotenv').config();

const fs = require('fs');
const path = require('path');

const { MongoClient } = require('mongodb');

const MONGODB_URI = process.env.MONGODB_URI || 'BACKUP_REDACTED_SET_LOCALLY';

// SITE_CHAT_ID=-4961062249,350920766,-5094364912
const SITE_CHAT_ID = (process.env.SITE_CHAT_ID || '')
  .split(',')
  .map(s => s.trim())
  .filter(Boolean)
  .map(v => Number(v))
  .filter(v => !Number.isNaN(v));


// ===== Nick aggregation (.env.nicks) =====
// Формат файла .env.nicks (UTF-8):
// CanonNick=alias1,alias2,alias3
// Пример:
// NekiyRanger=NekiyRanger,NekoRanger01
let __NICK_MAP_CACHE__ = { mtimeMs: 0, map: {}, aliases: {} };

// Returns { map: {lowercaseAlias → canonicalName}, aliases: {canonicalName → [originalRhsAliases]} }
function loadNickData() {
  try {
    const filePath = path.join(process.cwd(), '.env.nicks');
    const st = fs.statSync(filePath);
    if (__NICK_MAP_CACHE__.mtimeMs === st.mtimeMs) {
      return __NICK_MAP_CACHE__;
    }

    const raw = fs.readFileSync(filePath, 'utf8');
    const map     = Object.create(null); // lowercase alias → canonical name
    const aliases = Object.create(null); // canonical name → [original-case RHS aliases]

    raw.split(/\r?\n/).forEach(line => {
      const s = String(line || '').trim();
      if (!s || s.startsWith('#') || s.startsWith(';')) return;

      const eq = s.indexOf('=');
      if (eq <= 0) return;

      const canon = s.slice(0, eq).trim();
      const rhs = s.slice(eq + 1).trim();
      if (!canon) return;

      const aliasList = rhs
        ? rhs.split(',').map(x => x.trim()).filter(Boolean)
        : [];

      // Always map canonical to itself
      map[String(canon).toLowerCase()] = canon;

      for (const a of aliasList) {
        map[String(a).toLowerCase()] = canon;
      }

      // Store original-case RHS aliases (used for tooltip display)
      if (aliasList.length > 0) {
        aliases[canon] = aliasList;
      }
    });

    __NICK_MAP_CACHE__ = { mtimeMs: st.mtimeMs, map, aliases };
    return __NICK_MAP_CACHE__;
  } catch (e) {
    // File may be absent — treat as empty
    __NICK_MAP_CACHE__ = { mtimeMs: 0, map: {}, aliases: {} };
    return __NICK_MAP_CACHE__;
  }
}




if (!SITE_CHAT_ID.length) {
  console.warn('[analytics] WARNING: SITE_CHAT_ID is empty or invalid. No chats will be available for analytics.');
}

let db;
let client;

// Инициализация подключения к MongoDB один раз на модуль
const dbReady = (async () => {
  try {
    client = new MongoClient(MONGODB_URI);
    await client.connect();
    db = client.db();
    console.log('[analytics] Connected to MongoDB');
  } catch (err) {
    console.error('[analytics] Failed to connect to MongoDB:', err);
    throw err;
  }
})();

// Хелпер: HTML-экранирование < в JSON, чтобы не ломать <script>
function safeJson(obj) {
  return JSON.stringify(obj).replace(/</g, '\\u003c');
}

// Основная функция: навешивает маршруты аналитики на существующий app
function attachAnalyticsRoutes(app) {
  console.log('[analytics] attachAnalyticsRoutes called');

  // Страница аналитики: GET /analytics
  app.get('/analytics', async (req, res) => {
    console.log('[analytics] GET /analytics', req.query);
    try {
      await dbReady;
      if (!db) {
        res.status(500).send('DB not initialized');
        return;
      }

      const chatsCol = db.collection('chats');
      const groupResultsCol = db.collection('group_results');
      const finalResultsCol = db.collection('final_results');
      const superfinalResultsCol = db.collection('superfinal_results');

      // Сначала загружаем *родительские* турниры по chatId из SITE_CHAT_ID
      const parentChats = await chatsCol
        .find({ chatId: { $in: SITE_CHAT_ID } })
        .sort({ tournamentName: 1 })
        .toArray();

      // Если вдруг в БД нет ни одного совпадения
      if (!parentChats.length) {
        res.status(200).send('<h1>No tournaments found for SITE_CHAT_ID</h1>');
        return;
      }

      // Собираем все subTournaments, чтобы подтянуть их имена (они тоже лежат в chats)
      const allSubIds = Array.from(new Set(
        parentChats
          .flatMap(c => Array.isArray(c.subTournaments) ? c.subTournaments : [])
          .filter(v => Number.isFinite(Number(v)))
          .map(v => Number(v))
      ));

      const subChats = allSubIds.length
        ? await chatsCol.find({ chatId: { $in: allSubIds } }).sort({ tournamentName: 1 }).toArray()
        : [];

      const subNameById = new Map(subChats.map(c => [c.chatId, c.tournamentName || String(c.chatId)]));
      const subCodeById = new Map(subChats.map(c => [c.chatId, c.tournamentSubCode || '']));

      // ТУРНИР / ПОД-ТУРНИР:
      // - tournamentId (или старый chatId) определяет выбранный *родительский* турнир или режим "Все турниры"
      // - SubID (SubId/subId/subID) определяет выбранный под-турнир (дочерний chatId)
      const rawTournamentParam = (req.query.tournamentId !== undefined)
        ? req.query.tournamentId
        : req.query.chatId;

      const rawSubParam =
        (req.query.SubID !== undefined) ? req.query.SubID :
        (req.query.SubId !== undefined) ? req.query.SubId :
        (req.query.subID !== undefined) ? req.query.subID :
        (req.query.subId !== undefined) ? req.query.subId :
        (req.query.subid !== undefined) ? req.query.subid :
        undefined;
      const isAllMode = (typeof rawTournamentParam === 'string') && rawTournamentParam.toLowerCase() === 'all';

      // Nick aggregation: load from .env.nicks; merge names only in "all tournaments" mode
      // nick_agr=0 явно отключает объединение (чекбокс "Объединить ники")
      const nickData = loadNickData();
      const nickMap = nickData.map;
      const nickAliases = nickData.aliases; // canonical → [original RHS aliases] for tooltips
      const nickAgrEnabled = isAllMode && Object.keys(nickMap).length > 0 && req.query.nick_agr !== '0';

      const parsedTournamentId = (!isAllMode && rawTournamentParam !== undefined && rawTournamentParam !== null && rawTournamentParam !== '')
        ? Number(rawTournamentParam)
        : null;

      const parsedSubId = (!isAllMode && rawSubParam !== undefined && rawSubParam !== null && rawSubParam !== '')
        ? Number(rawSubParam)
        : null;

      // Определяем родительский турнир
      let currentParentId = null;
      let currentSubId = null;

      const allSubSet = new Set(allSubIds);
      const parentBySubId = new Map();
      for (const p of parentChats) {
        const subs = Array.isArray(p.subTournaments) ? p.subTournaments : [];
        for (const sid of subs) parentBySubId.set(Number(sid), p.chatId);
      }

      if (isAllMode) {
        currentParentId = 'all';
      } else if (Number.isFinite(parsedTournamentId) && SITE_CHAT_ID.includes(parsedTournamentId)) {
        currentParentId = parsedTournamentId;
      } else if (Number.isFinite(parsedTournamentId) && allSubSet.has(parsedTournamentId)) {
        // совместимость: если в tournamentId прилетел дочерний chatId — поднимаем его до родителя
        currentParentId = parentBySubId.get(parsedTournamentId) || SITE_CHAT_ID[0];
        currentSubId = parsedTournamentId;
      } else {
        // если параметр невалиден / не разрешён — берём первый из SITE_CHAT_ID
        currentParentId = SITE_CHAT_ID[0];
      }

      // Текущий родительский турнир (в режиме all — null)
      const currentParentChat = (currentParentId !== 'all')
        ? (parentChats.find(c => c.chatId === currentParentId) || parentChats[0])
        : null;

      if (currentParentChat && currentParentChat.chatId !== currentParentId) {
        currentParentId = currentParentChat.chatId;
      }

      // Проверяем SubID: разрешаем только если он входит в subTournaments текущего родителя
      if (!isAllMode && Number.isFinite(parsedSubId) && currentParentChat) {
        const allowedSubs = Array.isArray(currentParentChat.subTournaments) ? currentParentChat.subTournaments.map(Number) : [];
        if (allowedSubs.includes(parsedSubId)) {
          currentSubId = parsedSubId;
        }
      }

      // Какая аналитика реально строится (эффективный chatId или "все")
      const effectiveChatId = (currentParentId === 'all')
        ? null
        : (Number.isFinite(currentSubId) ? currentSubId : currentParentId);

      // Данные по группам/финалам/суперфиналам
      const [groupResults, finalResults, superfinalResults] = await Promise.all([
        (effectiveChatId === null)
          ? groupResultsCol.find({ chatId: { $in: Array.from(new Set([...SITE_CHAT_ID, ...allSubIds])) } }).toArray()
          : groupResultsCol.find({ chatId: effectiveChatId }).toArray(),

        (effectiveChatId === null)
          ? finalResultsCol.find({ chatId: { $in: Array.from(new Set([...SITE_CHAT_ID, ...allSubIds])) } }).toArray()
          : finalResultsCol.find({ chatId: effectiveChatId }).toArray(),

        (effectiveChatId === null)
          ? superfinalResultsCol.find({ chatId: { $in: Array.from(new Set([...SITE_CHAT_ID, ...allSubIds])) } }).toArray()
          : superfinalResultsCol.find({ chatId: effectiveChatId }).toArray(),
      ]);

      const initialData = {
        allMode: isAllMode,
        chats: parentChats.map(c => ({
          chatId: c.chatId,
          tournamentName: c.tournamentName,
          subTournaments: Array.isArray(c.subTournaments) ? c.subTournaments.map(Number) : [],
        })),
        subChats: subChats.map(c => ({
          chatId: c.chatId,
          tournamentName: c.tournamentName,
          tournamentSubCode: c.tournamentSubCode || '',
        })),
        currentChatId: currentParentId,                 // number или 'all'
        currentSubId: Number.isFinite(currentSubId) ? currentSubId : null,
        effectiveChatId: effectiveChatId,               // number или null (all)
        nickAgrEnabled: !!nickAgrEnabled,
        nickMap: nickMap,
        nickAliases: nickAliases,

        groupResults,
        finalResults,
        superfinalResults,
      };

      const html = `<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <title>Турнирная аналитика</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    html {
      scroll-behavior: smooth;
    }
    body {
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      padding: 0;
      background: #0b1020;
      color: #f0f0f0;
    }
    header {
      position: sticky;
      top: 0;
      z-index: 10;
      background: #141b33;
      padding: 10px 16px;
      display: flex;
      flex-direction: column;
      gap: 6px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.5);
    }
    .header-top {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
    }
    header h1 {
      font-size: 18px;
      margin: 0;
      margin-right: 16px;
      white-space: nowrap;
    }
    header label {
      font-size: 14px;
      margin-right: 8px;
    }
    header select {
      padding: 4px 8px;
      border-radius: 4px;
      border: 1px solid #444;
      background: #1e2640;
      color: #fff;
    }
    .header-metrics-note {
      font-size: 12px;
      opacity: 0.7;
    }
    .header-toggles {
      margin-left: auto;
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .header-toggle {
      display: flex;
      align-items: center;
      gap: 4px;
      font-size: 13px;
      white-space: nowrap;
    }
    .header-toggle input {
      cursor: pointer;
      accent-color: #8ab4ff;
    }
    .main-nav {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 2px;
    }
    .main-nav a {
      font-size: 13px;
      padding: 4px 10px;
      border-radius: 999px;
      background: #1e2640;
      color: #ffffff;
      text-decoration: none;
      border: 1px solid #2a335a;
      transition: background 0.15s ease, transform 0.1s ease, box-shadow 0.15s ease;
    }
    .main-nav a:hover {
      background: #263059;
      transform: translateY(-1px);
      box-shadow: 0 2px 4px rgba(0,0,0,0.4);
    }
    .main-nav a:active {
      transform: translateY(0);
      box-shadow: none;
    }
    .main-nav a.active {
      background: #3b4b92;
      box-shadow: 0 0 0 1px rgba(138,180,255,0.8);
    }
    main {
      padding: 16px;
      max-width: 1400px;
      margin: 0 auto;
    }
    body.layout-fullwidth main {
      max-width: 100%;
    }
    section {
      margin-bottom: 32px;
      padding: 16px;
      border-radius: 8px;
      background: rgba(20, 27, 51, 0.9);
      box-shadow: 0 0 10px rgba(0,0,0,0.4);
    }
    section h2 {
      margin-top: 0;
      font-size: 20px;
      margin-bottom: 8px;
    }
    section h3 {
      margin-top: 16px;
      margin-bottom: 8px;
      font-size: 18px;
      color: #8ab4ff;
    }
    section h4 {
      margin-top: 12px;
      margin-bottom: 4px;
      font-size: 15px;
      color: #c8d3ff;
    }
    .chart-container {
      position: relative;
      width: 100%;
      max-width: 100%;
      height: 320px;
      margin-bottom: 12px;
    }
    canvas {
      width: 100% !important;
      height: 100% !important;
    }
    .metric-note {
      font-size: 12px;
      opacity: 0.8;
      margin-bottom: 8px;
    }
    .group-wrapper {
      border-top: 1px solid rgba(255,255,255,0.1);
      padding-top: 12px;
      margin-top: 12px;
    }
    .empty-note {
      font-size: 14px;
      opacity: 0.8;
      font-style: italic;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
      font-size: 13px;
    }
    th, td {
      border: 1px solid rgba(255,255,255,0.1);
      padding: 6px 8px;
      text-align: left;
    }
    th {
      background: rgba(255,255,255,0.05);
    }
    tbody tr:nth-child(odd) {
      background: rgba(255,255,255,0.02);
    }
    .controls-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
      margin-bottom: 8px;
    }
  </style>
  <!-- Chart.js -->
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
  <header>
    <div class="header-top">
      <h1>Турнирная аналитика</h1>

      <label for="chat-select">Турнир:</label>
      <select id="chat-select">
        <option value="all" ${initialData.allMode ? 'selected' : ''}>Все турниры</option>
        ${initialData.chats
          .map(c => `<option value="${c.chatId}" ${(!initialData.allMode && c.chatId === initialData.currentChatId) ? 'selected' : ''}>${c.tournamentName}</option>`)
          .join('')}
      </select>

      ${(() => {
        if (initialData.allMode) return '';
        const parent = initialData.chats.find(c => c.chatId === initialData.currentChatId);
        if (!parent || !parent.subTournaments || !parent.subTournaments.length) return '';
        const subName = new Map(initialData.subChats.map(sc => [sc.chatId, sc.tournamentName || String(sc.chatId)]));
        const opts = [
          `<option value="" ${initialData.currentSubId ? '' : 'selected'}>(Основной)</option>`,
          ...parent.subTournaments.map(id => `<option value="${id}" ${(id === initialData.currentSubId) ? 'selected' : ''}>${subName.get(id) || id}</option>`)
        ].join('');
        return (
          '<label for="sub-select">Под-турнир:</label>' +
          '<select id="sub-select">' + opts + '</select>'
        );
      })()}

      <span class="header-metrics-note">
        Метрики: Frags, Deaths, Efficiency (avg), FPH (avg) и другие
      </span>

      
      <div class="header-toggles">
        <label class="header-toggle" title="Если включено — объединять разные ники одного игрока по файлу .env.nicks">
          <input type="checkbox" id="nick-agr-toggle" />
          Объединить ники
        </label>

        <label class="header-toggle" title="Растянуть контент на всю ширину окна">
          <input type="checkbox" id="fullwidth-toggle" />
          На всю ширину
        </label>
      </div>

    </div>

    <nav class="main-nav">
      <a href="#section-group-results">Квалификации</a>
      <a href="#section-final-results">Финалы</a>
      <a href="#section-superfinal-results">Суперфиналы</a>
      <a href="#section-overall">Общие графики</a>
      <a href="#section-player-profiles">Профили игроков</a>
      <a href="#section-map-stats">Карты</a>
      <a href="#section-player-form">Форма игроков</a>
      <a href="#section-stage-comparison">Стадии</a>
      <a href="#section-science-rating">Научный рейтинг</a>
      <a href="#section-headtohead">Личные встречи</a>
      <a href="#section-win-streaks">Серии побед</a>
    </nav>
  </header>

  <main>
    <section id="section-group-results">
      <h2>Квалификационные группы (group_results)</h2>
      <div class="metric-note">Отдельно по каждой группе (groupId), сравнительные графики по игрокам внутри группы.</div>
      <div id="group-results-content"></div>
    </section>

    <section id="section-final-results">
      <h2>Финальные группы (final_results)</h2>
      <div class="metric-note">По каждой финальной группе (groupId), те же показатели.</div>
      <div id="final-results-content"></div>
    </section>

    <section id="section-superfinal-results">
      <h2>Суперфинальные группы (superfinal_results)</h2>
      <div class="metric-note">По каждой суперфинальной группе (groupId), те же показатели.</div>
      <div id="superfinal-results-content"></div>
    </section>

    <section id="section-overall">
      <h2>Общие графики по всем стадиям</h2>
      <div class="metric-note">
        Аггрегированные показатели по всем трём таблицам (group_results + final_results + superfinal_results),
        без учёта групп. Игроки определяются по совпадению имени (nameOrig).
      </div>
      <div id="overall-content"></div>
    </section>

    <section id="section-player-profiles">
      <h2>Профили игроков и расширенная статистика</h2>
      <div class="metric-note">
        Суммарные фраги, урон, F/D, эффективность конверсии урона во фраги и индексы стабильности.
      </div>
      <div id="player-profiles-table"></div>

      <h3>Детальный профиль игрока</h3>
      <div class="metric-note">
        Радар-диаграмма по ключевым метрикам: Frags, F/D, Efficiency, FPH (и Net Dmg при наличии данных), отн. к максимумам по турниру.
      </div>
      <div id="player-profile-detail"></div>
    </section>

    <section id="section-map-stats">
      <h2>Аналитика по картам</h2>
      <div class="metric-note">
        Нагрузка карты (total frags, при наличии данных — dmg), средние значения на игрока и любимые/сложные карты для выбранного игрока.
      </div>
      <div id="map-global-table"></div>

      <h3>Любимые и сложные карты игрока</h3>
      <div id="map-player-detail"></div>
    </section>

    <section id="section-player-form">
      <h2>Форма игроков по ходу турнира</h2>
      <div class="metric-note">
        Динамика фрагов и эффективности игрока по времени (сортировка по дате матча).
      </div>
      <div id="player-form-controls"></div>
      <div id="player-form-chart"></div>
    </section>

    <section id="section-stage-comparison">
      <h2>Сравнение игры по стадиям (Квалы / Финалы / Суперфинал)</h2>
      <div class="metric-note">
        Средние фраги и эффективность по стадиям: кто прогрессировал к финалу, а кто просел.
      </div>
      <div id="stage-comparison-table"></div>

      <h3>График по стадиям для игрока</h3>
      <div id="stage-comparison-chart"></div>
    </section>

    <section id="section-science-rating">
      <h2>Научный рейтинг игроков и корреляции метрик</h2>
      <div class="metric-note">
        Рейтинг на основе z-score по Frags и Efficiency (при наличии данных — также Net Damage), плюс корреляции между ключевыми показателями.
      </div>
      <div id="science-rating-content"></div>

      <h3>Корреляции метрик</h3>
      <div id="correlations-content"></div>
    </section>

    <section id="section-headtohead">
      <h2>Личные противостояния (head-to-head)</h2>
      <div class="metric-note">
        Сравнение двух игроков: кто чаще выигрывал карты, средняя разница фрагов и список общих матчей.
      </div>
      <div id="headtohead-controls"></div>
      <div id="headtohead-content"></div>
    </section>

    <section id="section-win-streaks">
      <h2>Серии из 3+ побед подряд</h2>
      <div class="metric-note">
        Учитываются серии, где игрок в рамках одной группы и одной стадии (Квалификации / Финалы / Суперфинал)
        занимает первое место на карте 3 и более раз подряд (по времени matchDateTime).
      </div>
      <div id="win-streaks-content"></div>

      <h3>Сквозные серии из 3+ побед подряд</h3>
      <div class="metric-note">
        Сквозные серии: победы подряд, считая последовательно игры игрока от Квалификаций через Финалы до Суперфинала,
        без обнуления на переходах стадий.
      </div>
      <div id="cross-win-streaks-content"></div>
    </section>
  </main>

  <script>
    // Начальные данные с сервера
    window.__INITIAL_DATA__ = ${safeJson(initialData)};

    const METRICS = [
      { key: 'frags', label: 'Frags', agg: 'sum', sourceKey: 'frags' },
      { key: 'kills', label: 'Deaths', agg: 'sum', sourceKey: 'kills' },
      { key: 'eff',   label: 'Efficiency', agg: 'avg', sourceKey: 'eff' },
      { key: 'fph',   label: 'FPH', agg: 'avg', sourceKey: 'fph' },
      { key: 'dgiv',  label: 'Damage Given', agg: 'sum', sourceKey: 'dgiv' },
      { key: 'drec',  label: 'Damage Received', agg: 'sum', sourceKey: 'drec' },
    ];

    const STAGE_META = [
      { key: 'group',      label: 'Квалификации', shortLabel: 'Квалы',      order: 1 },
      { key: 'final',      label: 'Финалы',       shortLabel: 'Финалы',     order: 2 },
      { key: 'superfinal', label: 'Суперфинал',   shortLabel: 'Суперфинал', order: 3 },
    ];

    function getStageMetaByKey(key) {
      return STAGE_META.find(s => s.key === key);
    }

    // Переключение турнира
    (function setupChatSelect() {
      const tourSelect = document.getElementById('chat-select');
      const subSelect = document.getElementById('sub-select');
      if (!tourSelect) return;

      function normalizeUrlParams(url) {
        // единый параметр для под-турнира
        url.searchParams.delete('subId');
        url.searchParams.delete('subID');
        url.searchParams.delete('SubId');
        url.searchParams.delete('subid');
      }

      tourSelect.addEventListener('change', () => {
        const tournamentId = tourSelect.value;
        const url = new URL(window.location.href);

        // основной параметр — tournamentId
        url.searchParams.set('tournamentId', tournamentId);
        // на всякий случай убираем старый chatId
        url.searchParams.delete('chatId');

        // при смене турнира всегда сбрасываем выбор под-турнира
        url.searchParams.delete('SubID');
        normalizeUrlParams(url);

        window.location.href = url.toString();
      });

      if (subSelect) {
        subSelect.addEventListener('change', () => {
          const subId = subSelect.value;
          const url = new URL(window.location.href);

          // родительский турнир остаётся выбранным в tournamentId
          url.searchParams.set('tournamentId', tourSelect.value);
          url.searchParams.delete('chatId');

          if (subId) {
            url.searchParams.set('SubID', subId);
          } else {
            url.searchParams.delete('SubID');
          }
          normalizeUrlParams(url);

          window.location.href = url.toString();
        });
      }
    })();

    // Общая утилита агрегации (для базовых графиков групп/финалов/суперфиналов)
    function aggregateMetric(docs, playerName, metricConf) {
      const { agg, sourceKey } = metricConf;
      const values = [];

      for (const doc of docs) {
        if (!Array.isArray(doc.players)) continue;
        const player = doc.players.find(p => p.nameOrig === playerName);
        if (!player) continue;
        const val = Number(player[sourceKey]);
        if (!Number.isFinite(val)) continue;
        values.push(val);
      }

      if (!values.length) return 0;

      const sum = values.reduce((a, b) => a + b, 0);
      if (agg === 'sum') return sum;
      if (agg === 'avg') return sum / values.length;
      return sum;
    }

    function unique(arr) {
      return Array.from(new Set(arr));
    }

    function createCanvas(parent, heightPx = 320) {
      const wrap = document.createElement('div');
      wrap.className = 'chart-container';
      wrap.style.height = heightPx + 'px';
      const canvas = document.createElement('canvas');
      wrap.appendChild(canvas);
      parent.appendChild(wrap);
      return canvas;
    }

    function buildStageSection(containerId, titlePrefix, docs, allMode) {
      const container = document.getElementById(containerId);
      container.innerHTML = '';

      if (!docs.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных для выбранного турнира.';
        container.appendChild(note);
        return;
      }

      // In "all tournaments" mode don't split by groupId — numbers are meaningless
      // across different tournaments; show all docs as one aggregated group instead.
      const groupIds = allMode ? [null] : unique(docs.map(d => d.groupId).filter(v => v !== undefined));
      if (!allMode) groupIds.sort((a, b) => a - b);

      for (const groupId of groupIds) {
        const groupDocs = allMode ? docs : docs.filter(d => d.groupId === groupId);
        if (!groupDocs.length) continue;

        const groupDiv = document.createElement('div');
        groupDiv.className = 'group-wrapper';
        container.appendChild(groupDiv);

        const h3 = document.createElement('h3');
        h3.textContent = allMode ? 'Все турниры' : titlePrefix + ' ' + groupId;
        groupDiv.appendChild(h3);

        const players = unique(
          groupDocs.flatMap(d => Array.isArray(d.players) ? d.players.map(p => p.nameOrig) : [])
        );
        const maps = unique(groupDocs.map(d => d.map)).sort();

        if (!players.length || !maps.length) {
          const note = document.createElement('div');
          note.className = 'empty-note';
          note.textContent = 'Недостаточно данных (нет игроков или карт).';
          groupDiv.appendChild(note);
          continue;
        }

        const hasDmgInGroup = groupDocs.some(d => (d.players || []).some(p => Number(p.dgiv) > 0));

        for (const metric of METRICS) {
          if ((metric.key === 'dgiv' || metric.key === 'drec') && !hasDmgInGroup) continue;

          const h4map = document.createElement('h4');
          h4map.textContent = metric.label + ' по картам';
          groupDiv.appendChild(h4map);

          // График "по картам": ось X — карты, в каждой серии игрок
          const canvasMap = createCanvas(groupDiv);
          new Chart(canvasMap.getContext('2d'), {
            type: 'line',
            data: {
              labels: maps,
              datasets: players.map(playerName => ({
                label: playerName,
                data: maps.map(mapName => {
                  const subset = groupDocs.filter(d => d.map === mapName);
                  return aggregateMetric(subset, playerName, metric);
                }),
                tension: 0.2
              }))
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              interaction: { mode: 'index', intersect: false },
              plugins: {
                legend: { display: true, position: 'bottom' },
                title: { display: false }
              },
              scales: {
                x: { title: { display: true, text: 'Map' } },
                y: { title: { display: true, text: metric.label } }
              }
            }
          });

          const h4total = document.createElement('h4');
          h4total.textContent = metric.label + ' суммарно по всем картам';
          groupDiv.appendChild(h4total);

          // График "суммарно": ось X — игроки, значение — сумма/среднее по всем картам
          const canvasTotal = createCanvas(groupDiv);
          new Chart(canvasTotal.getContext('2d'), {
            type: 'bar',
            data: {
              labels: players,
              datasets: [
                {
                  label: metric.label,
                  data: players.map(playerName => aggregateMetric(groupDocs, playerName, metric))
                }
              ]
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              plugins: {
                legend: { display: false },
                title: { display: false }
              },
              scales: {
                x: { title: { display: true, text: 'Player' } },
                y: { title: { display: true, text: metric.label } }
              }
            }
          });
        }
      }
    }

    function buildOverallSection(containerId, allDocs) {
      const container = document.getElementById(containerId);
      container.innerHTML = '';

      if (!allDocs.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по стадиям для выбранного турнира.';
        container.appendChild(note);
        return;
      }

      const players = unique(
        allDocs.flatMap(d => Array.isArray(d.players) ? d.players.map(p => p.nameOrig) : [])
      );
      const maps = unique(allDocs.map(d => d.map)).sort();

      if (!players.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет игроков в данных.';
        container.appendChild(note);
        return;
      }

      const h3 = document.createElement('h3');
      h3.textContent = 'Сводные показатели по всем стадиям';
      container.appendChild(h3);

      const hasDmgOverall = allDocs.some(d => (d.players || []).some(p => Number(p.dgiv) > 0));

      for (const metric of METRICS) {
        if ((metric.key === 'dgiv' || metric.key === 'drec') && !hasDmgOverall) continue;

        // Первая часть: по картам
        if (maps.length) {
          const h4map = document.createElement('h4');
          h4map.textContent = metric.label + ' по картам (все стадии вместе)';
          container.appendChild(h4map);

          const canvasMap = createCanvas(container);
          new Chart(canvasMap.getContext('2d'), {
            type: 'line',
            data: {
              labels: maps,
              datasets: players.map(playerName => ({
                label: playerName,
                data: maps.map(mapName => {
                  const subset = allDocs.filter(d => d.map === mapName);
                  return aggregateMetric(subset, playerName, metric);
                }),
                tension: 0.2
              }))
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              interaction: { mode: 'index', intersect: false },
              plugins: {
                legend: { display: true, position: 'bottom' },
                title: { display: false }
              },
              scales: {
                x: { title: { display: true, text: 'Map' } },
                y: { title: { display: true, text: metric.label } }
              }
            }
          });
        }

        // Вторая часть: суммарно по всем картам
        const h4total = document.createElement('h4');
        h4total.textContent = metric.label + ' суммарно по всем картам (все стадии вместе)';
        container.appendChild(h4total);

        const canvasTotal = createCanvas(container);
        new Chart(canvasTotal.getContext('2d'), {
          type: 'bar',
          data: {
            labels: players,
            datasets: [
              {
                label: metric.label,
                data: players.map(playerName => aggregateMetric(allDocs, playerName, metric))
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              title: { display: false }
            },
            scales: {
              x: { title: { display: true, text: 'Player' } },
              y: { title: { display: true, text: metric.label } }
            }
          }
        });
      }
    }

    // ---- Серии побед по стадиям ----

    function computeWinStreaksForStage(docs, stageLabel) {
      const streaks = [];
      if (!docs || !docs.length) return streaks;

      const groupIds = unique(docs.map(d => d.groupId).filter(v => v !== undefined));

      for (const groupId of groupIds) {
        const groupDocs = docs.filter(d => d.groupId === groupId);
        if (!groupDocs.length) continue;

        const players = unique(
          groupDocs.flatMap(d => Array.isArray(d.players) ? d.players.map(p => p.nameOrig) : [])
        );
        if (!players.length) continue;

        for (const playerName of players) {
          const matches = groupDocs
            .filter(d => Array.isArray(d.players) && d.players.some(p => p.nameOrig === playerName))
            .slice()
            .sort((a, b) => {
              const aTime = String(a.matchDateTime || '');
              const bTime = String(b.matchDateTime || '');
              return aTime.localeCompare(bTime);
            });

          if (!matches.length) continue;

          let currentCount = 0;
          let currentMaps = [];

          function commitIfStreak() {
            if (currentCount >= 3) {
              streaks.push({
                playerName,
                stageLabel,
                groupId,
                count: currentCount,
                maps: currentMaps.slice(),
              });
            }
          }

          for (const m of matches) {
            const playersArr = Array.isArray(m.players) ? m.players : [];
            if (!playersArr.length) continue;

            const maxFrags = playersArr.reduce((max, p) => {
              const val = Number(p.frags) || 0;
              return val > max ? val : max;
            }, -Infinity);

            const isWin = playersArr.some(p =>
              p.nameOrig === playerName && Number(p.frags) === maxFrags
            );

            if (isWin) {
              currentCount += 1;
              currentMaps.push(m.map || '');
            } else {
              commitIfStreak();
              currentCount = 0;
              currentMaps = [];
            }
          }

          commitIfStreak();
        }
      }

      return streaks;
    }

    function buildWinStreaksSection(containerId, groupResults, finalResults, superfinalResults) {
      const container = document.getElementById(containerId);
      container.innerHTML = '';

      const streaks = []
        .concat(computeWinStreaksForStage(groupResults, 'Квалификации'))
        .concat(computeWinStreaksForStage(finalResults, 'Финалы'))
        .concat(computeWinStreaksForStage(superfinalResults, 'Суперфинал'));

      if (!streaks.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Не найдено ни одной серии из 3+ побед подряд.';
        container.appendChild(note);
        return;
      }

      streaks.sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        if (a.playerName !== b.playerName) return a.playerName.localeCompare(b.playerName);
        if (a.stageLabel !== b.stageLabel) return a.stageLabel.localeCompare(b.stageLabel);
        return Number(a.groupId || 0) - Number(b.groupId || 0);
      });

      const table = document.createElement('table');

      const thead = document.createElement('thead');
      thead.innerHTML = '<tr>' +
        '<th>Игрок</th>' +
        '<th>Стадия</th>' +
        '<th>Группа</th>' +
        '<th>Длина серии</th>' +
        '<th>Карты в серии</th>' +
        '</tr>';
      table.appendChild(thead);

      const tbody = document.createElement('tbody');

      for (const s of streaks) {
        const tr = document.createElement('tr');

        const tdPlayer = document.createElement('td');
        setPlayerNameTd(tdPlayer, s.playerName);
        tr.appendChild(tdPlayer);

        const tdStage = document.createElement('td');
        tdStage.textContent = s.stageLabel;
        tr.appendChild(tdStage);

        const tdGroup = document.createElement('td');
        tdGroup.textContent = String(s.groupId);
        tr.appendChild(tdGroup);

        const tdCount = document.createElement('td');
        tdCount.textContent = String(s.count);
        tr.appendChild(tdCount);

        const tdMaps = document.createElement('td');
        tdMaps.textContent = s.maps.join(', ');
        tr.appendChild(tdMaps);

        tbody.appendChild(tr);
      }

      table.appendChild(tbody);
      container.appendChild(table);
    }

    // ---- Сквозные серии побед ----

    function computeCrossStageWinStreaks(groupResults, finalResults, superfinalResults) {
      const streaks = [];

      const stages = [
        { label: 'Квалификации', order: 1, docs: groupResults || [] },
        { label: 'Финалы',       order: 2, docs: finalResults || [] },
        { label: 'Суперфинал',   order: 3, docs: superfinalResults || [] },
      ];

      const allDocs = stages.flatMap(s =>
        (s.docs || []).map(doc => ({ ...doc, __stageLabel: s.label, __stageOrder: s.order }))
      );

      if (!allDocs.length) return streaks;

      const players = unique(
        allDocs.flatMap(d => Array.isArray(d.players) ? d.players.map(p => p.nameOrig) : [])
      );
      if (!players.length) return streaks;

      for (const playerName of players) {
        const entries = [];

        for (const doc of allDocs) {
          if (!Array.isArray(doc.players)) continue;
          const player = doc.players.find(p => p.nameOrig === playerName);
          if (!player) continue;

          const playersArr = doc.players;

          const maxFrags = playersArr.reduce((max, p) => {
            const val = Number(p.frags) || 0;
            return val > max ? val : max;
          }, -Infinity);

          const isWin = Number(player.frags) === maxFrags;

          entries.push({
            stageLabel: doc.__stageLabel,
            stageOrder: doc.__stageOrder,
            groupId: doc.groupId,
            map: doc.map || '',
            matchDateTime: String(doc.matchDateTime || ''),
            isWin,
          });
        }

        if (!entries.length) continue;

        entries.sort((a, b) => {
          if (a.stageOrder !== b.stageOrder) return a.stageOrder - b.stageOrder;
          return a.matchDateTime.localeCompare(b.matchDateTime);
        });

        let currentCount = 0;
        let currentItems = [];

        function commitIfStreak() {
          if (currentCount >= 3) {
            const maps = currentItems.map(it => ({
              map: it.map,
              stageLabel: it.stageLabel,
              groupId: it.groupId,
            }));
            streaks.push({
              playerName,
              count: currentCount,
              maps,
            });
          }
        }

        for (const e of entries) {
          if (e.isWin) {
            currentCount += 1;
            currentItems.push(e);
          } else {
            commitIfStreak();
            currentCount = 0;
            currentItems = [];
          }
        }
        commitIfStreak();
      }

      return streaks;
    }

    function buildCrossWinStreaksSection(containerId, groupResults, finalResults, superfinalResults) {
      const container = document.getElementById(containerId);
      container.innerHTML = '';

      const streaks = computeCrossStageWinStreaks(groupResults, finalResults, superfinalResults);

      if (!streaks.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Не найдено ни одной сквозной серии из 3+ побед подряд.';
        container.appendChild(note);
        return;
      }

      // сортируем по длине серии (убывание), потом по имени
      streaks.sort((a, b) => {
        if (b.count !== a.count) return b.count - a.count;
        return a.playerName.localeCompare(b.playerName);
      });

      const table = document.createElement('table');

      const thead = document.createElement('thead');
      thead.innerHTML =
        '<tr>' +
          '<th>Игрок</th>' +
          '<th>Длина серии</th>' +
          '<th>Стадии/группы</th>' +
          '<th>Карты в серии</th>' +
        '</tr>';
      table.appendChild(thead);

      const tbody = document.createElement('tbody');

      for (const s of streaks) {
        const tr = document.createElement('tr');

        const tdPlayer = document.createElement('td');
        setPlayerNameTd(tdPlayer, s.playerName);
        tr.appendChild(tdPlayer);

        const tdCount = document.createElement('td');
        tdCount.textContent = String(s.count);
        tr.appendChild(tdCount);

        const tdStages = document.createElement('td');
        const stageGroups = [];
        for (const m of s.maps) {
          const key = m.stageLabel + '|' + m.groupId;
          if (!stageGroups.some(x => x.key === key)) {
            stageGroups.push({ key: key, label: m.stageLabel, groupId: m.groupId });
          }
        }
        tdStages.textContent = stageGroups
          .map(function (sg) {
            return sg.label + ' (группа ' + sg.groupId + ')';
          })
          .join(', ');
        tr.appendChild(tdStages);

        const tdMaps = document.createElement('td');
        tdMaps.textContent = s.maps
          .map(function (m) {
            return m.map + ' [' + m.stageLabel + ', группа ' + m.groupId + ']';
          })
          .join(', ');
        tr.appendChild(tdMaps);

        tbody.appendChild(tr);
      }

      table.appendChild(tbody);
      container.appendChild(table);
    }

    // ===== Новые вспомогательные функции для расширенной аналитики =====

    function buildAllDocsWithStage(groupResults, finalResults, superfinalResults) {
      const stages = [
        { key: 'group',      label: 'Квалификации', order: 1, docs: groupResults || [] },
        { key: 'final',      label: 'Финалы',       order: 2, docs: finalResults || [] },
        { key: 'superfinal', label: 'Суперфинал',   order: 3, docs: superfinalResults || [] },
      ];
      return stages.flatMap(s =>
        (s.docs || []).map(doc => ({
          ...doc,
          __stage: s.key,
          __stageLabel: s.label,
          __stageOrder: s.order,
        }))
      );
    }

    function computePlayerAggregates(allDocsWithStage) {
      const playerMap = new Map();

      for (const doc of allDocsWithStage) {
        const playersArr = Array.isArray(doc.players) ? doc.players : [];
        const mapName = doc.map || '';
        const stage = doc.__stage || null;
        const stageLabel = doc.__stageLabel || '';
        const stageOrder = doc.__stageOrder || 0;
        const matchDateTime = String(doc.matchDateTime || '');
        const groupId = doc.groupId;

        for (const p of playersArr) {
          const name = p.nameOrig;
          if (!name) continue;

          let agg = playerMap.get(name);
          if (!agg) {
            agg = { name, matches: [] };
            playerMap.set(name, agg);
          }

          const frags = Number(p.frags) || 0;
          const kills = Number(p.kills) || 0;
          const eff   = Number(p.eff)   || 0;
          const fph   = Number(p.fph)   || 0;
          const dgiv  = Number(p.dgiv)  || 0;
          const drec  = Number(p.drec)  || 0;

          agg.matches.push({
            frags,
            kills,
            eff,
            fph,
            dgiv,
            drec,
            map: mapName,
            stage,
            stageLabel,
            stageOrder,
            groupId,
            matchDateTime,
          });
        }
      }

      const players = [];

      playerMap.forEach((agg, name) => {
        const ms = agg.matches;
        const n = ms.length || 0;

        const sum = (key) => ms.reduce((s, m) => s + (m[key] || 0), 0);
        const mean = (key) => (n ? sum(key) / n : 0);

        const fragsTotal = sum('frags');
        const killsTotal = sum('kills');
        const effMean    = mean('eff');
        const fphMean    = mean('fph');
        const dgivTotal  = sum('dgiv');
        const drecTotal  = sum('drec');

        const kd = killsTotal > 0
          ? fragsTotal / killsTotal
          : (fragsTotal > 0 ? fragsTotal : 0);

        const netDmg       = dgivTotal - drecTotal;
        const dmgPerFrag   = fragsTotal > 0 ? dgivTotal / fragsTotal : 0;
        const dmgPerDeath  = killsTotal > 0 ? drecTotal / killsTotal : 0;
        const fragPer1kDmg = dgivTotal > 0 ? fragsTotal / (dgivTotal / 1000) : 0;

        const calcStd = (key) => {
          if (n <= 1) return 0;
          const m = mean(key);
          const variance = ms.reduce((s, mObj) => {
            const v = (mObj[key] || 0) - m;
            return s + v * v;
          }, 0) / n;
          return Math.sqrt(variance);
        };

        const fragsMean = mean('frags');
        const fragsStd  = calcStd('frags');
        const effStd    = calcStd('eff');
        const fphStd    = calcStd('fph');

        const effConsistency   = effStd   > 0 ? effMean   / effStd   : (effMean   > 0 ? effMean   : 0);
        const fragsConsistency = fragsStd > 0 ? fragsMean / fragsStd : (fragsMean > 0 ? fragsMean : 0);

        players.push({
          name,
          matches: ms,
          count: n,
          fragsTotal,
          killsTotal,
          effMean,
          fphMean,
          dgivTotal,
          drecTotal,
          kd,
          netDmg,
          dmgPerFrag,
          dmgPerDeath,
          fragPer1kDmg,
          fragsMean,
          fragsStd,
          effStd,
          fphStd,
          effConsistency,
          fragsConsistency,
        });
      });

      return players;
    }

    function computeMapAggregates(allDocsWithStage) {
      const mapMap = new Map();

      for (const doc of allDocsWithStage) {
        const mapName = doc.map || '';
        if (!mapName) continue;
        const playersArr = Array.isArray(doc.players) ? doc.players : [];
        if (!playersArr.length) continue;

        let agg = mapMap.get(mapName);
        if (!agg) {
          agg = {
            map: mapName,
            matchesCount: 0,
            totalFrags: 0,
            totalKills: 0,
            totalDgiv: 0,
            totalDrec: 0,
            playersSet: new Set(),
          };
          mapMap.set(mapName, agg);
        }

        agg.matchesCount += 1;
        for (const p of playersArr) {
          const frags = Number(p.frags) || 0;
          const kills = Number(p.kills) || 0;
          const dgiv  = Number(p.dgiv)  || 0;
          const drec  = Number(p.drec)  || 0;
          agg.totalFrags += frags;
          agg.totalKills += kills;
          agg.totalDgiv  += dgiv;
          agg.totalDrec  += drec;
          if (p.nameOrig) agg.playersSet.add(p.nameOrig);
        }
      }

      const result = [];
      mapMap.forEach((agg) => {
        const playersCount = agg.playersSet.size || 1;
        result.push({
          map: agg.map,
          matchesCount: agg.matchesCount,
          totalFrags: agg.totalFrags,
          totalKills: agg.totalKills,
          totalDgiv: agg.totalDgiv,
          totalDrec: agg.totalDrec,
          playersCount,
          avgFragsPerPlayer: agg.totalFrags / playersCount,
          avgDeathsPerPlayer: agg.totalKills / playersCount,
          avgDmgPerPlayer: agg.totalDgiv / playersCount,
        });
      });

      result.sort((a, b) => b.totalFrags - a.totalFrags || a.map.localeCompare(b.map));
      return result;
    }

    function arrayMean(arr) {
      if (!arr.length) return 0;
      return arr.reduce((s, v) => s + v, 0) / arr.length;
    }

    function arrayStd(arr) {
      const n = arr.length;
      if (n <= 1) return 0;
      const m = arrayMean(arr);
      const v = arr.reduce((s, v) => {
        const d = v - m;
        return s + d * d;
      }, 0) / n;
      return Math.sqrt(v);
    }

    function pearsonCorrelation(xs, ys) {
      const n = xs.length;
      if (!n || ys.length !== n) return 0;
      const meanX = arrayMean(xs);
      const meanY = arrayMean(ys);

      let num = 0;
      let denX = 0;
      let denY = 0;

      for (let i = 0; i < n; i++) {
        const dx = xs[i] - meanX;
        const dy = ys[i] - meanY;
        num  += dx * dy;
        denX += dx * dx;
        denY += dy * dy;
      }

      if (!denX || !denY) return 0;
      return num / Math.sqrt(denX * denY);
    }

    function describeCorrelation(r) {
      const abs = Math.abs(r);
      let strength;
      if (abs >= 0.8) strength = 'сильная';
      else if (abs >= 0.5) strength = 'умеренная';
      else if (abs >= 0.3) strength = 'слабая';
      else strength = 'очень слабая';

      const sign = r > 0 ? 'положительная' : (r < 0 ? 'отрицательная' : 'отсутствует');
      if (!r) return 'корреляция практически отсутствует';
      return strength + ' ' + sign;
    }

    // ====== Профили игроков ======

    function buildPlayerProfilesSection(tableContainerId, detailContainerId, playersAgg) {
      const tableContainer = document.getElementById(tableContainerId);
      const detailContainer = document.getElementById(detailContainerId);
      tableContainer.innerHTML = '';
      detailContainer.innerHTML = '';

      if (!playersAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по игрокам.';
        tableContainer.appendChild(note);
        return;
      }

      const players = playersAgg.slice().sort((a, b) => b.fragsTotal - a.fragsTotal || a.name.localeCompare(b.name));
      const hasDmgProfiles = playersAgg.some(p => p.dgivTotal > 0);

      // Таблица с основными метриками
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      thead.innerHTML =
        '<tr>' +
          '<th>Игрок</th>' +
          '<th>Карт</th>' +
          '<th>Frags (total)</th>' +
          '<th>Deaths (total)</th>' +
          '<th>F/D</th>' +
          (hasDmgProfiles ? '<th>Net dmg</th>' : '') +
          '<th>Eff (avg)</th>' +
          '<th>FPH (avg)</th>' +
          (hasDmgProfiles ? '<th>Dmg/frag</th>' : '') +
          (hasDmgProfiles ? '<th>Dmg/death</th>' : '') +
          (hasDmgProfiles ? '<th>Frags / 1000 dmg</th>' : '') +
          '<th>Consist(Eff)</th>' +
        '</tr>';
      table.appendChild(thead);

      const tbody = document.createElement('tbody');

      for (const p of players) {
        const tr = document.createElement('tr');

        // First cell: player name with optional alias tooltip
        const nameTd = document.createElement('td');
        setPlayerNameTd(nameTd, p.name);
        tr.appendChild(nameTd);

        const cells = [
          p.count,
          p.fragsTotal.toFixed(0),
          p.killsTotal.toFixed(0),
          p.kd.toFixed(2),
          ...(hasDmgProfiles ? [p.netDmg.toFixed(0)] : []),
          p.effMean.toFixed(1),
          p.fphMean.toFixed(1),
          ...(hasDmgProfiles ? [p.dmgPerFrag.toFixed(1), p.dmgPerDeath.toFixed(1), p.fragPer1kDmg.toFixed(2)] : []),
          p.effConsistency.toFixed(2),
        ];

        for (const val of cells) {
          const td = document.createElement('td');
          td.textContent = val;
          tr.appendChild(td);
        }

        tbody.appendChild(tr);
      }

      table.appendChild(tbody);
      tableContainer.appendChild(table);

      // Радар-диаграмма по выбранному игроку
      if (!players.length) return;

      const controls = document.createElement('div');
      controls.className = 'controls-row';

      const label = document.createElement('label');
      label.textContent = 'Игрок:';
      label.setAttribute('for', 'player-profile-select');
      controls.appendChild(label);

      const select = document.createElement('select');
      select.id = 'player-profile-select';
      for (const p of players) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        select.appendChild(opt);
      }
      controls.appendChild(select);
      detailContainer.appendChild(controls);

      const radarCanvas = createCanvas(detailContainer, 320);
      let radarChart = null;

      const radarMetrics = [
        { key: 'fragsTotal', label: 'Frags' },
        { key: 'kd',         label: 'F/D' },
        ...(hasDmgProfiles ? [{ key: 'netDmg', label: 'Net dmg' }] : []),
        { key: 'effMean',    label: 'Eff' },
        { key: 'fphMean',    label: 'FPH' },
      ];

      const maxByMetric = {};
      for (const m of radarMetrics) {
        maxByMetric[m.key] = Math.max(...players.map(p => Math.max(p[m.key] || 0, 0))) || 1;
      }

      function renderRadar(playerName) {
        const p = players.find(x => x.name === playerName);
        if (!p) return;

        const labels = radarMetrics.map(m => m.label);
        const data = radarMetrics.map(m => {
          const max = maxByMetric[m.key] || 1;
          const val = p[m.key] || 0;
          return max > 0 ? (val / max) * 100 : 0;
        });

        if (radarChart) radarChart.destroy();

        radarChart = new Chart(radarCanvas.getContext('2d'), {
          type: 'radar',
          data: {
            labels,
            datasets: [
              {
                label: 'Относительные показатели (0-100%)',
                data,
                fill: true,
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
            },
            scales: {
              r: {
                beginAtZero: true,
                suggestedMax: 100,
                ticks: { stepSize: 20 }
              }
            }
          }
        });
      }

      select.addEventListener('change', () => {
        renderRadar(select.value);
      });

      renderRadar(players[0].name);
    }

    // ====== Аналитика по картам ======

    function buildMapStatsSection(globalContainerId, playerDetailContainerId, mapAgg, playersAgg) {
      const globalContainer = document.getElementById(globalContainerId);
      const playerDetailContainer = document.getElementById(playerDetailContainerId);
      globalContainer.innerHTML = '';
      playerDetailContainer.innerHTML = '';

      if (!mapAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по картам.';
        globalContainer.appendChild(note);
        return;
      }

      const hasDmgMap = mapAgg.some(m => m.totalDgiv > 0);

      // Таблица по картам
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      thead.innerHTML =
        '<tr>' +
          '<th>Карта</th>' +
          '<th>Матчей</th>' +
          '<th>Total frags</th>' +
          '<th>Total deaths</th>' +
          (hasDmgMap ? '<th>Total dmg given</th>' : '') +
          (hasDmgMap ? '<th>Total dmg received</th>' : '') +
          '<th>Игроков</th>' +
          '<th>Frags / игрок</th>' +
          '<th>Deaths / игрок</th>' +
          (hasDmgMap ? '<th>Dmg / игрок</th>' : '') +
        '</tr>';
      table.appendChild(thead);

      const tbody = document.createElement('tbody');
      for (const m of mapAgg) {
        const tr = document.createElement('tr');
        const cells = [
          m.map,
          m.matchesCount,
          m.totalFrags.toFixed(0),
          m.totalKills.toFixed(0),
          ...(hasDmgMap ? [m.totalDgiv.toFixed(0), m.totalDrec.toFixed(0)] : []),
          m.playersCount,
          m.avgFragsPerPlayer.toFixed(1),
          m.avgDeathsPerPlayer.toFixed(1),
          ...(hasDmgMap ? [m.avgDmgPerPlayer.toFixed(1)] : []),
        ];
        for (const val of cells) {
          const td = document.createElement('td');
          td.textContent = val;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
      table.appendChild(tbody);
      globalContainer.appendChild(table);

      // График "мясности" карт: total frags
      const canvas = createCanvas(globalContainer, 280);
      new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
          labels: mapAgg.map(m => m.map),
          datasets: [
            {
              label: 'Total frags',
              data: mapAgg.map(m => m.totalFrags),
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
          },
          scales: {
            x: { title: { display: true, text: 'Map' } },
            y: { title: { display: true, text: 'Total frags' } }
          }
        }
      });

      // Детализация по игроку: любимые/сложные карты
      if (!playersAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по игрокам для детализации по картам.';
        playerDetailContainer.appendChild(note);
        return;
      }

      const players = playersAgg.slice().sort((a, b) => a.name.localeCompare(b.name));

      const controls = document.createElement('div');
      controls.className = 'controls-row';

      const label = document.createElement('label');
      label.textContent = 'Игрок:';
      label.setAttribute('for', 'map-player-select');
      controls.appendChild(label);

      const select = document.createElement('select');
      select.id = 'map-player-select';
      for (const p of players) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        select.appendChild(opt);
      }
      controls.appendChild(select);
      playerDetailContainer.appendChild(controls);

      const detailArea = document.createElement('div');
      playerDetailContainer.appendChild(detailArea);

      function renderPlayerMaps(playerName) {
        detailArea.innerHTML = '';
        const p = players.find(x => x.name === playerName);
        if (!p || !p.matches || !p.matches.length) {
          const note = document.createElement('div');
          note.className = 'empty-note';
          note.textContent = 'Нет матчей для выбранного игрока.';
          detailArea.appendChild(note);
          return;
        }

        const byMap = new Map();
        for (const m of p.matches) {
          const key = m.map || '';
          if (!key) continue;
          let agg = byMap.get(key);
          if (!agg) {
            agg = { map: key, count: 0, sumFrags: 0, sumDeaths: 0, sumEff: 0 };
            byMap.set(key, agg);
          }
          agg.count += 1;
          agg.sumFrags  += m.frags || 0;
          agg.sumDeaths += m.kills || 0;
          agg.sumEff    += m.eff   || 0;
        }

        const rows = [];
        byMap.forEach((agg) => {
          rows.push({
            map: agg.map,
            count: agg.count,
            avgFrags: agg.sumFrags / agg.count,
            avgDeaths: agg.sumDeaths / agg.count,
            avgEff:   agg.sumEff   / agg.count,
          });
        });

        if (!rows.length) {
          const note = document.createElement('div');
          note.className = 'empty-note';
          note.textContent = 'Нет статистики по картам для выбранного игрока.';
          detailArea.appendChild(note);
          return;
        }

        rows.sort((a, b) => b.avgEff - a.avgEff || b.avgFrags - a.avgFrags);

        const top3 = rows.slice(0, 3);
        const bottom3 = rows.slice(-3);

        const hBest = document.createElement('h4');
        hBest.textContent = 'Лучшие карты (по эффективности)';
        detailArea.appendChild(hBest);

        const tableBest = document.createElement('table');
        const theadBest = document.createElement('thead');
        theadBest.innerHTML =
          '<tr>' +
            '<th>Карта</th>' +
            '<th>Карт</th>' +
            '<th>Frags (avg)</th>' +
            '<th>Deaths (avg)</th>' +
            '<th>Eff (avg)</th>' +
          '</tr>';
        tableBest.appendChild(theadBest);
        const tbodyBest = document.createElement('tbody');
        for (const r of top3) {
          const tr = document.createElement('tr');
          const cells = [
            r.map,
            r.count,
            r.avgFrags.toFixed(1),
            r.avgEff.toFixed(1),
          ];
          for (const val of cells) {
            const td = document.createElement('td');
            td.textContent = val;
            tr.appendChild(td);
          }
          tbodyBest.appendChild(tr);
        }
        tableBest.appendChild(tbodyBest);
        detailArea.appendChild(tableBest);

        const hWorst = document.createElement('h4');
        hWorst.textContent = 'Сложные карты (по эффективности)';
        detailArea.appendChild(hWorst);

        const tableWorst = document.createElement('table');
        const theadWorst = document.createElement('thead');
        theadWorst.innerHTML =
          '<tr>' +
            '<th>Карта</th>' +
            '<th>Карт</th>' +
            '<th>Frags (avg)</th>' +
            '<th>Deaths (avg)</th>' +
            '<th>Eff (avg)</th>' +
          '</tr>';
        tableWorst.appendChild(theadWorst);
        const tbodyWorst = document.createElement('tbody');
        for (const r of bottom3) {
          const tr = document.createElement('tr');
          const cells = [
            r.map,
            r.count,
            r.avgFrags.toFixed(1),
            r.avgEff.toFixed(1),
          ];
          for (const val of cells) {
            const td = document.createElement('td');
            td.textContent = val;
            tr.appendChild(td);
          }
          tbodyWorst.appendChild(tr);
        }
        tableWorst.appendChild(tbodyWorst);
        detailArea.appendChild(tableWorst);
      }

      select.addEventListener('change', () => {
        renderPlayerMaps(select.value);
      });

      renderPlayerMaps(players[0].name);
    }

    // ====== Форма игроков (динамика по времени) ======

    function buildPlayerFormSection(controlsId, chartContainerId, playersAgg) {
      const controls = document.getElementById(controlsId);
      const chartContainer = document.getElementById(chartContainerId);
      controls.innerHTML = '';
      chartContainer.innerHTML = '';

      if (!playersAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по игрокам.';
        chartContainer.appendChild(note);
        return;
      }

      const players = playersAgg.slice().sort((a, b) => a.name.localeCompare(b.name));

      const row = document.createElement('div');
      row.className = 'controls-row';

      const label = document.createElement('label');
      label.textContent = 'Игрок:';
      label.setAttribute('for', 'player-form-select');
      row.appendChild(label);

      const select = document.createElement('select');
      select.id = 'player-form-select';
      for (const p of players) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        select.appendChild(opt);
      }
      row.appendChild(select);

      controls.appendChild(row);

      const canvas = createCanvas(chartContainer, 320);
      let formChart = null;

      function renderForm(playerName) {
        const p = players.find(x => x.name === playerName);
        if (!p) return;

        const matches = (p.matches || []).slice().sort((a, b) => {
          if (a.stageOrder !== b.stageOrder) return a.stageOrder - b.stageOrder;
          return String(a.matchDateTime || '').localeCompare(String(b.matchDateTime || ''));
        });

        if (!matches.length) {
          chartContainer.innerHTML = '';
          const note = document.createElement('div');
          note.className = 'empty-note';
          note.textContent = 'Нет матчей для выбранного игрока.';
          chartContainer.appendChild(note);
          return;
        }

        const labels = matches.map((m, idx) => {
          const stageMeta = getStageMetaByKey(m.stage);
          const stageLabel = stageMeta ? stageMeta.shortLabel : (m.stageLabel || '');
          const map = m.map || 'map';
          return (idx + 1) + '. ' + map + ' (' + stageLabel + ')';
        });

        const fragsData = matches.map(m => m.frags || 0);
        const effData   = matches.map(m => m.eff   || 0);

        if (formChart) formChart.destroy();

        formChart = new Chart(canvas.getContext('2d'), {
          type: 'line',
          data: {
            labels,
            datasets: [
              {
                label: 'Frags',
                data: fragsData,
                yAxisID: 'y1',
                tension: 0.2,
              },
              {
                label: 'Eff',
                data: effData,
                yAxisID: 'y2',
                tension: 0.2,
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
              legend: { display: true, position: 'bottom' },
            },
            scales: {
              x: { title: { display: true, text: 'Матчи по времени' } },
              y1: {
                type: 'linear',
                position: 'left',
                title: { display: true, text: 'Frags' }
              },
              y2: {
                type: 'linear',
                position: 'right',
                title: { display: true, text: 'Eff' },
                grid: { drawOnChartArea: false }
              }
            }
          }
        });
      }

      select.addEventListener('change', () => {
        renderForm(select.value);
      });

      renderForm(players[0].name);
    }

    // ====== Сравнение по стадиям ======

    function computeStageStatsForPlayer(player) {
      const result = {
        group:      { frags: 0, kills: 0, eff: 0, count: 0 },
        final:      { frags: 0, kills: 0, eff: 0, count: 0 },
        superfinal: { frags: 0, kills: 0, eff: 0, count: 0 },
      };

      for (const m of player.matches || []) {
        const stage = m.stage;
        if (!stage || !result[stage]) continue;
        result[stage].frags += m.frags || 0;
        result[stage].kills += m.kills || 0;
        result[stage].eff   += m.eff   || 0;
        result[stage].count += 1;
      }

      const out = {};
      for (const key of Object.keys(result)) {
        const st = result[key];
        const fragsMean = st.count ? (st.frags / st.count) : 0;
        const killsMean = st.count ? (st.kills / st.count) : 0;
        const effMean   = st.count ? (st.eff   / st.count) : 0;
        out[key] = { fragsMean, killsMean, effMean, count: st.count };
      }
      return out;
    }

    function buildStageComparisonSection(tableContainerId, chartContainerId, playersAgg) {
      const tableContainer = document.getElementById(tableContainerId);
      const chartContainer = document.getElementById(chartContainerId);
      tableContainer.innerHTML = '';
      chartContainer.innerHTML = '';

      if (!playersAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по игрокам.';
        tableContainer.appendChild(note);
        return;
      }

      const players = playersAgg.slice().sort((a, b) => a.name.localeCompare(b.name));

      // Таблица
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      thead.innerHTML =
        '<tr>' +
          '<th>Игрок</th>' +
          '<th>Frags (Квалы)</th>' +
          '<th>Deaths (Квалы)</th>' +
          '<th>Frags (Финалы)</th>' +
          '<th>Deaths (Финалы)</th>' +
          '<th>Frags (Суперфинал)</th>' +
          '<th>Deaths (Суперфинал)</th>' +
          '<th>Eff (Квалы)</th>' +
          '<th>Eff (Финалы)</th>' +
          '<th>Eff (Суперфинал)</th>' +
        '</tr>';
      table.appendChild(thead);

      const tbody = document.createElement('tbody');

      const stageKeys = ['group', 'final', 'superfinal'];

      const statsByPlayer = new Map();

      for (const p of players) {
        const st = computeStageStatsForPlayer(p);
        statsByPlayer.set(p.name, st);

        const tr = document.createElement('tr');
        const cells = [p.name];

        for (const key of stageKeys) {
          const v = st[key];
          cells.push(v.count ? v.fragsMean.toFixed(1) : '-');
          cells.push(v.count ? v.killsMean.toFixed(1) : '-');
        }
        for (const key of stageKeys) {
          const v = st[key];
          cells.push(v.count ? v.effMean.toFixed(1) : '-');
        }

        for (const val of cells) {
          const td = document.createElement('td');
          td.textContent = val;
          tr.appendChild(td);
        }

        tbody.appendChild(tr);
      }

      table.appendChild(tbody);
      tableContainer.appendChild(table);

      // График по стадиям для игрока
      const controls = document.createElement('div');
      controls.className = 'controls-row';

      const label = document.createElement('label');
      label.textContent = 'Игрок:';
      label.setAttribute('for', 'stage-comparison-select');
      controls.appendChild(label);

      const select = document.createElement('select');
      select.id = 'stage-comparison-select';
      for (const p of players) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        select.appendChild(opt);
      }
      controls.appendChild(select);
      chartContainer.appendChild(controls);

      const canvas = createCanvas(chartContainer, 320);
      let stageChart = null;

      function renderStageChart(playerName) {
        const st = statsByPlayer.get(playerName);
        if (!st) return;

        const labels = STAGE_META.map(s => s.shortLabel);
        const fragsData = STAGE_META.map(s => (st[s.key] && st[s.key].count ? st[s.key].fragsMean : null));
        const effData   = STAGE_META.map(s => (st[s.key] && st[s.key].count ? st[s.key].effMean   : null));

        if (stageChart) stageChart.destroy();

        stageChart = new Chart(canvas.getContext('2d'), {
          type: 'line',
          data: {
            labels,
            datasets: [
              {
                label: 'Frags (avg)',
                data: fragsData,
                yAxisID: 'y1',
                tension: 0.2,
              },
              {
                label: 'Eff (avg)',
                data: effData,
                yAxisID: 'y2',
                tension: 0.2,
              }
            ]
          },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
              legend: { display: true, position: 'bottom' },
            },
            scales: {
              x: { title: { display: true, text: 'Стадия' } },
              y1: {
                type: 'linear',
                position: 'left',
                title: { display: true, text: 'Frags (avg)' }
              },
              y2: {
                type: 'linear',
                position: 'right',
                title: { display: true, text: 'Eff (avg)' },
                grid: { drawOnChartArea: false }
              }
            }
          }
        });
      }

      select.addEventListener('change', () => {
        renderStageChart(select.value);
      });

      renderStageChart(players[0].name);
    }

    // ====== Научный рейтинг и корреляции ======

    function computeScienceRating(playersAgg, hasDmg) {
      if (!playersAgg.length) return { ratingRows: [], meta: null };

      const fragsArr = playersAgg.map(p => p.fragsTotal || 0);
      const killsArr = playersAgg.map(p => p.killsTotal || 0);
      const effArr   = playersAgg.map(p => p.effMean    || 0);
      const netArr   = playersAgg.map(p => p.netDmg     || 0);

      const fragsMean = arrayMean(fragsArr);
      const fragsStd  = arrayStd(fragsArr);
      const killsMean = arrayMean(killsArr);
      const killsStd  = arrayStd(killsArr);
      const effMean   = arrayMean(effArr);
      const effStd    = arrayStd(effArr);
      const netMean   = arrayMean(netArr);
      const netStd    = arrayStd(netArr);

      const ratingRows = playersAgg.map((p, idx) => {
        const zFrags  = fragsStd > 0 ? (p.fragsTotal - fragsMean) / fragsStd : 0;
        const zDeaths = killsStd > 0 ? (p.killsTotal - killsMean) / killsStd : 0;
        const zEff    = effStd   > 0 ? (p.effMean    - effMean)   / effStd   : 0;
        const zNet    = netStd   > 0 ? (p.netDmg     - netMean)   / netStd   : 0;

        const rating = hasDmg
          ? 0.5 * zFrags + 0.3 * zEff + 0.2 * zNet
          : 0.5 * zFrags + 0.5 * zEff;

        return {
          name: p.name,
          games: p.count,
          zFrags,
          zDeaths,
          zEff,
          zNet,
          rating,
        };
      });

      ratingRows.sort((a, b) => b.rating - a.rating || a.name.localeCompare(b.name));

      return {
        ratingRows,
        meta: {
          fragsMean,
          fragsStd,
          killsMean,
          killsStd,
          effMean,
          effStd,
          netMean,
          netStd,
        }
      };
    }

    function buildScienceSection(ratingContainerId, correlationsContainerId, playersAgg) {
      const ratingContainer = document.getElementById(ratingContainerId);
      const corrContainer   = document.getElementById(correlationsContainerId);
      ratingContainer.innerHTML = '';
      corrContainer.innerHTML   = '';

      if (!playersAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по игрокам.';
        ratingContainer.appendChild(note);
        return;
      }

      const hasDmgScience = playersAgg.some(p => p.dgivTotal > 0);
      const { ratingRows } = computeScienceRating(playersAgg, hasDmgScience);

      if (!ratingRows.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Не удалось посчитать рейтинг (недостаточно разброса по метрикам).';
        ratingContainer.appendChild(note);
        return;
      }

      // Таблица рейтинга
      const table = document.createElement('table');
      const thead = document.createElement('thead');
      thead.innerHTML =
        '<tr>' +
          '<th>Игрок</th>' +
          '<th>Карт</th>' +
          '<th>Rating</th>' +
          '<th>z(Frags)</th>' +
          '<th>z(Deaths)</th>' +
          '<th>z(Eff)</th>' +
          (hasDmgScience ? '<th>z(Net dmg)</th>' : '') +
        '</tr>';
      table.appendChild(thead);

      const tbody = document.createElement('tbody');
      for (const r of ratingRows) {
        const tr = document.createElement('tr');
        const cells = [
          r.name,
          r.games,
          r.rating.toFixed(2),
          r.zFrags.toFixed(2),
          r.zDeaths.toFixed(2),
          r.zEff.toFixed(2),
          ...(hasDmgScience ? [r.zNet.toFixed(2)] : []),
        ];
        for (const val of cells) {
          const td = document.createElement('td');
          td.textContent = val;
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
      table.appendChild(tbody);
      ratingContainer.appendChild(table);

      // Бар-чарт топ-10 по рейтингу
      const topN = ratingRows.slice(0, 10);
      const canvas = createCanvas(ratingContainer, 280);
      new Chart(canvas.getContext('2d'), {
        type: 'bar',
        data: {
          labels: topN.map(r => r.name),
          datasets: [
            {
              label: 'Rating',
              data: topN.map(r => r.rating),
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
          },
          scales: {
            x: { title: { display: true, text: 'Игрок' } },
            y: { title: { display: true, text: 'Rating (z-комбинация)' } },
          }
        }
      });

      // Корреляции
      const metricDefs = [
        { key: 'fragsTotal', label: 'Total frags' },
        { key: 'killsTotal', label: 'Total deaths' },
        { key: 'effMean',    label: 'Avg eff' },
        { key: 'fphMean',    label: 'Avg FPH' },
        { key: 'dgivTotal',  label: 'Total dmg given' },
        { key: 'drecTotal',  label: 'Total dmg received' },
        { key: 'kd',         label: 'F/D' },
        { key: 'netDmg',     label: 'Net dmg' },
      ];

      const pairs = [
        ['fragsTotal', 'effMean'],
        ['fragsTotal', 'fphMean'],
        ...(hasDmgScience ? [
          ['fragsTotal', 'dgivTotal'],
          ['effMean',    'dgivTotal'],
          ['netDmg',     'fragsTotal'],
          ['netDmg',     'effMean'],
        ] : []),
        ['effMean',    'kd'],
      ];

      const corrTable = document.createElement('table');
      const corrThead = document.createElement('thead');
      corrThead.innerHTML =
        '<tr>' +
          '<th>Пара метрик</th>' +
          '<th>Коэффициент корреляции (r)</th>' +
          '<th>Интерпретация</th>' +
        '</tr>';
      corrTable.appendChild(corrThead);

      const corrTbody = document.createElement('tbody');

      for (const [aKey, bKey] of pairs) {
        const aDef = metricDefs.find(m => m.key === aKey);
        const bDef = metricDefs.find(m => m.key === bKey);
        if (!aDef || !bDef) continue;

        const xs = playersAgg.map(p => p[aKey] || 0);
        const ys = playersAgg.map(p => p[bKey] || 0);
        const r = pearsonCorrelation(xs, ys);
        const desc = describeCorrelation(r);

        const tr = document.createElement('tr');
        const pairLabel = aDef.label + ' vs ' + bDef.label;

        const cells = [
          pairLabel,
          r.toFixed(3),
          desc,
        ];
        for (const val of cells) {
          const td = document.createElement('td');
          td.textContent = val;
          tr.appendChild(td);
        }

        corrTbody.appendChild(tr);
      }

      corrTable.appendChild(corrTbody);
      corrContainer.appendChild(corrTable);
    }

    // ====== Head-to-head ======

    function buildHeadToHeadSection(controlsId, contentId, allDocsWithStage, playersAgg) {
      const controls = document.getElementById(controlsId);
      const content  = document.getElementById(contentId);
      controls.innerHTML = '';
      content.innerHTML  = '';

      if (!playersAgg.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных по игрокам.';
        content.appendChild(note);
        return;
      }

      const players = playersAgg.slice().sort((a, b) => a.name.localeCompare(b.name));

      const row = document.createElement('div');
      row.className = 'controls-row';

      const labelA = document.createElement('label');
      labelA.textContent = 'Игрок A:';
      labelA.setAttribute('for', 'h2h-player-a');
      row.appendChild(labelA);

      const selectA = document.createElement('select');
      selectA.id = 'h2h-player-a';
      for (const p of players) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        selectA.appendChild(opt);
      }
      row.appendChild(selectA);

      const labelB = document.createElement('label');
      labelB.textContent = 'Игрок B:';
      labelB.setAttribute('for', 'h2h-player-b');
      row.appendChild(labelB);

      const selectB = document.createElement('select');
      selectB.id = 'h2h-player-b';
      for (const p of players) {
        const opt = document.createElement('option');
        opt.value = p.name;
        opt.textContent = p.name;
        selectB.appendChild(opt);
      }
      row.appendChild(selectB);

      controls.appendChild(row);

      function renderH2H() {
        content.innerHTML = '';

        const aName = selectA.value;
        const bName = selectB.value;
        if (!aName || !bName || aName === bName) {
          const note = document.createElement('div');
          note.className = 'empty-note';
          note.textContent = 'Выберите двух разных игроков для сравнения.';
          content.appendChild(note);
          return;
        }

        const matches = [];

        for (const doc of allDocsWithStage) {
          const playersArr = Array.isArray(doc.players) ? doc.players : [];
          const a = playersArr.find(p => p.nameOrig === aName);
          const b = playersArr.find(p => p.nameOrig === bName);
          if (!a || !b) continue;

          const aFrags = Number(a.frags) || 0;
          const aDeaths = Number(a.kills) || 0;
          const bFrags = Number(b.frags) || 0;
          const bDeaths = Number(b.kills) || 0;

          let winner = 'ничья';
          if (aFrags > bFrags) winner = 'A';
          else if (bFrags > aFrags) winner = 'B';

          matches.push({
            stageLabel: doc.__stageLabel || '',
            groupId: doc.groupId,
            map: doc.map || '',
            aFrags,
            aDeaths,
            bFrags,
            bDeaths,
            winner,
          });
        }

        if (!matches.length) {
          const note = document.createElement('div');
          note.className = 'empty-note';
          note.textContent = 'У выбранных игроков нет общих карт в данных.';
          content.appendChild(note);
          return;
        }

        let winsA = 0;
        let winsB = 0;
        let draws = 0;
        let sumDiff = 0;

        for (const m of matches) {
          if (m.winner === 'A') winsA += 1;
          else if (m.winner === 'B') winsB += 1;
          else draws += 1;
          sumDiff += (m.aFrags - m.bFrags);
        }

        const total = matches.length;
        const avgDiff = total ? (sumDiff / total) : 0;

        const summary = document.createElement('div');
        summary.innerHTML =
          '<p><strong>Итого:</strong> ' + total + ' карт вместе. ' +
          aName + ' выиграл(а) ' + winsA + ', ' +
          bName + ' выиграл(а) ' + winsB + ', ничьих: ' + draws + '. ' +
          'Средняя разница фрагов (A - B): ' + avgDiff.toFixed(1) + '.</p>';
        content.appendChild(summary);

        const table = document.createElement('table');
        const thead = document.createElement('thead');
        thead.innerHTML =
          '<tr>' +
            '<th>Стадия</th>' +
            '<th>Группа</th>' +
            '<th>Карта</th>' +
            '<th>' + aName + ' frags</th>' +
            '<th>' + aName + ' deaths</th>' +
            '<th>' + bName + ' frags</th>' +
            '<th>' + bName + ' deaths</th>' +
            '<th>Победитель</th>' +
          '</tr>';
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        for (const m of matches) {
          const tr = document.createElement('tr');
          const winnerStr =
            m.winner === 'A' ? aName :
            m.winner === 'B' ? bName : 'ничья';

          const cells = [
            m.stageLabel,
            String(m.groupId),
            m.map,
            m.aFrags.toFixed(0),
            m.aDeaths.toFixed(0),
            m.bFrags.toFixed(0),
            m.bDeaths.toFixed(0),
            winnerStr,
          ];
          for (const val of cells) {
            const td = document.createElement('td');
            td.textContent = val;
            tr.appendChild(td);
          }
          tbody.appendChild(tr);
        }
        table.appendChild(tbody);
        content.appendChild(table);
      }

      selectA.addEventListener('change', renderH2H);
      selectB.addEventListener('change', renderH2H);

      // начальное значение
      if (players.length >= 2) {
        selectB.value = players[1].name;
      }
      renderH2H();
    }

    
    // ====== Player name tooltip helpers (outer scope — accessible from all functions) ======

    function getPlayerAliasHint(displayedName) {
      const d = window.__INITIAL_DATA__ || {};
      const nickMap     = d.nickMap     || {};
      const nickAliases = d.nickAliases || {};
      if (!displayedName || !Object.keys(nickAliases).length) return '';
      const canon = nickMap[String(displayedName).toLowerCase()];
      if (!canon) return '';
      const list = nickAliases[canon];
      if (!list || !list.length) return '';
      return list.join(', ');
    }

    function setPlayerNameTd(td, name) {
      td.textContent = name;
      const hint = getPlayerAliasHint(name);
      if (hint) {
        td.title = hint;
        td.style.cursor = 'help';
        td.style.textDecoration = 'underline dotted';
        td.style.textUnderlineOffset = '3px';
      }
    }

    // ====== Nick aggregation toggle (nick_agr=0/1) ======

    function setupNickAggregationToggle() {
      const checkbox = document.getElementById('nick-agr-toggle');
      if (!checkbox) return;

      const data = window.__INITIAL_DATA__ || {};
      checkbox.checked = !!data.nickAgrEnabled;

      checkbox.addEventListener('change', () => {
        const enabled = checkbox.checked;
        const url = new URL(window.location.href);
        url.searchParams.set('nick_agr', enabled ? '1' : '0');
        window.location.href = url.toString();
      });
    }

// ====== Fullwidth toggle ======

    function setupFullwidthToggle() {
      const checkbox = document.getElementById('fullwidth-toggle');
      if (!checkbox) return;

      const STORAGE_KEY = 'qjAnalyticsFullwidth';

      try {
        const saved = window.localStorage ? localStorage.getItem(STORAGE_KEY) : null;
        if (saved === '1') {
          document.body.classList.add('layout-fullwidth');
          checkbox.checked = true;
        }
      } catch (e) {
        // игнорируем ошибки localStorage
      }

      checkbox.addEventListener('change', () => {
        const enabled = checkbox.checked;
        if (enabled) {
          document.body.classList.add('layout-fullwidth');
        } else {
          document.body.classList.remove('layout-fullwidth');
        }
        try {
          if (window.localStorage) {
            localStorage.setItem(STORAGE_KEY, enabled ? '1' : '0');
          }
        } catch (e) {
          // игнорируем
        }
      });
    }

    function setupScrollSpy() {
      const links = Array.from(document.querySelectorAll('.main-nav a[href^="#"]'));
      if (!links.length) return;

      const sections = links
        .map(link => {
          const hash = link.getAttribute('href');
          if (!hash || !hash.startsWith('#')) return null;
          const section = document.querySelector(hash);
          if (!section) return null;
          return { link, section };
        })
        .filter(Boolean);

      if (!sections.length) return;

      function onScroll() {
        const offset = 120;
        const fromTop = window.scrollY + offset;

        let current = null;
        for (const item of sections) {
          const top = item.section.offsetTop;
          if (top <= fromTop) {
            if (!current || top > current.section.offsetTop) {
              current = item;
            }
          }
        }

        sections.forEach(item => {
          item.link.classList.toggle('active', item === current);
        });
      }

      window.addEventListener('scroll', onScroll);
      window.addEventListener('resize', onScroll);
      onScroll();
    }

    // Стартовая инициализация
    (function init() {
      const data = window.__INITIAL_DATA__;
      const groupResults = data.groupResults || [];
      const finalResults = data.finalResults || [];
      const superfinalResults = data.superfinalResults || [];
      const allMode = !!data.allMode;

      // Nick aggregation: заменяем nameOrig на канонический ник (только в allMode)
      const nickAgrEnabled = !!data.nickAgrEnabled;
      const nickMap = data.nickMap || {};

      function normalizeNick(name) {
        const s = String(name || '').trim();
        if (!s) return s;
        const key = s.toLowerCase();
        return (nickMap && nickMap[key]) ? nickMap[key] : s;
      }

      function applyNickAggregationToDocs(docs) {
        if (!nickAgrEnabled) return;
        for (const doc of docs) {
          if (!doc || !Array.isArray(doc.players)) continue;
          for (const p of doc.players) {
            if (!p) continue;
            if (p.nameRaw === undefined) p.nameRaw = p.nameOrig; // сохраняем оригинал (на всякий случай)
            p.nameOrig = normalizeNick(p.nameOrig);
          }
        }
      }

      applyNickAggregationToDocs(groupResults);
      applyNickAggregationToDocs(finalResults);
      applyNickAggregationToDocs(superfinalResults);


      // Базовые секции
      buildStageSection('group-results-content', 'Группа', groupResults, allMode);
      buildStageSection('final-results-content', 'Финальная группа', finalResults, allMode);
      buildStageSection('superfinal-results-content', 'Суперфинальная группа', superfinalResults, allMode);

      const allDocs = groupResults.concat(finalResults, superfinalResults);
      buildOverallSection('overall-content', allDocs);

      // Серии побед в разрезе стадий
      buildWinStreaksSection('win-streaks-content', groupResults, finalResults, superfinalResults);
      // Сквозные серии
      buildCrossWinStreaksSection('cross-win-streaks-content', groupResults, finalResults, superfinalResults);

      // Новые блоки аналитики
      const allDocsWithStage = buildAllDocsWithStage(groupResults, finalResults, superfinalResults);
      const playersAgg = computePlayerAggregates(allDocsWithStage);
      const mapAgg = computeMapAggregates(allDocsWithStage);

      buildPlayerProfilesSection('player-profiles-table', 'player-profile-detail', playersAgg);
      buildMapStatsSection('map-global-table', 'map-player-detail', mapAgg, playersAgg);
      buildPlayerFormSection('player-form-controls', 'player-form-chart', playersAgg);
      buildStageComparisonSection('stage-comparison-table', 'stage-comparison-chart', playersAgg);
      buildScienceSection('science-rating-content', 'correlations-content', playersAgg);
      buildHeadToHeadSection('headtohead-controls', 'headtohead-content', allDocsWithStage, playersAgg);

      setupNickAggregationToggle();
      setupFullwidthToggle();
      setupScrollSpy();
    })();
  </script>
</body>
</html>`;

      res.status(200).send(html);
    } catch (err) {
      console.error('[analytics] Error rendering analytics page:', err);
      res.status(500).send('Internal server error');
    }
  });
}

module.exports = {
  attachAnalyticsRoutes,
};
