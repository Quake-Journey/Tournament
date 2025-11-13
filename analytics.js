// analytics.js
require('dotenv').config();

const { MongoClient } = require('mongodb');

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017/tournament';

// SITE_CHAT_ID=-4961062249,350920766,-5094364912
const SITE_CHAT_ID = (process.env.SITE_CHAT_ID || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean)
    .map(v => Number(v))
    .filter(v => !Number.isNaN(v));

if (!SITE_CHAT_ID.length) {
    console.warn('[analytics] WARNING: SITE_CHAT_ID is empty or invalid. No chats will be available for analytics.');
}

let db;
let client;

// Инициализация подключения к MongoDB один раз на модуль
const dbReady = (async() => {
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
    app.get('/analytics', async(req, res) => {
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

                    // Сначала загружаем турниры по chatId из SITE_CHAT_ID
                    const chats = await chatsCol
                        .find({ chatId: { $in: SITE_CHAT_ID } })
                        .sort({ tournamentName: 1 })
                        .toArray();

                    // Если вдруг в БД нет ни одного совпадения
                    if (!chats.length) {
                        res.status(200).send('<h1>No tournaments found for SITE_CHAT_ID</h1>');
                        return;
                    }

                    // Основной параметр — tournamentId, но поддерживаем и старый chatId для совместимости
                    const rawParam = (req.query.tournamentId !== undefined) ?
                        req.query.tournamentId :
                        req.query.chatId;

                    const queryChatId = rawParam !== undefined ? Number(rawParam) : null;

                    let currentChatId = null;
                    if (Number.isFinite(queryChatId) && SITE_CHAT_ID.includes(queryChatId)) {
                        currentChatId = queryChatId;
                    } else {
                        // если параметр невалиден / не разрешён — берём первый из SITE_CHAT_ID
                        currentChatId = SITE_CHAT_ID[0];
                    }

                    // Текущий турнир
                    const currentChat = chats.find(c => c.chatId === currentChatId) || chats[0];
                    currentChatId = currentChat.chatId;

                    // Данные по группам/финалам/суперфиналам для выбранного чата
                    const [groupResults, finalResults, superfinalResults] = await Promise.all([
                        groupResultsCol.find({ chatId: currentChatId }).toArray(),
                        finalResultsCol.find({ chatId: currentChatId }).toArray(),
                        superfinalResultsCol.find({ chatId: currentChatId }).toArray(),
                    ]);

                    const initialData = {
                        chats: chats.map(c => ({
                            chatId: c.chatId,
                            tournamentName: c.tournamentName,
                        })),
                        currentChatId,
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
    .header-toggle {
      display: flex;
      align-items: center;
      gap: 4px;
      font-size: 13px;
      margin-left: auto;
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
        ${initialData.chats
          .map(c => `<option value="${c.chatId}" ${c.chatId === initialData.currentChatId ? 'selected' : ''}>${c.tournamentName}</option>`)
          .join('')}
      </select>

      <span class="header-metrics-note">
        Метрики: Frags, Deaths, Efficiency (avg), FPH (avg), Dmg Given, Dmg Received
      </span>

      <label class="header-toggle" title="Растянуть контент на всю ширину окна">
        <input type="checkbox" id="fullwidth-toggle" />
        На всю ширину
      </label>
    </div>

    <nav class="main-nav">
      <a href="#section-group-results">Квалификации</a>
      <a href="#section-final-results">Финалы</a>
      <a href="#section-superfinal-results">Суперфиналы</a>
      <a href="#section-overall">Общие графики</a>
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

    <section id="section-win-streaks">
      <h2>Серии из 3+ побед подряд</h2>
      <div class="metric-note">
        Учитываются серии, где игрок в рамках одной группы и одной стадии (Квалификации / Финалы / Суперфинал)
        занимает первое место на карте 3 и более раз подряд (по времени matchDateTime).
      </div>
      <div id="win-streaks-content"></div>

      <h3>Серии из 3+ побед подряд (сквозные)</h3>
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

    // Переключение турнира
    (function setupChatSelect() {
      const select = document.getElementById('chat-select');
      if (!select) return;

      select.addEventListener('change', () => {
        const chatId = select.value;
        const url = new URL(window.location.href);

        // основной параметр — tournamentId
        url.searchParams.set('tournamentId', chatId);
        // на всякий случай убираем старый chatId
        url.searchParams.delete('chatId');

        window.location.href = url.toString();
      });
    })();

    // Общая утилита агрегации
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

    function createCanvas(parent, width = '100%', height = '320px') {
      const wrap = document.createElement('div');
      wrap.className = 'chart-container';
      const canvas = document.createElement('canvas');
      wrap.appendChild(canvas);
      parent.appendChild(wrap);
      return canvas;
    }

    function buildStageSection(containerId, titlePrefix, docs) {
      const container = document.getElementById(containerId);
      container.innerHTML = '';

      if (!docs.length) {
        const note = document.createElement('div');
        note.className = 'empty-note';
        note.textContent = 'Нет данных для выбранного турнира.';
        container.appendChild(note);
        return;
      }

      const groupIds = unique(docs.map(d => d.groupId).filter(v => v !== undefined));
      groupIds.sort((a, b) => a - b);

      for (const groupId of groupIds) {
        const groupDocs = docs.filter(d => d.groupId === groupId);
        if (!groupDocs.length) continue;

        const groupDiv = document.createElement('div');
        groupDiv.className = 'group-wrapper';
        container.appendChild(groupDiv);

        const h3 = document.createElement('h3');
        h3.textContent = titlePrefix + ' ' + groupId;
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

        for (const metric of METRICS) {
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

      for (const metric of METRICS) {
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
        tdPlayer.textContent = s.playerName;
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
        tdPlayer.textContent = s.playerName;
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

      buildStageSection('group-results-content', 'Группа', groupResults);
      buildStageSection('final-results-content', 'Финальная группа', finalResults);
      buildStageSection('superfinal-results-content', 'Суперфинальная группа', superfinalResults);

      const allDocs = groupResults.concat(finalResults, superfinalResults);
      buildOverallSection('overall-content', allDocs);

      // Серии побед в разрезе стадий
      buildWinStreaksSection('win-streaks-content', groupResults, finalResults, superfinalResults);
      // Сквозные серии
      buildCrossWinStreaksSection('cross-win-streaks-content', groupResults, finalResults, superfinalResults);

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