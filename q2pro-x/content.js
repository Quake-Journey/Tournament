// q2pro-x/content.js
//
// Single source of truth for every visible string on the /q2pro-x page.
// Both `ru` and `en` variants live here; rendering code only reads from
// this file. Filling real copy later means editing this file, not the
// HTML / CSS / behaviour layers.
//
// Shape:
//   ui.*         — navigation labels, footer, modal chrome, switcher text
//   hero.*       — landing block above section nav
//   sections[]   — ordered list of anchored feature sections
//   docs.*       — per-language documentation URLs for the release guide set
//   meta.*       — <title> + <meta name="description"> per language
//
// Each section entry:
//   {
//     id,                         // anchor id, also HTML id
//     slug,                       // media folder name under media/
//     icon,                       // short unicode glyph for the section nav
//     title:    { ru, en },
//     summary:  { ru, en },       // one-line under title
//     body:     { ru: [...], en: [...] },  // paragraphs
//     bullets:  { ru: [...], en: [...] },  // optional
//     callout:  { ru, en } | null,         // optional
//     images:   [ { file, alt:{ru,en}, caption:{ru,en} }, ... ]
//   }
//
// Image paths resolve to /q2pro-x/media/<slug>/<file> so images can be
// dropped straight into O:\Claude\q2pro-x\media\<slug>\ folders with the
// names referenced here — the render layer never hardcodes them.

const fs = require('fs');
const path = require('path');

const ui = {
  brand:       { ru: 'Q2PRO-X',                 en: 'Q2PRO-X' },
  tagline:     { ru: 'умный клиент Quake II',   en: 'a smart Quake II client' },
  langLabel:   { ru: 'Язык',                    en: 'Language' },
  themeLabel:  { ru: 'Тема',                    en: 'Theme' },
  themeAuto:   { ru: 'Авто',                    en: 'Auto' },
  themeDark:   { ru: 'Тёмная',                  en: 'Dark' },
  themeLight:  { ru: 'Светлая',                 en: 'Light' },
  skip:        { ru: 'К содержимому',           en: 'Skip to content' },
  docsCta:     { ru: 'Документация',            en: 'Documentation' },
  downloadCta: { ru: 'Скачать',                 en: 'Download' },
  releaseCta:  { ru: 'Скачать 1.5',             en: 'Download 1.5' },
  betaCta:     { ru: 'Бета-версии',             en: 'Beta Builds' },
  sourcesCta:  { ru: 'Исходники',               en: 'Sources' },
  previewCta:  { ru: 'Открыть',                 en: 'Open' },
  telegramCta: { ru: 'Telegram',                en: 'Telegram' },
  navToggle:   { ru: 'Меню разделов',           en: 'Section menu' },
  modalClose:  { ru: 'Закрыть (Esc)',           en: 'Close (Esc)' },
  modalFullscreen: { ru: 'Во весь экран',       en: 'Fullscreen' },
  modalPrev:   { ru: 'Предыдущее',              en: 'Previous' },
  modalNext:   { ru: 'Следующее',               en: 'Next' },
  copyright:   { ru: 'Quake II — товарный знак id Software.',
                 en: 'Quake II is a trademark of id Software.' },
  author:      { ru: '© Q2PRO-X — проект ly',   en: '© Q2PRO-X by ly' },
  builtWith:   { ru: 'Сайт собран на Node.js + Express, без фреймворков.',
                 en: 'Built with Node.js + Express — no frameworks.' },
};

const navItems = [
  { id: 'history', href: '/q2pro-x/history', icon: '▤', label: { ru: 'История Q2PRO-X', en: 'Q2PRO-X History' } },
  { id: 'mapgen', href: '/mapgen', icon: '▧', iconSrc: '/mapgen/assets/mapgen_studio.ico', label: { ru: 'MAPGEN Studio', en: 'MAPGEN Studio' } },
  { id: 'botstats', href: 'https://q2pro-x.com/botstats/', icon: '▥', label: { ru: 'Статистика ботов', en: 'Bot statistics' } },
  { id: 'avfx', icon: '☔', label: { ru: 'AVFX 1.5', en: 'AVFX 1.5' } },
  { id: 'avfx-presets', icon: '▤', label: { ru: 'Пресеты и генератор', en: 'Presets & Generator' } },
  { id: 'network-play', icon: '↔', label: { ru: 'Бог сети', en: 'Network God' } },
  { id: 'startup-intro', icon: '✦', label: { ru: 'Startup Intro', en: 'Startup Intro' } },
  { id: 'demos', icon: '▶', label: { ru: 'Статистика и демки', en: 'Statistics & Demos' } },
  { id: 'overlays', icon: '▦', label: { ru: 'Таймеры и Performance', en: 'Timers & Performance' } },
  { id: 'interface', icon: '▣', label: { ru: 'Интерфейс и CVAR', en: 'Interface & CVAR' } },
  { id: 'single-player-mods', icon: '◆', label: { ru: 'Single Player и моды', en: 'Single Player & Mods' } },
];

const hero = {
  eyebrow: {
    ru: 'SMART QUAKE II CLIENT · RELEASE 1.5',
    en: 'SMART QUAKE II CLIENT · RELEASE 1.5',
  },
  title: {
    ru: 'Q2PRO-X — когда из Quake II хочется выжать больше.',
    en: 'Q2PRO-X — when you want more out of Quake II.',
  },
  subtitle: {
    ru: 'Когда на пинге дёргает камеру, выстрел визуально запаздывает, нужный cvar не найти, демки неудобно смотреть, а для голоса и перевода приходится держать внешние костыли — Q2PRO-X закрывает именно эти боли внутри клиента.',
    en: 'When ping makes the view twitch, weapon feedback arrives late, the cvar you need is buried, demos are awkward to review, and voice or translation still depend on external workarounds — Q2PRO-X brings those tools into the client.',
  },
  releaseNote: {
    ru: 'Версия 1.5 превращает Q2PRO-X в цельную платформу: AVFX-миры с материалами поверхностей, космосом, Чёрной дырой и генератором пресетов; Бог сети с адаптацией пинга; статистика игр и демок; Startup Intro; живые таймеры; Performance HUD и расширенный Single Player.',
    en: 'Version 1.5 turns Q2PRO-X into a complete platform: AVFX worlds with surface materials, space, a Black Hole, and a preset generator; Network God with Ping Adaptation; game and demo statistics; Startup Intro; live item timers; Performance HUD; and expanded Single Player.',
  },
  birthday: {
    icon: '✦',
    label: {
      ru: 'Рождение Q2PRO-X',
      en: 'Q2PRO-X birthday',
    },
    value: {
      ru: '29.03.2026 · 00:05',
      en: '2026-03-29 · 00:05',
    },
  },
  pillars: {
    ru: [
      { title: 'Бог сети', desc: 'Диагностика, Ping Adaptation, Jitter/Guardian и Network Lab объясняют состояние соединения человеческим языком.' },
      { title: 'Оружие отзывчивее', desc: 'Weapon Predict показывает локальные эффекты выстрела сразу, а LAGHAX смягчает сетевые коррекции.' },
      { title: 'AVFX-миры', desc: 'Погода, материалы поверхностей, рисунки, объёмный дым, зеркала, космос и Чёрная дыра работают внутри клиента.' },
      { title: 'Пресеты и генератор', desc: 'Готовые профили и управляемая генерация собирают совместимые AVFX-сцены без ручной настройки каждого параметра.' },
      { title: 'Статистика игры', desc: 'Локальная библиотека демок превращается в личную статистику матчей, карт, соперников и результатов.' },
      { title: 'Startup Intro', desc: 'Полноэкранное локализованное интро, музыка, логотип, молнии, салюты и отдельные настройки состава.' },
      { title: 'Инструменты внутри клиента', desc: 'CVAR Browser, консольные команды, Voice Chat, перевод, браузеры серверов и демок доступны без внешних утилит.' },
      { title: 'Живой Single Player', desc: 'Свободный мир, реакции змей, DM-тренировка, SP-мутаторы и 32-bit bridge расширяют локальную игру и моды.' },
    ],
    en: [
      { title: 'Network God', desc: 'Diagnostics, Ping Adaptation, Jitter/Guardian, and Network Lab explain connection health in plain language.' },
      { title: 'More responsive weapons', desc: 'Weapon Predict shows local fire feedback immediately while LAGHAX softens network corrections.' },
      { title: 'AVFX worlds', desc: 'Weather, surface materials, drawings, volumetric smoke, mirrors, space, and the Black Hole run inside the client.' },
      { title: 'Presets and a generator', desc: 'Built-in profiles and guided generation assemble compatible AVFX scenes without tuning every control by hand.' },
      { title: 'Game Statistics', desc: 'Your local demo library becomes personal match, map, opponent, and result statistics.' },
      { title: 'Startup Intro', desc: 'A localized full-screen intro with music, logo lighting, electricity, fireworks, and per-part controls.' },
      { title: 'Tools inside the client', desc: 'CVAR Browser, console commands, Voice Chat, translation, server browsing, and demo tools need no external utilities.' },
      { title: 'Living Single Player', desc: 'Free World behavior, snake reactions, DM training, SP mutators, and the 32-bit bridge expand local play and mods.' },
    ],
  },
};

const sections = [
  /* ───────────────────────── OVERVIEW ───────────────────────── */
  {
    id: 'overview',
    slug: 'home',
    icon: '⌂',
    title: {
      ru: 'Что такое Q2PRO-X',
      en: 'What Q2PRO-X is',
    },
    summary: {
      ru: 'Умный клиент Quake II на базе Q2PRO: сеть, AVFX, демки, статистика, интерфейс, Single Player и моды в одной сборке.',
      en: 'A smart Q2PRO-based Quake II client that unifies networking, AVFX, demos, statistics, interface, Single Player, and mods.',
    },
    body: {
      ru: [
        'Q2PRO-X — модифицированный клиент Q2PRO, в который встроены функции, обычно требующие сторонних утилит, скриптов и ручной правки конфигов. В 1.5 прежняя основа сохранена целиком и дополнена крупнейшим релизным слоем проекта: Богом сети, адаптацией пинга, статистикой игр, Startup Intro и расширенной системой AVFX.',
        'Это всё тот же Quake II и те же серверы, но теперь клиент умеет строить сложные AVFX-миры, генерировать совместимые пресеты, анализировать локальные демки, показывать живые таймеры и бюджеты производительности, а также расширять одиночную игру — без отказа от LAGHAX, weapon predict, Voice Chat, перевода, HDR, подсветок, браузеров и модов.',
        'Проект полностью совместим с существующими Q2-серверами и протоколом — ничего не меняется на стороне сервера. Все улучшения живут в клиенте и могут быть включены/выключены через меню, cvar browser или консоль.',
      ],
      en: [
        'Q2PRO-X is a modified Q2PRO client that brings in-tree the features that normally require external utilities, config scripting, and manual setup. Version 1.5 preserves the complete established foundation and adds the project’s largest release layer yet: Network God, Ping Adaptation, Game Statistics, Startup Intro, and a greatly expanded AVFX system.',
        'It is still Quake II and still connects to ordinary servers, but the client can now build complex AVFX worlds, generate compatible presets, analyze a local demo library, show live item timers and performance budgets, and expand local Single Player—without giving up LAGHAX, weapon prediction, Voice Chat, translation, HDR, highlights, browsers, or mod support.',
        'Fully compatible with existing Q2 servers and the vanilla protocol. No server-side changes required. Every improvement lives in the client and can be toggled via menu, cvar browser, or console.',
      ],
    },
    bullets: {
      ru: [
        'Полная совместимость с любыми Q2-серверами (vanilla protocol 34, R1Q2 35, Q2PRO 36).',
        'Настройки разделены на global, mod-related, demo visual и Q2PRO-X local scope; 1.5 сохраняет это разделение для новых сетевых, AVFX, Intro и статистических систем.',
        'Бинго RU / EN: меню, cvar help, автотранслит и `/en`-перевод встроены системно, а не по остаточному принципу.',
      ],
      en: [
        'Fully compatible with any Q2 server (vanilla protocol 34, R1Q2 35, Q2PRO 36).',
        'Global, mod-related, demo visual, and Q2PRO-X local scopes keep demos and mods from fighting each other; 1.5 extends the same contract to its new network, AVFX, Intro, and statistics systems.',
        'Bilingual RU / EN: menus, cvar help, auto-translit, and `/en` translation are built in systematically.',
      ],
    },
    callout: {
      ru: 'Сервер всегда авторитетен. Q2PRO-X только лучше показывает и предсказывает то, что уже происходит.',
      en: 'The server remains authoritative. Q2PRO-X just visualises and predicts what is already happening, more cleanly.',
    },
    images: [
      { file: '01-overview.jpg',  alt: { ru: 'Q2PRO-X в игре',                   en: 'Q2PRO-X in-game' },
                                  caption: { ru: 'Общий вид клиента',            en: 'Client overview' } },
      { file: '02-brand.jpg',     alt: { ru: 'Фирменный стиль Q2PRO-X',          en: 'Q2PRO-X branding' },
                                  caption: { ru: 'Логотип и идентификация',      en: 'Logo and identity' } },
      { file: 'hero_avfx_rainstorm_ultra_q2dm1.jpg', alt: { ru: 'AVFX дождь и гроза на q2dm1', en: 'AVFX rain and storm on q2dm1' },
                                  caption: { ru: 'Визуальный слой Q2PRO-X: дождь, лужи, гроза, HDR и пресеты качества', en: 'The Q2PRO-X visual layer: rain, puddles, storm, HDR, and quality presets' } },
      { file: 'hero_avfx_snow_ultra_q2dm1.jpg', alt: { ru: 'AVFX снег на q2dm1', en: 'AVFX snow on q2dm1' },
                                  caption: { ru: 'Снежный профиль AVFX с накоплением снега', en: 'AVFX snow profile with surface accumulation' } },
    ],
  },

  /* ───────────────────────── AVFX ───────────────────────── */
  {
    id: 'avfx',
    slug: 'avfx',
    icon: '☔',
    title: {
      ru: 'AVFX 1.5 — погода, поверхности и космос',
      en: 'AVFX 1.5 — weather, surfaces, and space',
    },
    summary: {
      ru: 'От дождя и снега до живых материалов, рисунков, объёмного дыма, зеркал, Земли и Чёрной дыры.',
      en: 'From rain and snow to living materials, drawings, volumetric smoke, mirrors, Earth, and the Black Hole.',
    },
    body: {
      ru: [
        'Погодная основа сохранена полностью: дождь, гроза, лужи, стоки воды, снег с накоплением, пар от лавы, мокрая линза, подводные эффекты и реакции оружия. К ней добавлены объекты и трава на BSP-поверхностях, покрытия и рисунки, объёмный пар/дым и живые материалы: плесень, вода, зелень, трещины, кирпичи и живность.',
        'Небо стало отдельным миром: Земля и Луна, Солнце, планеты, метеоры, кометы, корабли, облака, лучи и Чёрная дыра с линзированием и захватом материи. Небесные тела и небесный трафик настраиваются независимо, поэтому выбор Солнца больше не включает метеоры или корабли.',
        'Native WGL поддерживает тесселяцию архитектуры и жидкостей; зеркальные поверхности отражают сцену, модели, материалы и небесные объекты. EGL остаётся полноценным бэкендом со всеми общими AVFX-системами, кроме WGL-only аппаратной тесселяции.',
      ],
      en: [
        'The complete weather foundation remains: rain, storms, puddles, runoff, accumulating snow, lava steam, wet-lens effects, underwater rendering, and weapon reactions. Version 1.5 adds BSP surface objects and grass, coverings and drawings, volumetric steam/smoke, and living materials such as mold, wall water, greenery, cracks, bricks, and creatures.',
        'The sky is now a world of its own: Earth and Moon, Sun, planets, meteors, comets, ships, clouds, rays, and a Black Hole with lensing and matter capture. Celestial bodies and sky traffic are independent, so selecting the Sun no longer enables meteors or spaceships.',
        'Native WGL supports architecture and liquid tessellation; mirror surfaces reflect the scene, models, materials, and celestial objects. EGL remains a fully supported backend for every shared AVFX system except WGL-only hardware tessellation.',
      ],
    },
    bullets: {
      ru: [
        'Погода: дождь, гроза, лужи, стоки, снег, лёд, пепел, туман, облака и реакции на оружие.',
        'Поверхности: трава и объекты, живые материалы, покрытия, рисунки и объёмный пар/дым.',
        'Космос: Земля, Луна, Солнце, планеты, Чёрная дыра, метеоры, кометы и корабли.',
        'Геометрия: WGL-тесселяция стен, пола, воды и лавы; зеркальные полы, склоны и стены.',
        'Поставляемая библиотека пресетов и генератор AVFX с отдельными селекторами небесных тел и небесного трафика.',
      ],
      en: [
        'Weather: rain, storms, puddles, runoff, snow, ice, ash, fog, clouds, and weapon reactions.',
        'Surfaces: grass and objects, living materials, coverings, drawings, and volumetric steam/smoke.',
        'Space: Earth, Moon, Sun, planets, the Black Hole, meteors, comets, and ships.',
        'Geometry: WGL tessellation for walls, floors, water, and lava; mirrored floors, slopes, and walls.',
        'A shipped preset library and the AVFX generator with independent celestial-body and sky-traffic selectors.',
      ],
    },
    callout: {
      ru: 'AVFX по умолчанию выключен у новых пользователей; пресеты включаются вручную из меню, чтобы классический вид Quake II оставался безопасным дефолтом.',
      en: 'AVFX is off by default for new users; presets are enabled manually from the menu so the classic Quake II look remains the safe default.',
    },
    images: [
      { file: '1-5-surface-world.jpg', fit: 'cover', alt: { ru: 'AVFX-материалы и объёмные эффекты 1.5', en: 'Q2PRO-X 1.5 AVFX materials and volumetrics' },
                                  caption: { ru: 'Главный кадр мира 1.5: поверхности, растительность и объёмный пар/дым', en: 'The main 1.5 world shot: surfaces, vegetation, and volumetric steam/smoke' } },
      { file: '1-5-space-black-hole.jpg', fit: 'cover', alt: { ru: 'Космос и Чёрная дыра AVFX 1.5', en: 'Q2PRO-X 1.5 AVFX space and Black Hole' },
                                  caption: { ru: 'Космический слой 1.5: небесные тела, трафик и Чёрная дыра', en: 'The 1.5 space layer: celestial bodies, traffic, and the Black Hole' } },
      { file: 'hero_avfx_rainstorm_ultra_q2dm1.jpg', alt: { ru: 'Дождь и гроза AVFX на Q2DM1', en: 'AVFX rain and storm on Q2DM1' },
                                  caption: { ru: 'Дождь, гроза, лужи и изменённое небо', en: 'Rain, storm, puddles, and a transformed sky' } },
      { file: 'hero_avfx_snow_ultra_q2dm1.jpg', alt: { ru: 'Снег AVFX на Q2DM1', en: 'AVFX snow on Q2DM1' },
                                  caption: { ru: 'Снег с накоплением на поверхности', en: 'Snow accumulation on surfaces' } },
      { file: 'avfx_rainstorm_lightning_sky.jpg', alt: { ru: 'Гроза и небо AVFX', en: 'AVFX storm and sky' },
                                  caption: { ru: 'Грозовой профиль: облака, дождь, молнии и мокрая атмосфера', en: 'Storm profile: clouds, rain, lightning, and wet atmosphere' } },
      { file: 'avfx_puddles_runoff_flow.jpg', alt: { ru: 'Стекание воды и лужи AVFX', en: 'AVFX puddle runoff' },
                                  caption: { ru: 'Flow дождя: лужи растут и соединяются по геометрии уровня', en: 'Rain flow: puddles grow and connect across level geometry' } },
      { file: 'avfx_underwater_surface_rings.jpg', alt: { ru: 'Кольца на воде из-под поверхности', en: 'Underwater view of surface rings' },
                                  caption: { ru: 'Кольца, пена и поверхность воды видны из-под воды с размытием', en: 'Rings, foam, and the water surface are visible from below with blur' } },
      { file: 'avfx_snow_parallax_surface_closeup.jpg', alt: { ru: 'Крупный план снежного покрытия', en: 'Snow surface close-up' },
                                  caption: { ru: 'Снежный материал, объём и детализация поверхности', en: 'Snow material, volume, and surface detail' } },
      { file: 'avfx_weapon_impacts_water.jpg', alt: { ru: 'Попадания оружия по воде', en: 'Weapon impacts on water' },
                                  caption: { ru: 'Всплески, пар и пятна от оружия на воде и лужах', en: 'Splashes, steam, and marks from weapon impacts on water and puddles' } },
      { file: 'avfx_weapon_impacts_snow.jpg', alt: { ru: 'Попадания оружия по снегу', en: 'Weapon impacts on snow' },
                                  caption: { ru: 'Следы, кратеры, снежная пыль и пар от попаданий по снегу', en: 'Marks, craters, snow dust, and steam from impacts on snow' } },
      { file: 'ui_avfx_presets_menu.jpg', alt: { ru: 'Меню пресетов AVFX', en: 'AVFX presets menu' },
                                  caption: { ru: 'Готовые профили качества AVFX прямо из меню', en: 'Ready-made AVFX quality profiles from the menu' } },
    ],
  },

  /* ───────────────────────── HDR ───────────────────────── */
  {
    id: 'hdr',
    slug: 'hdr',
    icon: '☀',
    title: {
      ru: 'HDR и настоящий HDR-вывод',
      en: 'HDR and true HDR output',
    },
    summary: {
      ru: 'HDR-рендер, true HDR output, nits-калибровка, WGL/EGL различия и отдельная защита EGL от пересвета.',
      en: 'HDR rendering, true HDR output, nits calibration, WGL/EGL differences, and the EGL overexposure guard.',
    },
    body: {
      ru: [
        'Q2PRO-X 1.5 включает отдельный HDR-пайплайн: `gl_hdr` включает внутренний HDR-рендер, а `gl_hdr_output` включает настоящий HDR-сигнал на дисплей, если backend и Windows/display path это поддерживают.',
        'Для лучшего HDR-эффекта рекомендуется backend `win32wgl`: он обычно даёт более яркую картинку без потери деталей в лампах, солнце и луне. `win32egl` тоже поддерживает HDR output, но требует включённого HDR в Windows и имеет отдельную настройку `gl_hdr_egl_app_white_max` для защиты деталей от пересвета.',
        'Пользователь настраивает `Peak brightness` под реальный peak монитора, `Paper white` под комфорт обычного белого, tone mapping под вкус, а для HDR-скриншотов использует `screenshotjxr` / `screenshotjxr_raw`.',
      ],
      en: [
        'Q2PRO-X 1.5 includes a dedicated HDR pipeline: `gl_hdr` enables internal HDR rendering, while `gl_hdr_output` enables a real HDR signal to the monitor when the backend and Windows/display path support it.',
        'For the best HDR effect, use the `win32wgl` backend: it generally gives a brighter image without losing detail in lamps, sun, and moon. `win32egl` also supports HDR output, but needs Windows HDR enabled and uses `gl_hdr_egl_app_white_max` to protect detail from overexposure.',
        'Users tune `Peak brightness` to the monitor peak, `Paper white` to ordinary-white comfort, tone mapping to taste, and use `screenshotjxr` / `screenshotjxr_raw` for HDR screenshots.',
      ],
    },
    bullets: {
      ru: [
        '`/HDR` открывает меню Video and Display -> HDR Settings.',
        '`gl_hdr_info` печатает backend/caps/scene-format диагностику.',
        '`gl_hdr_egl_app_white_max` действует только на win32egl; win32wgl его игнорирует.',
      ],
      en: [
        '`/HDR` opens Video and Display -> HDR Settings.',
        '`gl_hdr_info` prints backend/capability/scene-format diagnostics.',
        '`gl_hdr_egl_app_white_max` affects win32egl only; win32wgl ignores it.',
      ],
    },
    callout: {
      ru: 'Подробная настройка вынесена в отдельный документ HDR Guide 1.5.',
      en: 'Detailed tuning lives in the dedicated 1.5 HDR Guide.',
    },
    images: [
      { file: 'hdr_wgl_scene.jpg', alt: { ru: 'HDR-сцена в win32wgl', en: 'HDR scene in win32wgl' },
                                  caption: { ru: 'Настоящий HDR-вывод с яркими пиками без потери деталей', en: 'True HDR output with bright peaks and preserved detail' } },
      { file: 'hdr_settings_menu.jpg', alt: { ru: 'Меню настроек HDR', en: 'HDR settings menu' },
                                  caption: { ru: 'Peak brightness, paper white и EGL app white в меню HDR', en: 'Peak brightness, paper white, and EGL app white in the HDR menu' } },
    ],
  },

  /* ───────────────────────── AVFX PRESETS ───────────────────────── */
  {
    id: 'avfx-presets',
    slug: 'avfx-presets',
    icon: '▤',
    title: {
      ru: 'AVFX Presets & Generator',
      en: 'AVFX Presets & Generator',
    },
    summary: {
      ru: 'Поставляемая библиотека пресетов, пользовательские слоты и управляемый генератор совместимых AVFX-миров.',
      en: 'A shipped preset library, personal slots, and a guided generator for compatible AVFX worlds.',
    },
    body: {
      ru: [
        'Библиотека AVFX-пресетов объединяет готовые визуальные профили для разных сцен и задач и может расширяться вместе с клиентом без изменения привычного способа выбора и загрузки.',
        'Генератор использует детерминированный seed, уровни качества и хаоса, область покрытия поверхностей, цветовые и формовые сочетания и итоговую сводку. Независимые группы эффектов действуют только после команды Generate; «Небесные тела» ограничены Солнцем, планетами/Землёй и Чёрной дырой, а метеоры, кометы и корабли вынесены в отдельный мастер «Метеоры и корабли». Материалы поверхностей и живность управляются отдельными мастерами «Рандом / Всегда / Выключено», поэтому их можно гарантированно включить или полностью исключить из генерации; зона покрытия по-прежнему настраивается независимо.',
        'Пользовательские слоты позволяют загружать, сохранять и переименовывать личные варианты из меню, не затрагивая штатные cfg.',
      ],
      en: [
        'The AVFX preset library brings together ready-made visual profiles for different scenes and purposes, and it can grow with the client without changing the familiar selection and loading workflow.',
        'The generator uses a deterministic seed, quality and chaos levels, surface coverage, color and shape combinations, independently configurable effect groups, and a final summary. Changes remain inert until Generate is pressed; Celestial Bodies is limited to the Sun, planets/Earth, and the Black Hole, while meteors, comets, and spaceships live under the separate Meteors and Ships master. Surface Materials and Wildlife have independent Random / Always / Off masters, so either family can be guaranteed or excluded while the coverage zone remains independently configurable.',
        'Personal slots let players load, save, and rename their own variants from the menu without modifying the shipped cfg files.',
      ],
    },
    bullets: {
      ru: [
        'Генерация: seed, качество, хаос, покрытие, цвет/форма и независимо настраиваемые группы эффектов.',
        'Небесные тела и небесный трафик разделены; режим Random у трафика является безопасным дефолтом.',
        'Материалы поверхностей и живность имеют независимые режимы Рандом, Всегда и Выключено.',
        'Библиотека организована для быстрого выбора подходящего визуального профиля и может свободно расширяться.',
        'Пользовательские слоты поддерживают загрузку, сохранение и переименование личных вариантов.',
      ],
      en: [
        'Generation controls: seed, quality, chaos, coverage, color/shape, and independently configurable effect groups.',
        'Celestial bodies and sky traffic are separate; Random is the safe default for the traffic master.',
        'Surface Materials and Wildlife have independent Random, Always, and Off modes.',
        'The library is organized for quick visual-profile selection and can expand freely.',
        'Personal slots support loading, saving, and renaming custom variants.',
      ],
    },
    callout: {
      ru: 'Генератор не меняет активную сцену до нажатия Generate; результат можно проверить по сводке и сохранить в пользовательский слот.',
      en: 'The generator does not change the active scene until Generate is pressed; review the summary, then save the result to a personal slot.',
    },
    images: [
      { file: '1-5-presets-categories.jpg', fit: 'cover', alt: { ru: 'Меню библиотеки AVFX-пресетов Q2PRO-X 1.5', en: 'Q2PRO-X 1.5 AVFX preset library menu' },
                                  caption: { ru: 'Библиотека готовых AVFX-профилей и переход к пользовательским пресетам', en: 'The ready-made AVFX profile library and access to personal presets' } },
      { file: '1-5-user-presets.jpg', fit: 'cover', alt: { ru: 'Пользовательские пресеты AVFX', en: 'Personal AVFX presets' },
                                  caption: { ru: 'Пример пользовательских пресетов с именами и действиями загрузки, сохранения и переименования', en: 'Personal presets with editable names and load, save, and rename actions' } },
      { file: '1-5-generator-main.jpg', fit: 'cover', alt: { ru: 'Главная страница генератора AVFX 1.5', en: 'Q2PRO-X 1.5 AVFX Preset Generator main page' },
                                  caption: { ru: 'Главный экран генератора: качество, хаос, материалы поверхностей, живность и независимые группы эффектов', en: 'Generator overview: quality, chaos, surface materials, wildlife, and independent effect groups' } },
    ],
  },

  /* ───────────────────────── LAGHAX ───────────────────────── */
  {
    id: 'laghax',
    slug: 'laghax',
    icon: '~',
    title: {
      ru: 'LAGHAX — адаптивное сглаживание',
      en: 'LAGHAX — adaptive smoothing',
    },
    summary: {
      ru: 'Сглаживает скачки коррекции предсказания, когда сеть пульсирует, без задержки на ровной сети.',
      en: 'Smooths prediction-error jumps when the network pulses, without adding latency on a clean link.',
    },
    body: {
      ru: [
        'Стандартный клиент Q2 резко телепортирует модель, когда приходит коррекция от сервера. На нестабильной сети это выглядит как дёрганье. LAGHAX наблюдает ошибку предсказания, пинг, джиттер и потери, и растягивает коррекцию во времени — но только когда это нужно.',
        'Режимы: `cl_laghax 0` (выкл), `1` (фиксированное окно), `2` (адаптивный, по умолчанию). Есть полнофункциональный HUD со статистикой, перетаскиванием, edit-mode, опциональными блоками `pred:` (включённые предикты), `phys:` (режим физики) и `rt:` (runtime-бухгалтерия предиктора оружия).',
      ],
      en: [
        'Stock Q2 clients snap the model when a server correction arrives. On a jittery link that reads as a twitchy view. LAGHAX watches prediction error, ping, jitter, and loss, and stretches the correction across time — but only when it actually needs to.',
        'Modes: `cl_laghax 0` (off), `1` (fixed window), `2` (adaptive, default). Ships with a full HUD showing live stats, drag-to-reposition, edit mode, and optional `pred:` / `phys:` / `rt:` diagnostic rows.',
      ],
    },
    bullets: {
      ru: [
        'Авто-адаптация к ping / jitter / loss / debt / gap без ручных подкрутки.',
        'HUD отображает live-статистику, стресс-метрику и breakdown причин.',
        'Опциональный блок `rt:` визуализирует runtime-состояние предиктора оружия.',
      ],
      en: [
        'Auto-adapts to ping / jitter / loss / debt / gap with no manual tuning.',
        'Live-stats HUD with stress metric and per-cause breakdown.',
        'Optional `rt:` block visualises the weapon predictor runtime state.',
      ],
    },
    callout: null,
    images: [
      { file: '01-hud.jpg',       alt: { ru: 'LAGHAX HUD в игре',              en: 'LAGHAX HUD in-game' },
                                  caption: { ru: 'LAGHAX HUD при активной коррекции', en: 'LAGHAX HUD during active correction' } },
      { file: '02-settings.jpg',  alt: { ru: 'Меню настроек LAGHAX',           en: 'LAGHAX settings menu' },
                                  caption: { ru: 'Настройки в guided-меню',    en: 'Settings in the guided menu' } },
      { file: '03-runtime.jpg',   alt: { ru: 'LAGHAX HUD с блоком rt:',        en: 'LAGHAX HUD with rt: block' },
                                  caption: { ru: 'Runtime-диагностика (опционально)', en: 'Runtime diagnostics (optional)' } },
    ],
  },

  /* ───────────────────────── WEAPON PREDICT ───────────────────────── */
  {
    id: 'weapon-predict',
    slug: 'weapon-predict',
    icon: '✦',
    title: {
      ru: 'Weapon Predict — предсказание выстрелов',
      en: 'Weapon Predict — fire prediction',
    },
    summary: {
      ru: 'Ракеты, railgun, hyperblaster, chaingun, shotgun и др. — плавная визуальная непрерывность без изменения серверной логики.',
      en: 'Rockets, railgun, hyperblaster, chaingun, shotgun, and more — smooth visual continuity without touching server logic.',
    },
    body: {
      ru: [
        'Клиент рендерит «ghost»-версии снарядов и эффектов сразу при выстреле, до подтверждения от сервера. Когда реальная сущность приходит, они соединяются через handoff, и пользователь не видит двойного рендера или телепорта.',
        'Это строго визуальный слой. Урон, хитрег, ammo authority — всё на сервере. Клиент просто показывает то, что всё равно произойдёт, с компенсацией сетевой задержки.',
      ],
      en: [
        'The client renders "ghost" versions of projectiles and effects right at fire time, before the server confirms them. When the real entity arrives, they are handed off seamlessly, so you never see a double render or a teleport.',
        'Strictly visual. Damage, hit-reg, ammo authority all live on the server. The client just shows you what is about to happen, latency-compensated.',
      ],
    },
    bullets: {
      ru: [
        'Предикты: rockets (+explosion), railgun, shotgun/SSG, machinegun, chaingun, blaster, grenades, hand grenades, hyperblaster.',
        'Ammo admission gate не даёт «призраку» выстрелить, если патрон уже потрачен.',
        'Muzzle-flash / звук выстрела подавляются на сервере, чтобы не было двойных FX.',
      ],
      en: [
        'Predicts: rockets (+explosion), railgun, shotgun/SSG, machinegun, chaingun, blaster, grenades, hand grenades, hyperblaster.',
        'An ammo-admission gate prevents the ghost from firing when the round has already been spent.',
        'Server muzzle-flash / fire sound is suppressed to avoid double FX.',
      ],
    },
    callout: {
      ru: 'Сервер остаётся единственным источником истины. Клиент никогда не «дорисовывает» несуществующие попадания.',
      en: 'The server is the sole source of truth. The client never invents hits that did not happen.',
    },
    images: [
      { file: '01-settings.jpg',  alt: { ru: 'Меню предсказания оружия',       en: 'Weapon predict settings' },
                                  caption: { ru: 'Настройки предиктов',         en: 'Per-weapon toggles' } },
      { file: '02-rocket.jpg',    alt: { ru: 'Ракета ghost → server',           en: 'Rocket ghost → server' },
                                  caption: { ru: 'Rocket handoff в действии',   en: 'Rocket handoff in action' } },
      { file: '03-runtime.jpg',   alt: { ru: 'rt: runtime-диагностика',         en: 'rt: runtime diagnostics' },
                                  caption: { ru: 'Runtime-счётчики предиктора', en: 'Predictor runtime counters' } },
    ],
  },

  /* ───────────────────────── MOVEMENT ───────────────────────── */
  {
    id: 'movement-physics',
    slug: 'movement-physics',
    icon: '⇌',
    title: {
      ru: 'Movement & Physics',
      en: 'Movement & Physics',
    },
    summary: {
      ru: 'Выбираемые профили ощущения движения плюс плавный шаг по лестницам и ступенькам.',
      en: 'Selectable movement-feel profiles plus smoothed stepping on stairs and ledges.',
    },
    body: {
      ru: [
        'Классический Q2, Q2PRO и R1Q2 предлагают разные варианты ощущения движения — у них немного отличается прогнозирование, шаг и subframe-физика. Q2PRO-X даёт выбрать нужный профиль одной настройкой вместо правки набора cvar.',
        'Плюс отдельный режим `fixedmove` для тех, кому важна детерминированная скорость повторения. Step-smoothing сглаживает «подпрыгивание» при подъёме по лестницам без изменения реального перемещения.',
      ],
      en: [
        'Classic Q2, Q2PRO, and R1Q2 each have their own subtly-different movement feel — prediction, stepping, and subframe physics all differ. Q2PRO-X lets you pick one via a single setting instead of editing a constellation of cvars.',
        'Plus a dedicated `fixedmove` mode for people who want a deterministic repeat cadence. Step smoothing damps the stair-popping without changing real movement.',
      ],
    },
    bullets: {
      ru: [
        '`cl_movement_feel_mode`: 0 = Q2PRO (new), 1 = R1Q2-like.',
        '`cl_step_smoothing_mode`: 0 = Q2PRO, 1..3 = варианты R1Q2.',
        '`cl_fixedmove` для фиксированной тактовой частоты.',
        'Legacy-предикт доступен через `cl_predict_move_mode 1` для совместимости.',
      ],
      en: [
        '`cl_movement_feel_mode`: 0 = Q2PRO (new), 1 = R1Q2-like.',
        '`cl_step_smoothing_mode`: 0 = Q2PRO, 1..3 = R1Q2 variants.',
        '`cl_fixedmove` for a fixed movement tick cadence.',
        'Legacy predict available via `cl_predict_move_mode 1` for compatibility.',
      ],
    },
    callout: null,
    images: [
      { file: '01-modes.jpg',     alt: { ru: 'Меню режимов движения',       en: 'Movement modes menu' },
                                  caption: { ru: 'Выбор movement feel mode', en: 'Movement feel mode selection' } },
      { file: '02-step.jpg',      alt: { ru: 'Step smoothing modes',         en: 'Step smoothing modes' },
                                  caption: { ru: 'Сглаживание ступенек',     en: 'Stair smoothing variants' } },
    ],
  },

  /* ───────────────────────── SOUND ───────────────────────── */
  {
    id: 'sound',
    slug: 'sound',
    icon: '♪',
    title: {
      ru: 'Sound & Acoustics',
      en: 'Sound & Acoustics',
    },
    summary: {
      ru: 'OpenAL, бинауральный 3D, реверберация EAX и HRTF — для наушников и больших комнат.',
      en: 'OpenAL, binaural 3D, EAX-style reverb, and HRTF — for headphones and big rooms alike.',
    },
    body: {
      ru: [
        'Q2PRO-X подключает OpenAL Soft и разворачивает полный набор пространственных настроек. Бинауральный 3D, HRTF, реверб, выбор устройства, автоопределение — всё в меню и с понятной русской справкой по cvar.',
        'Для игроков в наушниках это один из самых ощутимых визуально-невизуальных апгрейдов: прокидывание звука по координатам сцены делает позиционирование противника намного точнее.',
      ],
      en: [
        'Q2PRO-X wires up OpenAL Soft and exposes the full spatial-audio surface — binaural 3D, HRTF, reverb, device selection, auto-detection — via the menu, with proper cvar reference docs.',
        'If you play on headphones, this is one of the most perceptually-impactful upgrades: proper positional audio dramatically improves enemy localisation.',
      ],
    },
    bullets: {
      ru: [
        '`al_binaural`, `al_hrtf`, `al_eax` для управления пространственной моделью.',
        'Подстройка подводного lowpass-фильтра (`s_underwater_gain_hf`).',
        'Опция `s_auto_focus` — звук только при активном окне.',
      ],
      en: [
        '`al_binaural`, `al_hrtf`, `al_eax` for the spatial model.',
        'Underwater lowpass filter tuning via `s_underwater_gain_hf`.',
        '`s_auto_focus` — mute audio when the window is not focused.',
      ],
    },
    callout: null,
    images: [
      { file: '01-binaural.jpg',  alt: { ru: 'Меню binaural',                en: 'Binaural settings' },
                                  caption: { ru: 'Настройки бинаурального 3D', en: 'Binaural 3D settings' } },
      { file: '02-reverb.jpg',    alt: { ru: 'Меню EAX reverb',               en: 'EAX reverb menu' },
                                  caption: { ru: 'Пресеты реверберации',      en: 'Reverb presets' } },
    ],
  },

  /* ───────────────────────── VISUALS ───────────────────────── */
  {
    id: 'visuals',
    slug: 'visuals',
    icon: '◎',
    title: {
      ru: 'Visuals & Highlights',
      en: 'Visuals & Highlights',
    },
    summary: {
      ru: 'Управление читаемостью сцены: подсветка, видимость оружия и частей, тонкая настройка рендера.',
      en: 'Scene-readability controls: highlights, weapon / parts visibility, fine-grained render tuning.',
    },
    body: {
      ru: [
        'Для матчей важнее всего видеть противника, свой урон и projectile-потоки. Q2PRO-X даёт точечные ручки: прозрачность оружия в руках, подсветка сегментов моделей, bloom, gamma-масштаб, damage-blend intensity — всё с дефолтами, которые не ломают классический look.',
        'Подсветки включают отдельный слой model color overrides: можно красить конкретные модели игроков именованными цветами, а на демках использовать единый demo visual profile независимо от мода, в котором была записана демка.',
        'В 1.5 доступен отдельный пользовательский слой для MD2/MD5 model parts: можно красить голову, руки, торс, ноги и оружие в руках, выбирать общий или per-model набор цветов и сохранять всё это в visual/AVFX presets.',
      ],
      en: [
        'In-match you mostly need to see your opponent, your damage, and the projectile streams. Q2PRO-X exposes surgical knobs: held-weapon transparency, segmented model highlights, bloom, gamma scale, damage-blend intensity — all with defaults that do not alter the classic look.',
        'Highlights include model color overrides: individual player models can be colored with named colors, while demos can use one dedicated demo visual profile regardless of the mod that recorded them.',
        'Version 1.5 includes a dedicated user layer for MD2/MD5 model parts: head, arms, torso, legs, and carried weapon can be tinted separately, with shared or per-model color sets stored through visual/AVFX presets.',
      ],
    },
    bullets: {
      ru: [
        '`gl_bloom`, `gl_brightness`, `gl_saturation`, `gl_celshading`.',
        'Настройки подсветки и частей оружия отдельным меню.',
        'Player mode умеет team/duel-логику и принудительные model-color overrides для демок.',
        'Готовые пресеты видимости объединяют подсветки, render settings, AVFX-профили и model-part tint.',
        'Управление screen-blend при получении урона (`gl_damageblend_frac`).',
      ],
      en: [
        '`gl_bloom`, `gl_brightness`, `gl_saturation`, `gl_celshading`.',
        'Dedicated menu for highlights and weapon-part visibility.',
        'Player mode supports team/duel logic and forced model-color overrides for demos.',
        'Visibility presets combine highlights, render settings, AVFX profiles, and model-part tint.',
        'Damage screen-blend tuning via `gl_damageblend_frac`.',
      ],
    },
    callout: null,
    images: [
      { file: '01-highlights.jpg',alt: { ru: 'Меню подсветки',                en: 'Highlights menu' },
                                  caption: { ru: 'Настройки подсветки модели', en: 'Model highlight settings' } },
      { file: '02-visuals.jpg',   alt: { ru: 'Visual FX меню',                en: 'Visual FX menu' },
                                  caption: { ru: 'Полный набор визуальных настроек', en: 'Full visual tuning panel' } },
      { file: '03-item-player-highlights.jpg', alt: { ru: 'Подсветки в игре', en: 'In-game highlights' },
                                  caption: { ru: 'Item / player / HUD readability в живой сцене', en: 'Item / player / HUD readability in a live scene' } },
      { file: 'hero_visual_presets_highlights.jpg', alt: { ru: 'Подсветка и визуальный профиль', en: 'Highlights and visual profile' },
                                  caption: { ru: 'Подсветка моделей и визуальный профиль без AVFX-погоды', en: 'Model highlights and a visual profile without AVFX weather' } },
      { file: 'ui_visual_presets_menu.jpg', alt: { ru: 'Меню пресетов видимости', en: 'Visibility presets menu' },
                                  caption: { ru: 'Готовые пресеты видимости и визуального качества', en: 'Ready-made visibility and visual-quality presets' } },
      { file: 'visual_highlights_bloom_scene.jpg', alt: { ru: 'Подсветки и bloom в живой сцене', en: 'Highlights and bloom in a live scene' },
                                  caption: { ru: 'Живой профиль подсветки и визуальных эффектов без AVFX-погоды', en: 'Live highlights and visual-effects profile without AVFX weather' } },
    ],
  },

  /* ───────────────────────── VISUAL HIGHLIGHTS ───────────────────────── */
  {
    id: 'visual-highlights',
    slug: 'visual-highlights',
    icon: '◈',
    title: {
      ru: 'Подсветки и раскраска моделей',
      en: 'Highlights and model tinting',
    },
    summary: {
      ru: 'Предметы, оружие, игроки, team/duel logic и новая раскраска частей MD2/MD5 моделей на лету.',
      en: 'Items, weapons, players, team/duel logic, and new runtime MD2/MD5 model-part tinting.',
    },
    body: {
      ru: [
        'Подсветки Q2PRO-X остаются локальным визуальным слоем: они не меняют сервер, хитбоксы, урон, движение или сетевой протокол. В 1.5 предметы, оружие и игроки имеют отдельные стили, alpha, shell/tint, brightness, scale и цвета.',
        'Режим `Player mode = Model parts` позволяет вместо одного цвета по всей модели отдельно красить голову, руки, торс, ноги и оружие в руках. Поддерживаются MD2 и MD5 пути; для MD5 сохраняются модель, скелет, анимация и skin path.',
        'Для точной подгонки есть общий/per-model набор цветов, источник цвета оружия, alpha, fullbright, replace tint и body-zone параметры `head_start`, `legs_end`, `softness`.',
      ],
      en: [
        'Q2PRO-X highlights remain a local visual layer: they do not change the server, hitboxes, damage, movement, or network protocol. In 1.5, items, weapons, and players have dedicated style, alpha, shell/tint, brightness, scale, and color controls.',
        '`Player mode = Model parts` replaces a single whole-model color with separate head, arms, torso, legs, and carried-weapon tints. MD2 and MD5 paths are supported while preserving model, skeleton, animation, and skin path.',
        'Precise tuning includes shared/per-model color sets, weapon color source, alpha, fullbright, replace tint, and body-zone controls `head_start`, `legs_end`, `softness`.',
      ],
    },
    bullets: {
      ru: [
        '`cl_vis_enable` включает весь слой подсветок.',
        '`cl_vis_players_mode model parts` включает раскраску частей игрока.',
        '`cl_vis_player_parts_fullbright` даёт чистые цвета; `replace_tint` заменяет старую цельную заливку.',
        '`cl_vis_items_info` печатает диагностику классификации предметов.',
      ],
      en: [
        '`cl_vis_enable` enables the highlight layer.',
        '`cl_vis_players_mode model parts` enables player-part tinting.',
        '`cl_vis_player_parts_fullbright` gives raw colors; `replace_tint` replaces the old whole-player tint.',
        '`cl_vis_items_info` prints item-classification diagnostics.',
      ],
    },
    callout: {
      ru: 'Подробная настройка подсветок и model parts вынесена в Visual Highlights Guide 1.5.',
      en: 'Detailed highlight and model-part setup lives in the 1.5 Visual Highlights Guide.',
    },
    images: [
      { file: 'visual_highlights_model_parts.jpg', alt: { ru: 'Раскраска частей модели', en: 'Model-part tinting' },
                                  caption: { ru: 'Голова, руки, торс, ноги и оружие могут иметь разные цвета', en: 'Head, arms, torso, legs, and weapon can use separate colors' } },
      { file: 'visual_highlights_menu.jpg', alt: { ru: 'Меню цветов частей игрока', en: 'Player part colors menu' },
                                  caption: { ru: 'Model parts: alpha, fullbright, replace tint и body zones', en: 'Model parts: alpha, fullbright, replace tint, and body zones' } },
    ],
  },

  /* ───────────────────────── OVERLAYS ───────────────────────── */
  {
    id: 'overlays',
    slug: 'overlays',
    icon: '▦',
    title: {
      ru: 'Overlays, Timers & Performance',
      en: 'Overlays, Timers & Performance',
    },
    summary: {
      ru: 'Браузеры и HUD дополнены живыми таймерами предметов и контролем бюджетов CPU/GPU.',
      en: 'The built-in browsers and HUDs now include live item timers and CPU/GPU budget visibility.',
    },
    body: {
      ru: [
        'Q2PRO-X объединяет server browser, cvar browser, mod overlay, LAGHAX HUD, Performance HUD и живые таймеры предметов. Оверлеи настраиваются внутри клиента и не требуют внешних программ.',
        'Q2PRO-X очень ответственно относится к честной игре, поэтому автоматические таймеры по умолчанию запрещены на всех онлайн-серверах. Исключения задаются на серверном уровне через защищённое управление; администраторы серверов могут самостоятельно разрешить автоматические таймеры Q2PRO-X на своих серверах. Ручные таймеры при этом доступны всегда. Подробная схема работы и настройки описана в «Руководстве по живым таймерам» в разделе документации.',
        'Performance HUD выключен по умолчанию и показывает только активные подсистемы, их бюджеты и доступные CPU/GPU-метрики. Неактивные AVFX-секции не считаются ради самого оверлея.',
      ],
      en: [
        'Q2PRO-X brings together the server browser, cvar browser, mod overlay, LAGHAX HUD, Performance HUD, and live item timers. These tools are configured inside the client and need no external utility.',
        'Q2PRO-X treats fair play as a serious responsibility, so automatic timers are disabled by default on every online server. Exceptions are managed at server level through protected controls; server administrators can enable them for their own servers. Manual timers remain available. Operating and administration details are covered by the Live Item Timers Guide in the Documentation section.',
        'The default-off Performance HUD reports only active subsystems, their budgets, and available CPU/GPU metrics. Inactive AVFX sections are not measured merely for the overlay.',
      ],
    },
    bullets: {
      ru: [
        'Server browser с глобальными избранными, фильтром и сохранением выбранной вкладки.',
        'CVAR Browser: полный каталог переменных, публичные команды и отдельная Debug-вкладка.',
        'Живые таймеры: HUD, цвета, звуки, интервалы, бинды и безопасная авторизация автоматики.',
        'Performance HUD: активные AVFX-бюджеты, CPU/GPU-метрики и отдельный bind.',
      ],
      en: [
        'Server browser with global favourites, filtering, and persistent selected tab.',
        'CVAR Browser with a complete variable catalog, public commands, and a dedicated Debug tab.',
        'Live item timers with HUD, colors, sounds, intervals, binds, and safe automatic authorization.',
        'Performance HUD with active AVFX budgets, CPU/GPU metrics, and a dedicated bind.',
      ],
    },
    callout: null,
    images: [
      { file: '01-server-browser.jpg', alt: { ru: 'Server browser',          en: 'Server browser' },
                                       caption: { ru: 'Поиск и избранное',   en: 'Search + favourites' } },
      { file: '03-mod-overlay.jpg',    alt: { ru: 'Mod overlay',             en: 'Mod overlay' },
                                       caption: { ru: 'Быстрый переключатель модов', en: 'Fast mod switcher' } },
      { file: '04-laghax-overlay.jpg', alt: { ru: 'LAGHAX HUD overlay',      en: 'LAGHAX HUD overlay' },
                                       caption: { ru: 'LAGHAX HUD в матче',  en: 'LAGHAX HUD during a match' } },
    ],
  },

  /* ───────────────────────── INTERFACE ───────────────────────── */
  {
    id: 'interface',
    slug: 'interface',
    icon: '▣',
    title: {
      ru: 'Interface, Menu & Console',
      en: 'Interface, Menu & Console',
    },
    summary: {
      ru: 'Новое Guided-меню, отдельное меню для мультиплеерных игроков, полная карта разделов 1.5, области настроек, быстрые команды и безопасное сохранение.',
      en: 'The new Guided menu, a separate menu for multiplayer players, the full 1.5 section map, setting scopes, quick commands, and safe saving.',
    },
    body: {
      ru: [
        'Основной интерфейс настроек — новое Guided-меню; для мультиплеерных игроков предусмотрено отдельное меню. Размеры и прозрачность меню и консоли, язык, шрифты, браузеры и политика сохранения настраиваются отдельно.',
        'Версия 1.5 добавляет крупные разделы Бога сети, Startup Intro, статистики игр, живых таймеров, Performance HUD, AVFX-материалов и генератора пресетов, сохраняя HDR, подсветки, Voice Chat, демо-инструменты, Single Player и моды.',
        'Метки `(X)`, `(G)` и `(M)` объясняют область действия параметра. Глобальные, mod-related, demo visual и локальные настройки не смешиваются, а обязательная миграция старых конфигов сохраняет пользовательские значения.',
      ],
      en: [
        'The primary settings interface is the new Guided menu; a separate menu for multiplayer players is also available. Menu and console scale/alpha, language, fonts, browser layouts, and save policy are configured independently.',
        'Version 1.5 adds major pages for Network God, Startup Intro, Game Statistics, live item timers, Performance HUD, AVFX materials, and the preset generator while retaining HDR, highlights, Voice Chat, demo tools, Single Player, and mods.',
        'The `(X)`, `(G)`, and `(M)` badges explain setting scope. Global, mod-related, demo-visual, and local values remain separate, while mandatory legacy migration preserves user choices.',
      ],
    },
    bullets: {
      ru: [
        'Новое Guided-меню, отдельное меню для мультиплеерных игроков и полная RU/EN-навигация.',
        'Console/menu scale, alpha, fonts and readable layouts for large displays.',
        'Global / mod-related / demo visual / local scopes and explicit `(X)/(G)/(M)` badges.',
        'AVFX presets, generator, Startup Intro, statistics, network and performance controls remain reachable from menus and quick commands.',
      ],
      en: [
        'The new Guided menu, a separate menu for multiplayer players, and complete RU/EN navigation.',
        'Console/menu scale, alpha, fonts, and readable layouts for large displays.',
        'Global / mod-related / demo visual / local scopes with explicit `(X)/(G)/(M)` badges.',
        'AVFX presets, generator, Startup Intro, statistics, network, and performance controls remain reachable from menus and quick commands.',
      ],
    },
    callout: {
      ru: 'Главная идея интерфейса 1.5: крупные системы доступны из актуальных меню, но исходный облик Quake II и выключенные тяжёлые эффекты остаются безопасным дефолтом.',
      en: 'The 1.5 interface keeps major systems discoverable while the original Quake II look and disabled heavy effects remain the safe default.',
    },
    images: [
      { file: '01-modern-menu.jpg', alt: { ru: 'Guided-меню Q2PRO-X', en: 'Q2PRO-X Guided menu' },
                                   caption: { ru: 'Меню, auto-port scan и настройки в живом клиенте', en: 'Menu, auto-port scan, and settings in the live client' } },
      { file: '02-menu-console.jpg', alt: { ru: 'Карта interface-настроек', en: 'Interface settings map' },
                                   caption: { ru: 'Меню, консоль, области настроек и безопасное сохранение', en: 'Menu, console, setting scopes, and safe saving' } },
      { file: 'ui_avfx_presets_menu.jpg', alt: { ru: 'Меню пресетов AVFX', en: 'AVFX presets menu' },
                                   caption: { ru: 'Меню библиотеки AVFX-пресетов', en: 'AVFX preset library menu' } },
      { file: 'ui_global_prefs_menu.jpg', alt: { ru: 'Меню глобальных настроек', en: 'Global preferences menu' },
                                   caption: { ru: 'Глобальная миграция интерфейсных параметров и scope-метки', en: 'Global UI preference migration and scope badges' } },
      { file: 'ui_skin_override_menu.jpg', alt: { ru: 'Меню override скинов', en: 'Skin override menu' },
                                   caption: { ru: 'Mod-local выбор own/enemy model/skin', en: 'Mod-local own/enemy model/skin selection' } },
      { file: 'ui_visual_presets_menu.jpg', alt: { ru: 'Меню пресетов видимости', en: 'Visibility presets menu' },
                                   caption: { ru: 'Пресеты видимости, подсветок и визуального качества', en: 'Visibility, highlights, and visual-quality presets' } },
    ],
  },

  /* ───────────────────────── STARTUP INTRO ───────────────────────── */
  {
    id: 'startup-intro',
    slug: 'startup-intro',
    icon: '✦',
    title: {
      ru: 'Startup Intro',
      en: 'Startup Intro',
    },
    summary: {
      ru: 'Полноэкранное вступление с локализованной музыкой, живым логотипом, салютами и отдельным управлением составом.',
      en: 'A full-screen opening with localized music, a live logo, fireworks, and independent composition controls.',
    },
    body: {
      ru: [
        'На холодном запуске Q2PRO-X показывает вступление, не закрывая доступ к главному меню: логотип пульсирует и освещает сцену, электрические дуги, салюты, корабли, метеоры и кометы собираются одним композитором.',
        'Музыка выбирается из отдельных русских и английских пулов текущего языка. Можно включать и выключать состав Intro, управлять громкостью и оставить освещение или электричество логотипа после окончания вступления.',
        'Запуск сразу с `+map` или подключением к серверу пропускает Intro и не задерживает вход. При загрузке карты музыка плавно затухает, а кнопки возле логотипа позволяют заглушить или вернуть её без изменения сохранённых cvar громкости.',
      ],
      en: [
        'On a cold start, Q2PRO-X presents an opening sequence without blocking the main menu: the logo pulses and lights the scene while electricity, fireworks, ships, meteors, and comets are composed in one timeline.',
        'Music is selected from separate Russian and English pools for the active language. The Intro composition can be enabled per part, volume is configurable, and logo lighting or electricity may remain active after the sequence ends.',
        'Starting directly with `+map` or a server connection skips Intro and never delays entry. Music fades smoothly during map loading, while controls beside the logo mute or restore it without changing saved volume cvars.',
      ],
    },
    bullets: {
      ru: [
        'Отдельный выбор музыки, логотипа, молний, салютов и небесного трафика.',
        'RU/EN музыкальные пулы и корректное продолжение обычной музыки после Intro.',
        'Честный прогресс подготовки шейдеров в окнах WGL и EGL.',
      ],
      en: [
        'Independent music, logo, electricity, fireworks, and sky-traffic choices.',
        'RU/EN music pools and clean continuation into ordinary music after Intro.',
        'Honest shader-preparation progress in both WGL and EGL windows.',
      ],
    },
    callout: {
      ru: 'Для сайта нужен один общий кадр самого Intro; подробные страницы настроек остаются в документации.',
      en: 'The website needs one overall Intro frame; detailed settings pages belong in the documentation.',
    },
    images: [
      { file: '1-5-startup-intro.jpg', fit: 'cover', alt: { ru: 'Startup Intro Q2PRO-X 1.5', en: 'Q2PRO-X 1.5 Startup Intro' },
                                  caption: { ru: 'Главный кадр Startup Intro с логотипом и сценой', en: 'Startup Intro overview with the logo and live scene' } },
    ],
  },

  /* ───────────────────────── CVARS ───────────────────────── */
  {
    id: 'cvars',
    slug: 'cvars',
    icon: '#',
    title: {
      ru: 'CVAR Browser & Commands',
      en: 'CVAR Browser & Commands',
    },
    summary: {
      ru: 'Поиск, фильтры, RU/EN справка, command rows и безопасные reset-действия для Q2PRO-X настроек.',
      en: 'Search, filters, RU/EN help, command rows, and safe reset actions for Q2PRO-X settings.',
    },
    body: {
      ru: [
        'CVAR Browser показывает описание, scope, default, flags и связанные действия, поэтому настройками можно управлять из интерфейса, не запоминая тысячи имён.',
        'В 1.5 каталог больше не молча обрезается старым фиксированным лимитом: он масштабируется вместе с реестром переменных. Отдельная вкладка «Отладка» показывает session-only диагностики, которые не сохраняются в пользовательские конфиги.',
        'Браузер также включает публичные консольные команды, организованные по назначению, с синтаксисом, ключами, примером и предупреждением о последствиях. Выбор строки только подставляет команду в консоль и никогда не выполняет её автоматически.',
      ],
      en: [
        'The CVAR Browser shows descriptions, scope, defaults, flags, and related actions so players can work through the interface instead of memorizing thousands of names.',
        'In 1.5, the catalog no longer truncates silently at an old fixed capacity: it scales with the variable registry. A dedicated Debug tab exposes session-only diagnostics that are never written to user configs.',
        'The browser also includes public console commands organized by purpose, with syntax, options, examples, and consequence badges. Selecting a row only inserts the command into the console and never executes it automatically.',
      ],
    },
    bullets: {
      ru: [
        'Поиск по имени и описанию, RU/EN тексты, scope/default/flags и безопасные reset-действия.',
        'Публичные команды: синтаксис, ключи, пример и подстановка без автоматического запуска.',
        'Диагностические записи в Debug-вкладке остаются строго session-only.',
        '`cl_writeconfig_full` позволяет сделать обычный `writeconfig` эквивалентом полной записи `-b -m -a`.',
      ],
      en: [
        'Search by name and description, RU/EN text, scope/default/flags, and safe reset actions.',
        'Public commands with syntax, options, examples, and insertion without automatic execution.',
        'Diagnostic entries in the Debug tab remain strictly session-only.',
        '`cl_writeconfig_full` can make plain `writeconfig` equivalent to the complete `-b -m -a` write.',
      ],
    },
    callout: null,
    images: [
      { file: '1-5-cvar-browser.jpg', fit: 'cover', alt: { ru: 'Актуальный справочник переменных Q2PRO-X 1.5', en: 'Current Q2PRO-X 1.5 variable reference' },
                                    caption: { ru: 'Категории, поиск, текущее значение и подробное описание выбранной переменной', en: 'Categories, search, current value, and a detailed description of the selected variable' } },
      { file: '1-5-cvar-browser-commands.jpg', fit: 'cover', alt: { ru: 'Команды в CVAR Browser Q2PRO-X 1.5', en: 'Commands in the Q2PRO-X 1.5 CVAR Browser' },
                                    caption: { ru: 'Публичные команды, организованные по назначению, с алиасами и фактическим действием', en: 'Public commands organized by purpose, with aliases and the actual action' } },
    ],
  },

  /* ───────────────────────── NETWORK ───────────────────────── */
  {
    id: 'network-play',
    slug: 'network',
    icon: '↔',
    title: {
      ru: 'Бог сети & Network Play',
      en: 'Network God & Network Play',
    },
    summary: {
      ru: 'Диагностика соединения, адаптация пинга, Jitter/Guardian, Network Lab и полный прежний сетевой инструментарий.',
      en: 'Connection diagnostics, Ping Adaptation, Jitter/Guardian, Network Lab, and the complete established network toolkit.',
    },
    body: {
      ru: [
        '«Бог сети» объединяет LAGHAX, предикты, AutoPort и новые адаптацию пинга, Jitter/Guardian и Network Lab. Большой RU/EN-отчёт показывает факт, его смысл, рекомендуемое действие, ожидаемый результат, способ проверки и откат; отчёт можно прокручивать, копировать и сохранять.',
        'Ping Adaptation умеет держать заданный итоговый пинг вручную либо автоматически подстраиваться под соперника в дуэли и средний пинг активных соперников в TDM/FFA. Спектаторы исключаются; слой не создаёт потерь пакетов и совместим с LAGHAX, предиктами и fixedmove.',
        'Прежний сетевой набор сохранён: server browser, глобальные избранные, auto-port scan, OpenTDM HUD, Voice Chat, русский ввод, автотранслит и `/en` перевод. Мониторинг компьютера отдельно предупреждает о фоновой сетевой нагрузке.',
      ],
      en: [
        'Network God unifies LAGHAX, prediction, AutoPort, Ping Adaptation, Jitter/Guardian, and Network Lab. Its large RU/EN report explains the measured fact, what it means, the recommended action, expected result, verification, and rollback; the report can be scrolled, copied, and saved.',
        'Ping Adaptation can maintain a manually selected final ping or automatically match a duel opponent and the average active-opponent ping in TDM/FFA. Spectators are excluded; the layer creates no packet loss and remains compatible with LAGHAX, prediction, and fixedmove.',
        'The established network suite remains complete: server browser, global favourites, auto-port scanning, OpenTDM HUD, Voice Chat, Russian input, auto-translit, and `/en` translation. Optional computer monitoring warns about background network load.',
      ],
    },
    bullets: {
      ru: [
        'Бог сети: измерение, понятное объяснение, рекомендация, проверка и откат.',
        'Ping Adaptation: ручная цель или автоматическое выравнивание по соперникам.',
        'Jitter/Guardian, Network Lab и опциональный мониторинг фоновой нагрузки компьютера.',
        'Server browser, глобальные избранные, AutoPort, OpenTDM HUD, Voice Chat и перевод.',
      ],
      en: [
        'Network God: measurement, plain-language explanation, recommendation, verification, and rollback.',
        'Ping Adaptation with a manual target or automatic opponent-based equalization.',
        'Jitter/Guardian, Network Lab, and optional background-load monitoring.',
        'Server browser, global favourites, AutoPort, OpenTDM HUD, Voice Chat, and translation.',
      ],
    },
    callout: {
      ru: 'Все сетевые удобства остаются client-side: серверная авторитетность и правила матча не меняются.',
      en: 'All network conveniences stay client-side: server authority and match rules remain unchanged.',
    },
    images: [
      { file: '1-5-network-god-overlay-summary.jpg', fit: 'cover', alt: { ru: 'Бог сети — вкладка Сводка', en: 'Network God — Summary tab' },
                                        caption: { ru: 'Сводка: состояние соединения, реальный и итоговый пинг, текущая рекомендация и состав задержки', en: 'Summary: connection state, real and final ping, current recommendation, and latency breakdown' } },
      { file: '1-5-network-god-overlay-events.jpg', fit: 'cover', alt: { ru: 'Бог сети — вкладка События', en: 'Network God — Events tab' },
                                        caption: { ru: 'События и диагнозы: что именно обнаружено и насколько система уверена', en: 'Events and diagnoses: what was detected and how confident the system is' } },
      { file: '1-5-network-god-overlay-actions.jpg', fit: 'cover', alt: { ru: 'Бог сети — вкладка Что делать', en: 'Network God — What to do tab' },
                                        caption: { ru: 'Центр помощи: конкретные действия, ограничения клиентских настроек и безопасные следующие шаги', en: 'Action center: concrete steps, client-side limits, and safe next actions' } },
      { file: '1-5-network-god-overlay-help.jpg', fit: 'cover', alt: { ru: 'Бог сети — вкладка Справка', en: 'Network God — Help tab' },
                                        caption: { ru: 'Справка: режимы, этапы диагностики, цвета и значения показателей', en: 'Help: modes, diagnostic stages, colors, and indicator meanings' } },
    ],
  },

  /* ───────────────────────── VOICE ───────────────────────── */
  {
    id: 'voice',
    slug: 'voice',
    icon: '◉',
    title: {
      ru: 'Voice Chat',
      en: 'Voice Chat',
    },
    summary: {
      ru: 'Встроенный голосовой чат с hosted control server, autojoin, комнатами, VAD/PTT, устройством микрофона и громкостью приёма.',
      en: 'Built-in voice chat with hosted control server, autojoin, rooms, VAD/PTT, mic device, and receive volume.',
    },
    body: {
      ru: [
        'Voice Chat включается из меню и может автоматически подключаться к серверной комнате при заходе на игровой сервер. Игроку не нужно руками задавать room name, если включён autojoin/server room mode.',
        'Настройки микрофона, VAD threshold, PTT/toggle keys, mic gain, receive volume, deafen и control server сохраняются в обычном Q2PRO-X профиле.',
      ],
      en: [
        'Voice Chat is controlled from the menu and can automatically join the server room when you connect to a game server. The player does not need to type a room name manually when autojoin/server room mode is enabled.',
        'Mic device, VAD threshold, PTT/toggle keys, mic gain, receive volume, deafen, and control server are saved in the normal Q2PRO-X profile.',
      ],
    },
    bullets: {
      ru: [
        'autojoin on connect + server/manual room modes.',
        'VAD, push-to-talk и mic toggle сценарии.',
        'Подробное руководство Voice Chat входит в документацию 1.5.',
      ],
      en: [
        'autojoin on connect + server/manual room modes.',
        'VAD, push-to-talk, and mic toggle workflows.',
        'A detailed Voice Chat guide is included in the 1.5 documentation.',
      ],
    },
    callout: null,
    images: [
      { file: '01-voice-chat.jpg', alt: { ru: 'Voice Chat карта', en: 'Voice Chat map' },
                                  caption: { ru: 'Голосовые комнаты, VAD/PTT и control server', en: 'Voice rooms, VAD/PTT, and control server' } },
    ],
  },

  /* ───────────────────────── DEMOS ───────────────────────── */
  {
    id: 'demos',
    slug: 'demos',
    icon: '▶',
    title: {
      ru: 'Статистика игры, демки и аналитика',
      en: 'Game Statistics, Demos & Analytics',
    },
    summary: {
      ru: 'Личная статистика по локальной библиотеке демок, browser/player, director, trails, heatmap и отдельный demo visual profile.',
      en: 'Personal statistics from the local demo library, browser/player, director, trails, heatmap, and a separate demo visual profile.',
    },
    body: {
      ru: [
        'Раздел «Статистика игры» анализирует локальную библиотеку демок и строит личную историю матчей: сыгранные игры, фраги, результаты, соперники, карты и режимы. Основной источник — записанные клиентом `.dm2`; серверные `.mvd2` остаются дополнительным форматом.',
        'Спектаторские сеансы, разминка, тестовые и слишком короткие записи отделены от настоящих матчей. Результаты читаются адаптерами baseq2, OpenFFA и OpenTDM; если итог нельзя доказать, он честно остаётся неизвестным. Автообновление настраивается и по умолчанию выключено.',
        'Браузер и player сохраняют MVD/DM2 playback, next-frag/director, heatmap, movement trails, item timer overlays и отдельный demo visual profile. Личные факты для Startup Intro выбираются независимо.',
      ],
      en: [
        'Game Statistics analyzes the local demo library and builds a personal match history: played games, frags, results, opponents, maps, and modes. Client-recorded `.dm2` files are the primary source; server-side `.mvd2` remains a secondary format.',
        'Spectator sessions, warmup, test recordings, and files below the minimum size are kept separate from real matches. Results are parsed through baseq2, OpenFFA, and OpenTDM adapters; when an outcome cannot be proven, it remains honestly unknown. Automatic refresh is configurable and off by default.',
        'The browser and player retain MVD/DM2 playback, next-frag/director, heatmaps, movement trails, item-timer overlays, and a separate demo visual profile. Personal facts shown in Startup Intro are selected independently.',
      ],
    },
    bullets: {
      ru: [
        'Статистика: реальные матчи, фраги, результаты, соперники, карты, режимы, алиасы и сортировка.',
        'Ручное обновление, настраиваемая автоматика и минимальный размер демки; auto-refresh по умолчанию выключен.',
        'Demo visual profile, director, movement trails, heatmap и аналитические item timers сохранены.',
      ],
      en: [
        'Statistics for real matches, frags, results, opponents, maps, modes, aliases, and sorting.',
        'Manual refresh, configurable automation, and a minimum demo size; auto-refresh is off by default.',
        'Demo visual profile, director, movement trails, heatmap, and analytical item timers remain available.',
      ],
    },
    callout: {
      ru: 'Статистика строится только по локальным файлам и не выдаёт недоказанный результат за победу или поражение.',
      en: 'Statistics are built from local files only and never present an unproven result as a win or loss.',
    },
    images: [
      { file: '01-demo-player.jpg', alt: { ru: 'Demo Player карта', en: 'Demo Player map' },
                                    caption: { ru: 'Браузер, player popup, director и demo visual config', en: 'Browser, player popup, director, and demo visual config' } },
      { file: '02-demo-analytics.jpg', alt: { ru: 'Demo Analytics карта', en: 'Demo Analytics map' },
                                    caption: { ru: 'Trails, height mode, colors и item timers', en: 'Trails, height mode, colors, and item timers' } },
      { file: '1-5-game-statistics.jpg', fit: 'cover', alt: { ru: 'Статистика игры Q2PRO-X 1.5', en: 'Q2PRO-X 1.5 Game Statistics' },
                                    caption: { ru: 'Главный экран личной статистики по локальной библиотеке демок', en: 'Personal statistics overview for the local demo library' } },
    ],
  },

  /* ───────────────────────── SINGLE PLAYER / MODS ───────────────────────── */
  {
    id: 'single-player-mods',
    slug: 'single-player-mods',
    icon: '◆',
    title: {
      ru: 'Single Player Experiments & Mods',
      en: 'Single Player Experiments & Mods',
    },
    summary: {
      ru: 'Свободный мир, бои и реакции змей, SP-мутаторы, DM-тренировка, изменение геометрии, жидкости и 32-bit bridge.',
      en: 'Free World behavior, snake combat and reactions, SP mutators, DM training, geometry, liquids, and the 32-bit bridge.',
    },
    body: {
      ru: [
        'Q2PRO-X 1.5 поставляет собственный `baseq2/q2pro-x/gamex86_64.dll` и не перезаписывает stock `baseq2/gamex86_64.dll`. Single-player эксперименты остаются внутри Q2PRO-X-слоя и не ломают обычную установку Quake II.',
        'В Single Player Experiments можно перемешивать предметы и монстров, запускать DM-карты в одиночке с монстрами и счётом, визуально менять BSP-геометрию, делать варианты внешности монстров и менять воду/лаву с настоящими damage/physics/material/AVFX contacts.',
        'Режим «Свободный мир» снимает искусственные маршрутные ограничения с монстров: они реагируют на звуки, вступают в чужой бой, заранее видят препятствия и выбирают реально открытый путь. AVFX-змеи взаимодействуют с водой, травой, снегом и оружием и могут сражаться с игроком и монстрами.',
        'Mods menu сканирует локальные моды. Native 64-bit game DLL запускается напрямую, legacy 32-bit `gamex86.dll` работает через helper `baseq2/q2pro-x/utils/q2pro-x-bridge32.exe`.',
      ],
      en: [
        'Q2PRO-X 1.5 ships its own `baseq2/q2pro-x/gamex86_64.dll` and does not overwrite the stock `baseq2/gamex86_64.dll`. Single-player experiments remain inside the Q2PRO-X layer without breaking a normal Quake II install.',
        'Single Player Experiments can randomize items and monsters, run DM maps locally with monsters and scoring, visually deform BSP geometry, vary monster appearance, and swap water/lava with real damage/physics/material/AVFX contacts.',
        'Free World removes artificial route restrictions from monsters: they react to sound, join an ongoing fight, anticipate obstacles, and choose paths that are actually open. AVFX snakes interact with water, grass, snow, and weapons and can fight the player and monsters.',
        'The Mods menu scans local mods. Native 64-bit game DLLs run directly, while legacy 32-bit `gamex86.dll` mods use `baseq2/q2pro-x/utils/q2pro-x-bridge32.exe`.',
      ],
    },
    bullets: {
      ru: [
        '`cl_sp_mutate_dm_training 1; map q2dm1` запускает локальную DM-тренировку.',
        '`sv sp_mutator_info` печатает статус, счётчики и диагностику SP-мутатора.',
        '`cl_sp_geo_deform` меняет только видимую геометрию; collision и gameplay остаются оригинальными.',
        '`cl_sp_mutate_water_to_lava` / `cl_sp_mutate_lava_to_water` делают настоящую подмену жидкости.',
        'Свободный мир и бои змей остаются локальными SP-возможностями и не меняют правила удалённого сервера.',
      ],
      en: [
        '`cl_sp_mutate_dm_training 1; map q2dm1` starts local DM training.',
        '`sv sp_mutator_info` prints SP-mutator status, counters, and diagnostics.',
        '`cl_sp_geo_deform` changes visible geometry only; collision and gameplay stay original.',
        '`cl_sp_mutate_water_to_lava` / `cl_sp_mutate_lava_to_water` perform a real liquid swap.',
        'Free World and snake combat remain local Single Player features and never change remote-server rules.',
      ],
    },
    callout: {
      ru: 'Подробная настройка одиночных экспериментов, Свободного мира и 32-bit bridge описана в руководстве Single Player & Mods 1.5.',
      en: 'The 1.5 Single Player & Mods guide covers experiments, Free World behavior, and 32-bit bridge setup.',
    },
    images: [
      { file: 'single_player_dm_training.jpg', alt: { ru: 'DM-тренировка с монстрами', en: 'DM training with monsters' },
                                    caption: { ru: 'DM-карта в локальном SP с монстрами, респауном и счётом', en: 'A DM map in local SP with monsters, respawn, and scoring' } },
      { file: 'mods_bridge_menu.jpg', alt: { ru: 'Меню модов и bridge', en: 'Mods menu and bridge' },
                                    caption: { ru: 'Native 64-bit моды и legacy 32-bit gamex86.dll через bridge', en: 'Native 64-bit mods and legacy 32-bit gamex86.dll through the bridge' } },
    ],
  },
];

/* ───────────────────────── DOCUMENTATION ───────────────────────── */

// Real per-language release documentation files. Served from
// `O:\Claude\public\q2pro-x\docs\{ru,en}\` via the static mount
// registered in `routes.js`. Browsers open `.docx` as a download or
// delegate to the OS office handler — both UX paths are acceptable.
//
// To change a filename or swap to a different format (html, pdf, md):
// just edit the URL here; render/route layers don't hardcode names.
function publicAssetVersion(urlPath) {
  try {
    const prefix = '/q2pro-x/';
    if (!urlPath.startsWith(prefix)) {
      return '1.5';
    }
    const publicRoot = path.resolve(__dirname, '..', 'public', 'q2pro-x');
    const assetFile = path.resolve(publicRoot, urlPath.slice(prefix.length));
    const rel = path.relative(publicRoot, assetFile);
    if (rel.startsWith('..') || path.isAbsolute(rel)) {
      return version;
    }
    const stat = fs.statSync(assetFile);
    return Math.trunc(stat.mtimeMs).toString(36) + '-' + stat.size.toString(36);
  } catch (err) {
    return '1.5';
  }
}

function publicAssetExists(urlPath) {
  try {
    const prefix = '/q2pro-x/';
    const cleanPath = String(urlPath || '').split('?')[0];
    if (!cleanPath.startsWith(prefix)) return false;
    const publicRoot = path.resolve(__dirname, '..', 'public', 'q2pro-x');
    const assetFile = path.resolve(publicRoot, cleanPath.slice(prefix.length));
    const rel = path.relative(publicRoot, assetFile);
    return !rel.startsWith('..') && !path.isAbsolute(rel) && fs.statSync(assetFile).isFile();
  } catch (err) {
    return false;
  }
}

function versionedPublicUrl(urlPath) {
  return urlPath + '?v=' + encodeURIComponent(publicAssetVersion(urlPath));
}

function docLinks(version, base) {
  const ruPreview = '/q2pro-x/docs-preview/ru/Q2PRO-X_' + version + '_' + base + '_RU.html';
  const ruDownload = '/q2pro-x/docs/ru/Q2PRO-X_' + version + '_' + base + '_RU.docx';
  const enPreview = '/q2pro-x/docs-preview/en/Q2PRO-X_' + version + '_' + base + '_EN.html';
  const enDownload = '/q2pro-x/docs/en/Q2PRO-X_' + version + '_' + base + '_EN.docx';
  return {
    ru: {
      preview: versionedPublicUrl(ruPreview),
      download: versionedPublicUrl(ruDownload),
    },
    en: {
      preview: versionedPublicUrl(enPreview),
      download: versionedPublicUrl(enDownload),
    },
  };
}

function localizedDocLinks(version, ruBase, enBase) {
  const ruPreview = '/q2pro-x/docs-preview/ru/Q2PRO-X_' + version + '_' + ruBase + '_RU.html';
  const ruDownload = '/q2pro-x/docs/ru/Q2PRO-X_' + version + '_' + ruBase + '_RU.docx';
  const enPreview = '/q2pro-x/docs-preview/en/Q2PRO-X_' + version + '_' + enBase + '_EN.html';
  const enDownload = '/q2pro-x/docs/en/Q2PRO-X_' + version + '_' + enBase + '_EN.docx';
  return {
    ru: { preview: versionedPublicUrl(ruPreview), download: versionedPublicUrl(ruDownload) },
    en: { preview: versionedPublicUrl(enPreview), download: versionedPublicUrl(enDownload) },
  };
}

const docs = {
  index: docLinks('1.5', 'Documentation_Index'),
  release: docLinks('1.5', 'Release_Notes'),
  networkGod: localizedDocLinks('1.5', 'Network_God_Руководство', 'Network_God_Guide'),
  network: docLinks('1.5', 'Network_Play_Guide'),
  avfxAdvanced: docLinks('1.5', 'AVFX_Advanced_Features_Guide'),
  avfxGenerator: docLinks('1.5', 'AVFX_Preset_Generator_Guide'),
  avfxPresets: docLinks('1.5', 'AVFX_Presets_Guide'),
  avfx: docLinks('1.5', 'AVFX_Settings_Guide'),
  gameStatistics: docLinks('1.5', 'Game_Statistics_Demo_Auto_Statistics_Guide'),
  startupIntro: docLinks('1.5', 'Startup_Intro_Settings_Guide'),
  liveTimers: docLinks('1.5', 'Live_Item_Timers_Guide'),
  hdr: docLinks('1.5', 'HDR_Guide'),
  highlights: docLinks('1.5', 'Visual_Highlights_Guide'),
  singleplayerMods: docLinks('1.5', 'Single_Player_Mods_Guide'),
  interface: docLinks('1.5', 'Interface_Menu_Console_Guide'),
  cvarReference: docLinks('1.5', 'Cvar_Command_Reference'),
  video: docLinks('1.5', 'Video_Visuals_Guide'),
  cvars: docLinks('1.5', 'Cvars_Cvar_Browser_Guide'),
  voice: docLinks('1.5', 'Voice_Chat_Guide'),
  controls: docLinks('1.5', 'Controls_Mouse_Zoom_Guide'),
  demos: docLinks('1.5', 'Demo_Browser_Player_Guide'),
  sound: docLinks('1.5', 'Sound_Guide'),
};

/* ───────────────────────── RELEASE / DOWNLOADS ───────────────────────── */

const release = {
  version: '1.5',
  available: true,
  downloadHref: versionedPublicUrl('/q2pro-x/downloads/q2pro-x-1.5.rar'),
  popupTitle: {
    ru: 'Скачать Q2PRO-X',
    en: 'Download Q2PRO-X',
  },
  popupMessage: {
    ru: 'Финальный архив Q2PRO-X 1.5 принят, проверен и доступен для скачивания.',
    en: 'The final Q2PRO-X 1.5 archive has been accepted, verified, and is available for download.',
  },
  sectionTitle: {
    ru: 'Q2PRO-X 1.5 — релизный пакет',
    en: 'Q2PRO-X 1.5 — release package',
  },
  sectionSummary: {
    ru: 'Финальный архив, полный комплект документации 1.5 и безопасный путь первого запуска.',
    en: 'The final archive, complete 1.5 documentation set, and the safe first-launch path.',
  },
  sectionBody: {
    ru: [
      'Финальный архив Q2PRO-X 1.5 опубликован после документационной приёмки, проверки состава и запуска на чистой установке.',
      'После загрузки пакет распаковывается в отдельную рабочую папку Quake II / Q2PRO-X и запускается через `Q2PRO-X.exe`. Новая установка не должна перезаписывать пользовательские cfg, PAK или stock game DLL.',
      'Релиз 1.5 включает полный прежний клиентский слой и новые AVFX-миры/генератор, Бога сети, адаптацию пинга, статистику игры, Startup Intro, живые таймеры, Performance HUD, Свободный мир и полный комплект RU/EN-документации.',
    ],
    en: [
      'The final Q2PRO-X 1.5 archive is published after documentation acceptance, package-composition verification, and a clean-install launch.',
      'After download, unpack the package into a dedicated Quake II / Q2PRO-X working folder and run `Q2PRO-X.exe`. A clean install must not overwrite user cfg files, PAKs, or the stock game DLL.',
      'Version 1.5 retains the complete established client layer and adds AVFX worlds and generation, Network God, Ping Adaptation, Game Statistics, Startup Intro, live item timers, Performance HUD, Free World, and the complete RU/EN documentation set.',
    ],
  },
  quickStartTitle: {
    ru: 'Быстрый старт',
    en: 'Quick start',
  },
  quickStartSummary: {
    ru: 'Минимальные шаги, чтобы скачать, распаковать и сразу запустить Q2PRO-X.',
    en: 'The minimum steps to download, unpack, and start Q2PRO-X.',
  },
  quickStartSteps: {
    ru: [
      {
        title: '1. Скачайте и распакуйте архив',
        text: 'Скачайте `q2pro-x-1.5.rar` и распакуйте его в отдельную папку Quake II / Q2PRO-X. Не смешивайте релизный архив с пользовательскими cfg и старыми тестовыми деревьями.',
      },
      {
        title: '2. Первый запуск = безопасные defaults 1.5',
        text: 'Тяжёлые AVFX-материалы, Чёрная дыра, тесселяция, зеркала, Performance HUD и SP-эксперименты не включаются сами. Все возможности доступны через меню и пресеты.',
      },
      {
        title: '3. Выберите пресет или генератор',
        text: 'Откройте AVFX Presets для выбора готового профиля либо AVFX Preset Generator для собственной совместимой сцены. HDR, visibility и network-профили настраиваются отдельно.',
      },
      {
        title: '4. Для деталей откройте docs 1.5',
        text: 'Полный комплект русской и английской документации охватывает все основные системы, включая отдельные руководства по AVFX, генератору, Богу сети, Intro, статистике и живым таймерам.',
      },
    ],
    en: [
      {
        title: '1. Download and unpack the archive',
        text: 'Download `q2pro-x-1.5.rar` and unpack it into a dedicated Quake II / Q2PRO-X folder. Do not merge the release archive with user cfg files or old test trees.',
      },
      {
        title: '2. First launch = safe 1.5 defaults',
        text: 'Heavy AVFX materials, the Black Hole, tessellation, mirrors, Performance HUD, and SP experiments do not enable themselves. Every feature remains available through menus and presets.',
      },
      {
        title: '3. Pick a preset or use the generator',
        text: 'Open AVFX Presets to choose a built-in profile or use AVFX Preset Generator for a custom compatible scene. HDR, visibility, and network profiles remain independent.',
      },
      {
        title: '4. Open the 1.5 docs for details',
        text: 'The complete Russian and English documentation set covers every major system, including dedicated guides for advanced AVFX, the generator, Network God, Intro, statistics, and live item timers.',
      },
    ],
  },
  sourcesPopupTitle: {
    ru: 'Исходники Q2PRO-X',
    en: 'Q2PRO-X Sources',
  },
  sourcesPopupMessage: {
    ru: 'Исходники Q2PRO-X на данный момент ещё не опубликованы, но такая возможность вскоре появится. Проверьте страницу позже.',
    en: 'Q2PRO-X sources are not published yet, but this option will appear soon. Please check the page again later.',
  },
};

const beta = {
  visible: false,
  version: '',
  available: false,
  downloadHref: '',
  slug: 'beta',
  popupTitle: {
    ru: 'Бета-версии Q2PRO-X',
    en: 'Q2PRO-X Beta Builds',
  },
  popupMessage: {
    ru: 'Публичные beta-сборки Q2PRO-X пока не опубликованы. Когда откроется следующая тестовая линия, актуальная сборка появится в этом разделе вместе с кратким описанием изменений.',
    en: 'Public Q2PRO-X beta builds are not published yet. When the next test line opens, the current build will appear in this section together with a short change summary.',
  },
  sectionTitle: {
    ru: 'Beta-версии',
    en: 'Beta Builds',
  },
  sectionSummary: {
    ru: 'Отдельный канал для предрелизных сборок. Release-пакет выше остаётся стабильной линией, а beta появляется здесь, когда мы открываем новый тестовый цикл.',
    en: 'A separate channel for pre-release builds. The release package above stays the stable line, and beta builds appear here when a new public test cycle opens.',
  },
  sectionBody: {
    ru: [
      'Здесь будут публиковаться будущие публичные beta-сборки Q2PRO-X, когда откроется следующий тестовый цикл.',
      'Beta-линия не заменяет стабильный релиз. Для обычной игры используйте актуальный public release из раздела скачивания выше.',
    ],
    en: [
      'This section will host future public Q2PRO-X beta builds when the next test cycle opens.',
      'The beta line does not replace the stable release. For normal play, use the current public release from the download section above.',
    ],
  },
  currentTitle: {
    ru: 'Текущая beta-сборка',
    en: 'Current beta build',
  },
  currentStatusLabel: {
    ru: 'Статус',
    en: 'Status',
  },
  currentStatusValue: {
    ru: 'Нет открытой публичной beta-линии',
    en: 'No public beta line is open',
  },
  currentVersionLabel: {
    ru: 'Сборка',
    en: 'Build',
  },
  currentVersionValue: {
    ru: 'Будет объявлена отдельно',
    en: 'To be announced separately',
  },
  currentDescriptionTitle: {
    ru: 'Описание текущей beta',
    en: 'Current beta description',
  },
  currentDescriptionBody: {
    ru: [
      'Стабильный релиз 1.5 опубликован. Когда появится новая beta-линия, этот блок будет открыт с отдельным описанием, архивом и точками внимания для тестировщиков.',
    ],
    en: [
      'Stable release 1.5 is published. When a new beta line appears, this block will open with its own description, archive, and tester focus areas.',
    ],
  },
  notesTitle: {
    ru: 'Что будет в beta-блоке',
    en: 'What the beta block will contain',
  },
  notes: {
    ru: [
      'номер beta-сборки и дата публикации',
      'краткое отличие от стабильного релиза',
      'отдельный архив для тестирования',
      'точки внимания для обратной связи',
    ],
    en: [
      'beta build number and publication date',
      'short difference from the stable release',
      'separate archive for testing',
      'focus areas for feedback',
    ],
  },
  images: [
    {
      file: '01-opentdm-team-hud.jpg',
      alt: {
        ru: 'Passive OpenTDM Team HUD',
        en: 'Passive OpenTDM Team HUD',
      },
      caption: {
        ru: 'OpenTDM Team HUD в реальном матче',
        en: 'OpenTDM Team HUD in a real match',
      },
    },
    {
      file: '02-predict-adaptive-hud.jpg',
      alt: {
        ru: 'LAGHAX с adaptive predict gate',
        en: 'LAGHAX with adaptive predict gate',
      },
      caption: {
        ru: 'Динамика включения / выключения predicts по ping',
        en: 'Predict on/off dynamics driven by ping',
      },
    },
    {
      file: '03-mouse-behavior-menu.jpg',
      alt: {
        ru: 'Меню mouse behavior',
        en: 'Mouse behavior menu',
      },
      caption: {
        ru: 'q2pro / r1q2 compatibility mode',
        en: 'q2pro / r1q2 compatibility mode',
      },
    },
    {
      file: '04-legacy-rail-compat.jpg',
      alt: {
        ru: 'Legacy rail compatibility',
        en: 'Legacy rail compatibility',
      },
      caption: {
        ru: 'Прозрачная миграция старого rail-конфига',
        en: 'Transparent migration for old rail configs',
      },
    },
    {
      file: '05-beta-overview.jpg',
      alt: {
        ru: 'Q2PRO-X beta overview',
        en: 'Q2PRO-X beta overview',
      },
      caption: {
        ru: 'Общий in-game вид beta-линии',
        en: 'Overall in-game look of the beta line',
      },
    },
    {
      file: '06-beta-feature.jpg',
      alt: {
        ru: 'Q2PRO-X beta feature highlight',
        en: 'Q2PRO-X beta feature highlight',
      },
      caption: {
        ru: 'Дополнительный кадр из beta-линии',
        en: 'Additional frame from the beta line',
      },
    },
  ],
};

const links = {
  telegram: 'https://t.me/Q2RTX',
  quakeJourney: 'https://t.me/QuakeJourney',
};

const brandingVersion = '2026-08-25-release-1-5';

const branding = {
  logoMain: '/q2pro-x/branding/Q2PRO-X_1_0_wordmark_dark.png?v=' + brandingVersion,
  logoLight: '/q2pro-x/branding/Q2PRO-X_1_0_wordmark_light.png?v=' + brandingVersion,
  favicon: '/q2pro-x/branding/Q2PRO-X_logo_telegram_square.png?v=' + brandingVersion,
  logoAlt: {
    ru: 'Логотип Q2PRO-X',
    en: 'Q2PRO-X logo',
  },
};

const docsSection = {
  id:    'docs',
  title: { ru: 'Документация',         en: 'Documentation' },
  summary: {
    ru: 'Полный комплект русской и английской документации Q2PRO-X 1.5 по всем основным системам клиента.',
    en: 'The complete Russian and English Q2PRO-X 1.5 documentation set covering every major client system.',
  },
  pending: {
    ru: 'Документация временно недоступна. Карточки появятся автоматически после восстановления полного комплекта DOCX и HTML-версий.',
    en: 'Documentation is temporarily unavailable. Cards will reappear automatically after the complete DOCX and HTML set is restored.',
  },
  items: [
    {
      key:   'index',
      title: { ru: 'Индекс документации 1.5',      en: 'Documentation Index 1.5' },
      desc:  { ru: 'Карта полного комплекта 1.5, назначение каждого руководства и порядок чтения.',
               en: 'Map of the complete 1.5 set, the purpose of every guide, and suggested reading order.' },
    },
    {
      key:   'release',
      title: { ru: 'Release Notes 1.5',            en: 'Release Notes 1.5' },
      desc:  { ru: 'Главные пользовательские возможности и изменения финальной версии 1.5.',
               en: 'The main user-facing features and changes in the final 1.5 release.' },
    },
    {
      key:   'networkGod',
      title: { ru: 'Бог сети 1.5',                 en: 'Network God Guide 1.5' },
      desc:  { ru: 'Полное руководство по диагностике, Ping Adaptation, Jitter/Guardian, Network Lab, отчётам и безопасному откату.',
               en: 'Complete guide to diagnostics, Ping Adaptation, Jitter/Guardian, Network Lab, reports, and safe rollback.' },
    },
    {
      key:   'avfxAdvanced',
      title: { ru: 'Расширенные возможности AVFX 1.5', en: 'AVFX Advanced Features 1.5' },
      desc:  { ru: 'Поверхности, объекты, трава, рисунки, объёмный пар/дым, космос, Чёрная дыра, зеркала и тесселяция.',
               en: 'Surfaces, objects, grass, drawings, volumetric steam/smoke, space, the Black Hole, mirrors, and tessellation.' },
    },
    {
      key:   'avfxGenerator',
      title: { ru: 'Генератор AVFX-пресетов 1.5', en: 'AVFX Preset Generator 1.5' },
      desc:  { ru: 'Seed, качество, хаос, покрытие, независимые селекторы, сводка и сохранение результата.',
               en: 'Seed, quality, chaos, coverage, independent selectors, summary, and saving the result.' },
    },
    {
      key:   'gameStatistics',
      title: { ru: 'Статистика игр и демок 1.5',   en: 'Game & Demo Statistics 1.5' },
      desc:  { ru: 'Локальная библиотека, реальные матчи, результаты, карты, соперники, auto-refresh и факты для Intro.',
               en: 'Local library, real matches, results, maps, opponents, auto-refresh, and Intro facts.' },
    },
    {
      key:   'startupIntro',
      title: { ru: 'Настройки Startup Intro 1.5',  en: 'Startup Intro Settings 1.5' },
      desc:  { ru: 'Музыка, логотип, молнии, салюты, небесный трафик, пропуск прямого входа и плавное затухание.',
               en: 'Music, logo, electricity, fireworks, sky traffic, direct-entry skipping, and smooth fade-out.' },
    },
    {
      key:   'liveTimers',
      title: { ru: 'Живые таймеры предметов 1.5',  en: 'Live Item Timers 1.5' },
      desc:  { ru: 'Ручные и безопасно авторизованные автоматические таймеры, HUD, звуки, бинды и правила наблюдения.',
               en: 'Manual and safely authorized automatic timers, HUD, sounds, binds, and observation rules.' },
    },
    {
      key:   'hdr',
      title: { ru: 'HDR Guide 1.5',                en: 'HDR Guide 1.5' },
      desc:  { ru: 'HDR-рендер, true HDR output, WGL/EGL, peak nits, paper white, JXR и типовые настройки.',
               en: 'HDR rendering, true HDR output, WGL/EGL, peak nits, paper white, JXR, and practical tuning.' },
    },
    {
      key:   'avfxPresets',
      title: { ru: 'AVFX Presets Guide 1.5',       en: 'AVFX Presets Guide 1.5' },
      desc:  { ru: 'Библиотека готовых профилей, пользовательские слоты и правила загрузки, сохранения и переименования.',
               en: 'A ready-made profile library, personal slots, and the load, save, and rename workflow.' },
    },
    {
      key:   'highlights',
      title: { ru: 'Visual Highlights Guide 1.5',  en: 'Visual Highlights Guide 1.5' },
      desc:  { ru: 'Предметы, оружие, игроки, team/duel logic и раскраска частей MD2/MD5 моделей.',
               en: 'Items, weapons, players, team/duel logic, and MD2/MD5 model-part tinting.' },
    },
    {
      key:   'singleplayerMods',
      title: { ru: 'Single Player & Mods Guide 1.5', en: 'Single Player & Mods Guide 1.5' },
      desc:  { ru: 'SP-мутаторы, DM-тренировка, изменение геометрии, water/lava swap и 32-bit bridge.',
               en: 'SP mutators, DM training, geometry deformation, water/lava swap, and the 32-bit bridge.' },
    },
    {
      key:   'interface',
      title: { ru: 'Полная карта меню 1.5',        en: 'Complete Menu Guide 1.5' },
      desc:  { ru: 'Все основные разделы Q2PRO-X, дочерние меню, хинты, scope-метки, быстрые команды и console UI.',
               en: 'All main Q2PRO-X sections, child pages, hints, scope labels, quick commands, and console UI.' },
    },
    {
      key:   'cvarReference',
      title: { ru: 'CVAR и команды 1.5',           en: 'Cvars & Commands 1.5' },
      desc:  { ru: 'Группы переменных и быстрые команды: renderer/HDR, AVFX, подсветки, SP, mods, voice, demos и сеть.',
               en: 'Variable groups and quick commands for renderer/HDR, AVFX, highlights, SP, mods, voice, demos, and network.' },
    },
    {
      key:   'avfx',
      title: { ru: 'AVFX Settings Guide 1.5',      en: 'AVFX Settings Guide 1.5' },
      desc:  { ru: 'Подробный гайд по базовой AVFX-погоде: дождь, снег, лужи, LOD и настройки.',
               en: 'Detailed guide to baseline AVFX weather: rain, snow, puddles, LOD, and settings.' },
    },
    {
      key:   'video',
      title: { ru: 'Видео и подсветки 1.5',        en: 'Video & Visuals 1.5' },
      desc:  { ru: 'Видео-настройки, visibility highlights, model color overrides и demo visual profile.',
               en: 'Video settings, visibility highlights, model color overrides, and the demo visual profile.' },
    },
    {
      key:   'cvars',
      title: { ru: 'CVAR Browser 1.5',             en: 'CVAR Browser 1.5' },
      desc:  { ru: 'Поиск переменных, command rows, flags, scope и reset-команды.',
               en: 'Variable search, command rows, flags, scope, and reset commands.' },
    },
    {
      key:   'voice',
      title: { ru: 'Voice Chat 1.5',               en: 'Voice Chat 1.5' },
      desc:  { ru: 'Комнаты, autojoin, VAD/PTT, микрофон, gain, receive volume и hosted control server.',
               en: 'Rooms, autojoin, VAD/PTT, microphone, gain, receive volume, and hosted control server.' },
    },
    {
      key:   'network',
      title: { ru: 'Сетевая игра 1.5',             en: 'Network Play 1.5' },
      desc:  { ru: 'Ping Adaptation, server browser, AutoPort, LAGHAX/predict, OpenTDM HUD, RU input и `/en`.',
               en: 'Ping Adaptation, server browser, AutoPort, LAGHAX/predict, OpenTDM HUD, RU input, and `/en`.' },
    },
    {
      key:   'controls',
      title: { ru: 'Управление, мышь и zoom 1.5',  en: 'Controls, Mouse & Zoom 1.5' },
      desc:  { ru: 'R1Q2-style mouse behavior, sensitivity, zoom FOV, zoom sens и key binds.',
               en: 'R1Q2-style mouse behavior, sensitivity, zoom FOV, zoom sens, and key binds.' },
    },
    {
      key:   'demos',
      title: { ru: 'Демки и demo player 1.5',      en: 'Demos & Demo Player 1.5' },
      desc:  { ru: 'Demo browser, MVD/DM2 playback, visual profile, director, analytics и timers.',
               en: 'Demo browser, MVD/DM2 playback, visual profile, director, analytics, and timers.' },
    },
    {
      key:   'sound',
      title: { ru: 'Руководство по звуку 1.5',    en: 'Sound Guide 1.5' },
      desc:  { ru: 'OpenAL, HRTF, бинауральный звук, реверберация и практическая настройка.',
               en: 'OpenAL, HRTF, binaural sound, reverb, and practical setup.' },
    },
  ],
};

/* ───────────────────────── META / SEO ───────────────────────── */

const meta = {
  title: {
    ru: 'Q2PRO-X 1.5 — умный клиент Quake II',
    en: 'Q2PRO-X 1.5 — a smart Quake II client',
  },
  description: {
    ru: 'Q2PRO-X 1.5 — современный клиент Quake II на базе Q2PRO: Бог сети, адаптация пинга, AVFX-миры и генератор, статистика, Startup Intro, демки, Single Player, моды и документация RU/EN.',
    en: 'Q2PRO-X 1.5 is a modern Q2PRO-based Quake II client with Network God, Ping Adaptation, AVFX worlds and generation, statistics, Startup Intro, demos, Single Player, mods, and RU/EN documentation.',
  },
  // Social preview uses the release-specific 1.5 cover. The adaptive
  // production wordmarks above stay untouched and continue to serve the page.
  ogImage: '/q2pro-x/branding/Q2PRO-X_1_5_release_cover.png?v=' + brandingVersion,
};

const supportedLangs = ['ru', 'en'];
const defaultLang = 'ru';
const supportedThemes = ['auto', 'dark', 'light'];
const defaultTheme = 'auto';

function pick(bundle, lang) {
  if (!bundle) return '';
  if (typeof bundle === 'string') return bundle;
  return bundle[lang] || bundle[defaultLang] || '';
}

function mediaVersion(section, file) {
  try {
    const mediaRoot = path.resolve(__dirname, 'media');
    const mediaFile = path.resolve(mediaRoot, section.slug, file);
    const rel = path.relative(mediaRoot, mediaFile);
    if (rel.startsWith('..') || path.isAbsolute(rel)) {
      return '1.5';
    }
    const stat = fs.statSync(mediaFile);
    return Math.trunc(stat.mtimeMs).toString(36) + '-' + stat.size.toString(36);
  } catch (err) {
    return '1.5';
  }
}

function mediaPath(section, file) {
  const version = encodeURIComponent(mediaVersion(section, file));
  return '/q2pro-x/media/' + section.slug + '/' + file + '?v=' + version;
}

function mediaExists(section, file) {
  try {
    const mediaRoot = path.resolve(__dirname, 'media');
    const mediaFile = path.resolve(mediaRoot, section.slug, file);
    const rel = path.relative(mediaRoot, mediaFile);
    return !rel.startsWith('..') && !path.isAbsolute(rel) && fs.statSync(mediaFile).isFile();
  } catch (err) {
    return false;
  }
}

function resolveLang(raw) {
  const v = String(raw || '').toLowerCase();
  return supportedLangs.includes(v) ? v : defaultLang;
}

function resolveTheme(raw) {
  const v = String(raw || '').toLowerCase();
  return supportedThemes.includes(v) ? v : defaultTheme;
}

require('./release-1.6')({ui,hero,navItems,sections,docs,docsSection,release,beta,meta},docLinks,localizedDocLinks,versionedPublicUrl);

require('./release-1.6')({ui,hero,navItems,sections,docs,docsSection,release,beta,meta},docLinks,localizedDocLinks,versionedPublicUrl);

module.exports = {
  ui, navItems, hero, sections, docs, docsSection, release, beta, links, branding, meta,
  supportedLangs, defaultLang, supportedThemes, defaultTheme,
  pick, mediaPath, mediaExists, publicAssetExists, resolveLang, resolveTheme,
};
