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
  releaseCta:  { ru: 'Скачать 1.2',             en: 'Download 1.2' },
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

const hero = {
  eyebrow: {
    ru: 'SMART QUAKE II CLIENT · RELEASE 1.2',
    en: 'SMART QUAKE II CLIENT · RELEASE 1.2',
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
    ru: 'LAGHAX сглаживает сетевые коррекции, Weapon Predict делает локальные эффекты оружия отзывчивее, Voice Chat собирает команду в матче, браузеры помогают с поиском, настройками и демками, а demo player с аналитикой превращает просмотр записей в рабочий инструмент. Это мощный клиент для тех, кто хочет тонко собрать игру под себя; версия 1.2 уже доступна как стабильный публичный релиз.',
    en: 'LAGHAX smooths network corrections, Weapon Predict makes local weapon effects feel more responsive, Voice Chat keeps the team together in a match, browsers help with discovery, settings, and demos, and the demo player with analytics turns recordings into a real review workspace. It is a powerful client for players who want to tune the game deeply; version 1.2 is available as the stable public release.',
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
      { title: 'Пинг без резких рывков', desc: 'LAGHAX сглаживает коррекции, когда сеть начинает ломать ощущение движения.' },
      { title: 'Оружие отзывчивее', desc: 'Weapon Predict показывает локальные эффекты выстрела сразу и убирает ощущение визуальной задержки.' },
      { title: 'Общение в матче', desc: 'Voice Chat, русский ввод, автотранслит и `/en` перевод встроены в клиент.' },
      { title: 'Настройки под контролем', desc: 'CVAR browser даёт поиск, описания, scope и быстрые команды для большого набора параметров.' },
      { title: 'Поиск без лишних окон', desc: 'Server browser и избранное помогают быстрее вернуться к игре.' },
      { title: 'Демки как рабочий режим', desc: 'Demo player, visual profile, director, trails, heatmap и item timers помогают разбирать игру.' },
    ],
    en: [
      { title: 'Ping without hard snaps', desc: 'LAGHAX smooths corrections when the network starts breaking movement feel.' },
      { title: 'More responsive weapons', desc: 'Weapon Predict shows local fire effects immediately and reduces visual delay.' },
      { title: 'Match communication', desc: 'Voice Chat, Russian input, auto-translit, and `/en` translation are built in.' },
      { title: 'Settings under control', desc: 'The CVAR browser gives search, descriptions, scope, and quick commands for a large settings surface.' },
      { title: 'Search without extra windows', desc: 'The server browser and favourites help you get back into a game faster.' },
      { title: 'Demos as a workspace', desc: 'Demo player, visual profile, director, trails, heatmap, and item timers help review play.' },
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
      ru: 'Умный клиент Quake II, построенный поверх Q2PRO, со встроенными инструментами для современного онлайн-гейминга.',
      en: 'A smart Quake II client built on top of Q2PRO with built-in tooling for modern online play.',
    },
    body: {
      ru: [
        'Q2PRO-X — это модифицированный клиент Q2PRO, в который интегрированы функции, обычно требующие сторонних утилит, скриптов или правок `autoexec.cfg`. Версия 1.2 превращает этот набор в цельный продукт: guided/classic интерфейс, браузеры, Voice Chat, демо-плейер, новые visual profiles и автоматическая миграция рекомендуемых настроек.',
        'Если коротко: это всё тот же Quake II и всё те же серверы, но с более дружелюбным первым запуском, встроенными инструментами для игры и демо, понятными меню и подробной документацией. Технические возможности 1.2 — LAGHAX, weapon predict, cvar browser, Voice Chat, автотранслит, перевод и demo analytics — разобраны ниже по разделам.',
        'Проект полностью совместим с существующими Q2-серверами и протоколом — ничего не меняется на стороне сервера. Все улучшения живут в клиенте и могут быть включены/выключены через меню, cvar browser или консоль.',
      ],
      en: [
        'Q2PRO-X is a modified Q2PRO client that bundles in-tree the features that normally require third-party utilities, config scripting, or `autoexec.cfg` surgery. Version 1.2 turns that toolbox into a product: guided/classic interface, browsers, Voice Chat, demo player, new visual profiles, and one-time migration of recommended settings.',
        'In plain terms: it is still Quake II and still connects to normal servers, but the first launch is friendlier, game and demo tools are built in, menus are easier to navigate, and the documentation is complete. The 1.2 technical features — LAGHAX, weapon predict, cvar browser, Voice Chat, auto-translit, translation, and demo analytics — are broken down below.',
        'Fully compatible with existing Q2 servers and the vanilla protocol. No server-side changes required. Every improvement lives in the client and can be toggled via menu, cvar browser, or console.',
      ],
    },
    bullets: {
      ru: [
        'Полная совместимость с любыми Q2-серверами (vanilla protocol 34, R1Q2 35, Q2PRO 36).',
        'Настройки разделены на global, mod-related и demo visual scope, чтобы демки и моды не ломали друг другу внешний вид.',
        'Бинго RU / EN: меню, cvar help, автотранслит и `/en`-перевод встроены системно, а не по остаточному принципу.',
      ],
      en: [
        'Fully compatible with any Q2 server (vanilla protocol 34, R1Q2 35, Q2PRO 36).',
        'Global, mod-related, and demo visual settings scopes keep demos and mods from fighting each other.',
        'Bilingual RU / EN: menus, cvar help, auto-translit, and `/en` translation are built in systematically.',
      ],
    },
    callout: {
      ru: 'Сервер всегда авторитетен. Q2PRO-X только лучше показывает и предсказывает то, что уже происходит.',
      en: 'The server remains authoritative. Q2PRO-X just visualises and predicts what is already happening, more cleanly.',
    },
    images: [
      { file: '01-overview.png',  alt: { ru: 'Q2PRO-X в игре',                   en: 'Q2PRO-X in-game' },
                                  caption: { ru: 'Общий вид клиента',            en: 'Client overview' } },
      { file: '02-brand.png',     alt: { ru: 'Фирменный стиль Q2PRO-X',          en: 'Q2PRO-X branding' },
                                  caption: { ru: 'Логотип и идентификация',      en: 'Logo and identity' } },
      { file: '03-release-1-2-logo.png', alt: { ru: 'Логотип Q2PRO-X 1.2',       en: 'Q2PRO-X 1.2 logo' },
                                  caption: { ru: 'Функциональная карта релиза 1.2', en: '1.2 feature map' } },
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
      { file: '01-hud.png',       alt: { ru: 'LAGHAX HUD в игре',              en: 'LAGHAX HUD in-game' },
                                  caption: { ru: 'LAGHAX HUD при активной коррекции', en: 'LAGHAX HUD during active correction' } },
      { file: '02-settings.png',  alt: { ru: 'Меню настроек LAGHAX',           en: 'LAGHAX settings menu' },
                                  caption: { ru: 'Настройки в guided-меню',    en: 'Settings in the guided menu' } },
      { file: '03-runtime.png',   alt: { ru: 'LAGHAX HUD с блоком rt:',        en: 'LAGHAX HUD with rt: block' },
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
      { file: '01-settings.png',  alt: { ru: 'Меню предсказания оружия',       en: 'Weapon predict settings' },
                                  caption: { ru: 'Настройки предиктов',         en: 'Per-weapon toggles' } },
      { file: '02-rocket.png',    alt: { ru: 'Ракета ghost → server',           en: 'Rocket ghost → server' },
                                  caption: { ru: 'Rocket handoff в действии',   en: 'Rocket handoff in action' } },
      { file: '03-runtime.png',   alt: { ru: 'rt: runtime-диагностика',         en: 'rt: runtime diagnostics' },
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
      ru: 'Четыре режима ощущения движения плюс плавный шаг по лестницам и ступенькам.',
      en: 'Four movement-feel modes plus smoothed stepping on stairs and ledges.',
    },
    body: {
      ru: [
        'Классический Q2, Q2PRO и R1Q2 предлагают три разных "ощущения" движения — у них немного отличается прогнозирование, шаг и subframe-физика. Q2PRO-X даёт выбрать любую из них одной настройкой вместо правки набора cvar.',
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
      { file: '01-modes.png',     alt: { ru: 'Меню режимов движения',       en: 'Movement modes menu' },
                                  caption: { ru: 'Выбор movement feel mode', en: 'Movement feel mode selection' } },
      { file: '02-step.png',      alt: { ru: 'Step smoothing modes',         en: 'Step smoothing modes' },
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
      { file: '01-binaural.png',  alt: { ru: 'Меню binaural',                en: 'Binaural settings' },
                                  caption: { ru: 'Настройки бинаурального 3D', en: 'Binaural 3D settings' } },
      { file: '02-reverb.png',    alt: { ru: 'Меню EAX reverb',               en: 'EAX reverb menu' },
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
        'В 1.2 подсветки получили отдельный слой для model color overrides: можно красить конкретные модели игроков именованными цветами, а на демках использовать единый demo visual profile независимо от мода, в котором была записана демка.',
      ],
      en: [
        'In-match you mostly need to see your opponent, your damage, and the projectile streams. Q2PRO-X exposes surgical knobs: held-weapon transparency, segmented model highlights, bloom, gamma scale, damage-blend intensity — all with defaults that do not alter the classic look.',
        'In 1.2, highlights gained model color overrides: individual player models can be colored with named colors, while demos can use one dedicated demo visual profile regardless of the mod that recorded them.',
      ],
    },
    bullets: {
      ru: [
        '`gl_bloom`, `gl_brightness`, `gl_saturation`, `gl_celshading`.',
        'Настройки подсветки и частей оружия отдельным меню.',
        'Player mode умеет team/duel-логику и принудительные model-color overrides для демок.',
        'Управление screen-blend при получении урона (`gl_damageblend_frac`).',
      ],
      en: [
        '`gl_bloom`, `gl_brightness`, `gl_saturation`, `gl_celshading`.',
        'Dedicated menu for highlights and weapon-part visibility.',
        'Player mode supports team/duel logic and forced model-color overrides for demos.',
        'Damage screen-blend tuning via `gl_damageblend_frac`.',
      ],
    },
    callout: null,
    images: [
      { file: '01-highlights.png',alt: { ru: 'Меню подсветки',                en: 'Highlights menu' },
                                  caption: { ru: 'Настройки подсветки модели', en: 'Model highlight settings' } },
      { file: '02-visuals.png',   alt: { ru: 'Visual FX меню',                en: 'Visual FX menu' },
                                  caption: { ru: 'Полный набор визуальных настроек', en: 'Full visual tuning panel' } },
      { file: '03-item-player-highlights.jpg', alt: { ru: 'Подсветки в игре', en: 'In-game highlights' },
                                  caption: { ru: 'Item / player / HUD readability в живой сцене', en: 'Item / player / HUD readability in a live scene' } },
    ],
  },

  /* ───────────────────────── OVERLAYS ───────────────────────── */
  {
    id: 'overlays',
    slug: 'overlays',
    icon: '▦',
    title: {
      ru: 'Overlays & Tools',
      en: 'Overlays & Tools',
    },
    summary: {
      ru: 'Современный server browser, cvar browser, mod overlay и LAGHAX HUD — всё встроено, без сторонних утилит.',
      en: 'Modern server browser, cvar browser, mod overlay, and LAGHAX HUD — all in-tree, no third-party utilities.',
    },
    body: {
      ru: [
        'Q2PRO-X добавляет набор встроенных оверлеев, которые раньше требовали отдельных утилит или costly консольных манёвров. Server browser с поиском и избранным, cvar browser с полной документацией на русском и английском, mod overlay для быстрых переключений, LAGHAX HUD со статистикой.',
        'Все оверлеи можно перетаскивать мышью, ресайзить, настраивать прозрачность независимо от `scr_alpha`. Есть `Ctrl+C` и Unicode-буфер — из cvar browser можно копировать описания прямо в чат / bug-репорт.',
      ],
      en: [
        'Q2PRO-X ships a set of in-tree overlays that used to require third-party tools or console gymnastics. A server browser with search and favourites, a cvar browser with full RU/EN documentation, a mod overlay for fast toggles, and the LAGHAX HUD.',
        'Every overlay is drag-to-move, resize-to-taste, and has independent alpha. `Ctrl+C` / Unicode clipboard work — you can copy a cvar reference straight into chat or a bug report.',
      ],
    },
    bullets: {
      ru: [
        'Server browser с фильтром и избранным, интеграция с GameTracker.',
        'CVAR Browser: 837 переменных с RU/EN-описаниями и 49 быстрых команд во вкладке Commands.',
        'Mod overlay — быстрый переключатель по избранным модам.',
        'LAGHAX HUD с live-статистикой и опциональными диагностическими блоками.',
      ],
      en: [
        'Server browser with filtering + favourites, GameTracker integration.',
        'Cvar browser with 837 cvar entries in RU/EN plus 49 quick-command rows in the Commands tab.',
        'Mod overlay — fast toggle across favourite mods.',
        'LAGHAX HUD with live stats and optional diagnostic blocks.',
      ],
    },
    callout: null,
    images: [
      { file: '01-server-browser.png', alt: { ru: 'Server browser',          en: 'Server browser' },
                                       caption: { ru: 'Поиск и избранное',   en: 'Search + favourites' } },
      { file: '02-cvar-browser.png',   alt: { ru: 'Cvar browser',            en: 'Cvar browser' },
                                       caption: { ru: 'Cvar browser с RU-описанием', en: 'Cvar browser with RU descriptions' } },
      { file: '03-mod-overlay.png',    alt: { ru: 'Mod overlay',             en: 'Mod overlay' },
                                       caption: { ru: 'Быстрый переключатель модов', en: 'Fast mod switcher' } },
      { file: '04-laghax-overlay.png', alt: { ru: 'LAGHAX HUD overlay',      en: 'LAGHAX HUD overlay' },
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
      ru: 'Guided/classic режимы меню, современные шрифты, прозрачность, масштаб, console tuning и миграция рекомендованных настроек 1.2.',
      en: 'Guided/classic menu modes, modern fonts, alpha, scale, console tuning, and recommended 1.2 settings migration.',
    },
    body: {
      ru: [
        '1.2 делает первый запуск и апгрейд мягче: если старый глобальный конфиг не знает schema 1.2, клиент один раз применяет рекомендованные UI/defaults, записывает маркер версии и дальше уважает пользовательские настройки.',
        'Интерфейс можно держать классическим или перейти на guided/modern меню. Отдельно настраиваются размеры и прозрачность меню/консоли, язык, шрифты, layout браузеров и поведение сохранения q2pro-x.cfg.',
      ],
      en: [
        '1.2 makes first launch and upgrades smoother: if the old global config has no schema 1.2 marker, the client applies recommended UI/defaults once, writes the version marker, and then respects user settings.',
        'The interface can stay classic or move to the guided/modern menu. Menu/console size and alpha, language, fonts, browser layout, and q2pro-x.cfg save behavior are controlled separately.',
      ],
    },
    bullets: {
      ru: [
        'Guided / classic menu mode, modern menu assets and first-run defaults.',
        'Настройки console/menu scale, alpha, font и readable layouts для больших экранов.',
        'Разделение global / mod-local / demo visual config сохраняет поведение предсказуемым.',
      ],
      en: [
        'Guided / classic menu mode, modern menu assets, and first-run defaults.',
        'Console/menu scale, alpha, font, and readable layouts for larger displays.',
        'Global / mod-local / demo visual config split keeps behavior predictable.',
      ],
    },
    callout: {
      ru: 'Главная идея интерфейса 1.2: меньше обязательной ручной настройки через консоль, больше понятных экранов и безопасных reset-действий.',
      en: 'The main 1.2 interface idea: fewer required console tweaks, more readable screens, and safer reset actions.',
    },
    images: [
      { file: '01-modern-menu.jpg', alt: { ru: 'Современное меню Q2PRO-X', en: 'Modern Q2PRO-X menu' },
                                   caption: { ru: 'Меню, auto-port scan и настройки в живом клиенте', en: 'Menu, auto-port scan, and settings in the live client' } },
      { file: '02-menu-console.png', alt: { ru: 'Карта interface-настроек', en: 'Interface settings map' },
                                   caption: { ru: 'Меню, консоль, сохранение и schema 1.2', en: 'Menu, console, saving, and schema 1.2' } },
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
        'CVAR Browser — это не просто список переменных. Он показывает описание, scope, default, flags и связанные команды, чтобы игрок мог управлять клиентом из интерфейса, а не держать в голове десятки cvar.',
        'В 1.2 особенно важны scope и reset-команды: global-настройки, mod-related настройки, demo visual profile и рекомендованные defaults должны сохраняться там, где пользователь ожидает.',
      ],
      en: [
        'The CVAR Browser is more than a variable list. It shows description, scope, default, flags, and related commands so players can configure the client through an interface instead of memorizing dozens of cvars.',
        'In 1.2, scope and reset commands matter even more: global settings, mod-related settings, the demo visual profile, and recommended defaults need to save exactly where users expect.',
      ],
    },
    bullets: {
      ru: [
        'Поиск по имени и описанию, RU/EN тексты, удобные command rows.',
        'Сбросы Q2PRO-X defaults для global/menu/UI сценариев.',
        'Отдельная документация объясняет, какие cvar живут в каком конфиге.',
      ],
      en: [
        'Search by name and description, RU/EN text, and useful command rows.',
        'Q2PRO-X defaults reset actions for global/menu/UI scenarios.',
        'Dedicated docs explain which cvars live in which config.',
      ],
    },
    callout: null,
    images: [
      { file: '01-cvar-browser.png', alt: { ru: 'CVAR Browser', en: 'CVAR Browser' },
                                    caption: { ru: 'Поиск и описание переменных', en: 'Search and variable descriptions' } },
      { file: '02-cvar-commands.png', alt: { ru: 'Command browser', en: 'Command browser' },
                                    caption: { ru: 'Команды, defaults и scope', en: 'Commands, defaults, and scope' } },
    ],
  },

  /* ───────────────────────── NETWORK ───────────────────────── */
  {
    id: 'network-play',
    slug: 'network',
    icon: '↔',
    title: {
      ru: 'Network Play',
      en: 'Network Play',
    },
    summary: {
      ru: 'Server browser, auto-port scan, OpenTDM HUD, laghax/predict настройки, RU input, автотранслит и `/en` перевод.',
      en: 'Server browser, auto-port scan, OpenTDM HUD, laghax/predict controls, RU input, auto-translit, and `/en` translation.',
    },
    body: {
      ru: [
        'Сетевой блок 1.2 закрывает путь от поиска сервера до комфортной игры: браузер серверов, расширенный скан портов, избранное, OpenTDM team HUD, настройки laghax/predict и visibility прямо в меню.',
        'Отдельный слой для коммуникации помогает смешанным RU/EN матчам: русская раскладка, автотранслит и команда `/en`, которая переводит сообщение на английский перед отправкой.',
      ],
      en: [
        'The 1.2 network layer covers the path from finding a server to playing comfortably: server browser, extended port scanning, favourites, OpenTDM team HUD, laghax/predict controls, and visibility settings from the menu.',
        'A communication layer helps mixed RU/EN matches: Russian input, auto-translit, and the `/en` command that translates a message to English before sending.',
      ],
    },
    bullets: {
      ru: [
        'Server browser и auto-port scan для поиска живых серверов и нестандартных портов.',
        'OpenTDM/team HUD и настройки сетевых predictor/laghax сценариев.',
        'Автотранслит и `/en` перевод для чата без внешних утилит.',
      ],
      en: [
        'Server browser and auto-port scan for live servers and non-standard ports.',
        'OpenTDM/team HUD plus network predictor/laghax settings.',
        'Auto-translit and `/en` translation for chat without external utilities.',
      ],
    },
    callout: {
      ru: 'Все сетевые удобства остаются client-side: серверная авторитетность и правила матча не меняются.',
      en: 'All network conveniences stay client-side: server authority and match rules remain unchanged.',
    },
    images: [
      { file: '01-opentdm-team-hud.png', alt: { ru: 'OpenTDM team HUD', en: 'OpenTDM team HUD' },
                                        caption: { ru: 'Командный HUD в матче', en: 'Team HUD in a match' } },
      { file: '02-server-browser.png', alt: { ru: 'Server Browser', en: 'Server Browser' },
                                        caption: { ru: 'Поиск серверов и избранное', en: 'Server search and favourites' } },
      { file: '03-translation.png', alt: { ru: 'Network Play карта', en: 'Network Play map' },
                                        caption: { ru: 'Серверы, laghax, ru input и перевод', en: 'Servers, laghax, RU input, and translation' } },
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
        'Voice Chat в 1.2 включается из меню и может автоматически подключаться к серверной комнате при заходе на игровой сервер. Игроку не нужно руками задавать room name, если включён autojoin/server room mode.',
        'Настройки микрофона, VAD threshold, PTT/toggle keys, mic gain, receive volume, deafen и control server сохраняются в обычном Q2PRO-X профиле.',
      ],
      en: [
        'Voice Chat in 1.2 is controlled from the menu and can automatically join the server room when you connect to a game server. The player does not need to type a room name manually when autojoin/server room mode is enabled.',
        'Mic device, VAD threshold, PTT/toggle keys, mic gain, receive volume, deafen, and control server are saved in the normal Q2PRO-X profile.',
      ],
    },
    bullets: {
      ru: [
        'autojoin on connect + server/manual room modes.',
        'VAD, push-to-talk и mic toggle сценарии.',
        'Подробное руководство Voice Chat входит в docs 1.2.',
      ],
      en: [
        'autojoin on connect + server/manual room modes.',
        'VAD, push-to-talk, and mic toggle workflows.',
        'A detailed Voice Chat guide is included in the 1.2 docs.',
      ],
    },
    callout: null,
    images: [
      { file: '01-voice-chat.png', alt: { ru: 'Voice Chat карта', en: 'Voice Chat map' },
                                  caption: { ru: 'Голосовые комнаты, VAD/PTT и control server', en: 'Voice rooms, VAD/PTT, and control server' } },
    ],
  },

  /* ───────────────────────── DEMOS ───────────────────────── */
  {
    id: 'demos',
    slug: 'demos',
    icon: '▶',
    title: {
      ru: 'Demo Browser, Player & Analytics',
      en: 'Demo Browser, Player & Analytics',
    },
    summary: {
      ru: 'Новый браузер демок, player popup, MVD/DM2 playback, next-frag director, analytics heatmap/trails и item timer overlays.',
      en: 'New demo browser, player popup, MVD/DM2 playback, next-frag director, analytics heatmap/trails, and item timer overlays.',
    },
    body: {
      ru: [
        'Demo product family в 1.2 превращает просмотр демок в отдельный рабочий режим. Демо можно выбирать из браузера, запускать через новый player, переключать MVD/DM2 сценарии и держать графику стабильной через demo visual config.',
        'Аналитика добавляет next-frag/director идеи, heatmap, movement trails и item timer overlays для armors, powerups, megasphere/megahealth и других важных девайсов в стиле Quake Live.',
      ],
      en: [
        'The 1.2 demo product family turns demo watching into its own workflow. Demos can be selected from the browser, launched through the new player, handled across MVD/DM2 scenarios, and kept visually stable through the demo visual config.',
        'Analytics adds next-frag/director ideas, heatmap, movement trails, and item timer overlays for armors, powerups, megahealth, and other key devices in the Quake Live style.',
      ],
    },
    bullets: {
      ru: [
        'Demo visual profile отделяет внешний вид демок от mod-local q2pro-x.cfg.',
        'Popup-настройки аналитики доступны из настроек demo player и из самого player.',
        'Movement trails, heatmap colors и item timers настраиваются под нужный способ просмотра.',
      ],
      en: [
        'Demo visual profile separates demo appearance from mod-local q2pro-x.cfg.',
        'Analytics popup settings are reachable from demo player settings and from the player itself.',
        'Movement trails, heatmap colors, and item timers can be tuned for the way you review demos.',
      ],
    },
    callout: {
      ru: 'Демо-инструменты работают внутри клиента: можно быстро найти запись, запустить просмотр и держать отдельный визуальный профиль для демок, не ломая обычные игровые настройки.',
      en: 'Demo tools live inside the client: find a recording, start playback, and keep a separate visual profile for demos without disturbing normal play settings.',
    },
    images: [
      { file: '01-demo-player.png', alt: { ru: 'Demo Player карта', en: 'Demo Player map' },
                                    caption: { ru: 'Браузер, player popup, director и demo visual config', en: 'Browser, player popup, director, and demo visual config' } },
      { file: '02-demo-analytics.png', alt: { ru: 'Demo Analytics карта', en: 'Demo Analytics map' },
                                    caption: { ru: 'Trails, height mode, colors и item timers', en: 'Trails, height mode, colors, and item timers' } },
    ],
  },
];

/* ───────────────────────── DOCUMENTATION ───────────────────────── */

// Real per-language 1.2 documentation files. Served from
// `O:\Claude\public\q2pro-x\docs\{ru,en}\` via the static mount
// registered in `routes.js`. Browsers open `.docx` as a download or
// delegate to the OS office handler — both UX paths are acceptable.
//
// To change a filename or swap to a different format (html, pdf, md):
// just edit the URL here; render/route layers don't hardcode names.
const docs = {
  index: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Documentation_Index_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Documentation_Index_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Documentation_Index_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Documentation_Index_EN.docx',
    },
  },
  release: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Release_Notes_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Release_Notes_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Release_Notes_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Release_Notes_EN.docx',
    },
  },
  interface: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Interface_Menu_Console_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Interface_Menu_Console_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Interface_Menu_Console_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Interface_Menu_Console_Guide_EN.docx',
    },
  },
  video: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Video_Visuals_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Video_Visuals_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Video_Visuals_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Video_Visuals_Guide_EN.docx',
    },
  },
  cvars: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Cvars_Cvar_Browser_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Cvars_Cvar_Browser_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Cvars_Cvar_Browser_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Cvars_Cvar_Browser_Guide_EN.docx',
    },
  },
  voice: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Voice_Chat_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Voice_Chat_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Voice_Chat_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Voice_Chat_Guide_EN.docx',
    },
  },
  network: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Network_Play_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Network_Play_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Network_Play_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Network_Play_Guide_EN.docx',
    },
  },
  controls: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Controls_Mouse_Zoom_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Controls_Mouse_Zoom_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Controls_Mouse_Zoom_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Controls_Mouse_Zoom_Guide_EN.docx',
    },
  },
  demos: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Demo_Browser_Player_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Demo_Browser_Player_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Demo_Browser_Player_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Demo_Browser_Player_Guide_EN.docx',
    },
  },
  sound: {
    ru: {
      preview: '/q2pro-x/docs-preview/ru/Q2PRO-X_1.2_Sound_Guide_RU.html',
      download: '/q2pro-x/docs/ru/Q2PRO-X_1.2_Sound_Guide_RU.docx',
    },
    en: {
      preview: '/q2pro-x/docs-preview/en/Q2PRO-X_1.2_Sound_Guide_EN.html',
      download: '/q2pro-x/docs/en/Q2PRO-X_1.2_Sound_Guide_EN.docx',
    },
  },
};

/* ───────────────────────── RELEASE / DOWNLOADS ───────────────────────── */

const release = {
  version: '1.2',
  available: true,
  downloadHref: '/q2pro-x/downloads/q2pro-x-1.2.rar',
  popupTitle: {
    ru: 'Скачать Q2PRO-X',
    en: 'Download Q2PRO-X',
  },
  popupMessage: {
    ru: 'Q2PRO-X 1.2 уже доступен как единый стабильный пакет.',
    en: 'Q2PRO-X 1.2 is available as a single stable package.',
  },
  sectionTitle: {
    ru: 'Скачать Q2PRO-X 1.2',
    en: 'Download Q2PRO-X 1.2',
  },
  sectionSummary: {
    ru: 'Актуальный стабильный архив 1.2, документация 1.2 и рекомендованный путь первого запуска.',
    en: 'The current stable 1.2 archive, 1.2 documentation, and the recommended first-launch path.',
  },
  sectionBody: {
    ru: [
      'Сайт отдаёт актуальный архив Q2PRO-X 1.2 напрямую. После загрузки распакуйте пакет в вашу рабочую папку Quake II / Q2PRO-X и запускайте `Q2PRO-X.exe` локально, без дополнительных лаунчеров.',
      'При первом запуске или обновлении со старой сборки релиз применяет рекомендованные настройки меню, UI, видео-бэкенда и профилей один раз, если глобальный q2pro-x.cfg ещё не знает версию 1.2.',
    ],
    en: [
      'The website serves the current Q2PRO-X 1.2 archive directly. After downloading, unpack the package into your Quake II / Q2PRO-X working folder and run `Q2PRO-X.exe` locally without extra launchers.',
      'On first launch or when upgrading from an older build, the release applies recommended menu, UI, video-backend, and profile settings once if the global q2pro-x.cfg does not yet know version 1.2.',
    ],
  },
  quickStartTitle: {
    ru: 'Быстрый старт',
    en: 'Quick start',
  },
  quickStartSummary: {
    ru: 'Минимальные шаги, чтобы скачать, распаковать и сразу запустить Q2PRO-X в нужном режиме меню.',
    en: 'The minimum set of steps to download, unpack, and launch Q2PRO-X with the menu mode you actually want.',
  },
  quickStartSteps: {
    ru: [
      {
        title: '1. Скачайте и распакуйте архив',
        text: 'Скачайте `q2pro-x-1.2.rar` и распакуйте его в вашу папку Quake II / Q2PRO-X. Если держите несколько сборок отдельно, лучше выделить для Q2PRO-X собственную рабочую папку.',
      },
      {
        title: '2. Первый запуск = рекомендованные defaults 1.2',
        text: 'При отсутствии маркера schema 1.2 клиент один раз применит рекомендуемые UI/defaults, включая нормальный video backend `win32egl`, menu defaults и актуальные глобальные Q2PRO-X настройки.',
      },
      {
        title: '3. Настройте интерфейс из меню',
        text: 'Выберите guided/classic режим, масштаб и прозрачность меню/консоли, язык, voice, network и demo player настройки. После первого применения schema marker пользовательские настройки больше не перетираются.',
      },
      {
        title: '4. Для деталей откройте docs 1.2',
        text: 'В архив и на сайт добавлены отдельные DOCX/preview по интерфейсу, видео, cvar browser, voice, network play, управлению, демкам и звуку.',
      },
    ],
    en: [
      {
        title: '1. Download and unpack the archive',
        text: 'Download `q2pro-x-1.2.rar` and unpack it into your Quake II / Q2PRO-X working folder. If you keep multiple builds side by side, giving Q2PRO-X its own working folder is the cleaner option.',
      },
      {
        title: '2. First launch = recommended 1.2 defaults',
        text: 'If the schema 1.2 marker is missing, the client applies recommended UI/defaults once, including the normal `win32egl` video backend, menu defaults, and current global Q2PRO-X settings.',
      },
      {
        title: '3. Tune the interface from the menu',
        text: 'Choose guided/classic mode, menu/console scale and alpha, language, voice, network, and demo player settings. After the schema marker is written, user settings are no longer overwritten.',
      },
      {
        title: '4. Open the 1.2 docs for details',
        text: 'The archive and the website include dedicated DOCX/preview guides for interface, video, cvar browser, voice, network play, controls, demos, and sound.',
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
      'На данный момент публичный сайт ведёт к стабильному релизу 1.2. Когда появится новая beta-линия, этот блок будет открыт с отдельным описанием, архивом и точками внимания для тестировщиков.',
    ],
    en: [
      'Right now the public site points to the stable 1.2 release. When a new beta line appears, this block will open with its own description, archive, and tester focus areas.',
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
      file: '01-opentdm-team-hud.png',
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
      file: '02-predict-adaptive-hud.png',
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
      file: '03-mouse-behavior-menu.png',
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
      file: '04-legacy-rail-compat.png',
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
      file: '05-beta-overview.png',
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
      file: '06-beta-feature.png',
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

const brandingVersion = '2026-04-28-release-1-2';

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
    ru: 'Полный комплект Q2PRO-X 1.2: релизные заметки и отдельные подробные руководства по интерфейсу, видео, cvar, voice, сети, управлению, демкам и звуку.',
    en: 'The full Q2PRO-X 1.2 documentation set: release notes plus dedicated guides for interface, video, cvars, voice, network play, controls, demos, and sound.',
  },
  items: [
    {
      key:   'index',
      title: { ru: 'Индекс документации 1.2',      en: 'Documentation Index 1.2' },
      desc:  { ru: 'Карта всех документов релиза и быстрый выбор нужного руководства.',
               en: 'Map of all release documents and a quick way to choose the right guide.' },
    },
    {
      key:   'release',
      title: { ru: 'Release Notes 1.2',            en: 'Release Notes 1.2' },
      desc:  { ru: 'Главные изменения, возможности и поведение стабильного релиза 1.2.',
               en: 'Main changes, features, and behavior of the stable 1.2 release.' },
    },
    {
      key:   'interface',
      title: { ru: 'Интерфейс, меню и консоль',    en: 'Interface, Menu & Console' },
      desc:  { ru: 'Guided/classic меню, сохранение настроек, scale/alpha, console UI и schema migration.',
               en: 'Guided/classic menus, settings persistence, scale/alpha, console UI, and schema migration.' },
    },
    {
      key:   'video',
      title: { ru: 'Видео и подсветки',            en: 'Video & Visuals' },
      desc:  { ru: 'Видео-настройки, visibility highlights, model color overrides и demo visual profile.',
               en: 'Video settings, visibility highlights, model color overrides, and the demo visual profile.' },
    },
    {
      key:   'cvars',
      title: { ru: 'CVAR Browser',                 en: 'CVAR Browser' },
      desc:  { ru: 'Поиск переменных, command rows, flags, scope и reset-команды.',
               en: 'Variable search, command rows, flags, scope, and reset commands.' },
    },
    {
      key:   'voice',
      title: { ru: 'Voice Chat',                   en: 'Voice Chat' },
      desc:  { ru: 'Комнаты, autojoin, VAD/PTT, микрофон, gain, receive volume и hosted control server.',
               en: 'Rooms, autojoin, VAD/PTT, microphone, gain, receive volume, and hosted control server.' },
    },
    {
      key:   'network',
      title: { ru: 'Сетевая игра',                 en: 'Network Play' },
      desc:  { ru: 'Server browser, auto-port scan, laghax/predict, OpenTDM HUD, RU input и `/en`.',
               en: 'Server browser, auto-port scan, laghax/predict, OpenTDM HUD, RU input, and `/en`.' },
    },
    {
      key:   'controls',
      title: { ru: 'Управление, мышь и zoom',      en: 'Controls, Mouse & Zoom' },
      desc:  { ru: 'R1Q2-style mouse behavior, sensitivity, zoom FOV, zoom sens и key binds.',
               en: 'R1Q2-style mouse behavior, sensitivity, zoom FOV, zoom sens, and key binds.' },
    },
    {
      key:   'demos',
      title: { ru: 'Демки и demo player',          en: 'Demos & Demo Player' },
      desc:  { ru: 'Demo browser, MVD/DM2 playback, visual profile, director, analytics и timers.',
               en: 'Demo browser, MVD/DM2 playback, visual profile, director, analytics, and timers.' },
    },
    {
      key:   'sound',
      title: { ru: 'Руководство по звуку',        en: 'Sound Guide' },
      desc:  { ru: 'OpenAL, HRTF, бинауральный звук, реверберация и практическая настройка.',
               en: 'OpenAL, HRTF, binaural sound, reverb, and practical setup.' },
    },
  ],
};

/* ───────────────────────── META / SEO ───────────────────────── */

const meta = {
  title: {
    ru: 'Q2PRO-X 1.2 — умный клиент Quake II',
    en: 'Q2PRO-X 1.2 — a smart Quake II client',
  },
  description: {
    ru: 'Q2PRO-X 1.2 — современный клиент Quake II на базе Q2PRO: удобный запуск, онлайн-инструменты, Voice Chat, демки, визуальные настройки и документация RU/EN.',
    en: 'Q2PRO-X 1.2 is a modern Q2PRO-based Quake II client with easier setup, online tools, Voice Chat, demos, visual tuning, and RU/EN documentation.',
  },
  // Social preview — points at the real brand asset shipped under
  // public/q2pro-x/branding/. Swap this to an in-game hero screenshot
  // later once one is produced; the brand logo is the sensible default.
  ogImage: '/q2pro-x/branding/Q2PRO-X_1_0_wordmark_dark.png',
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

function mediaPath(section, file) {
  return '/q2pro-x/media/' + section.slug + '/' + file;
}

function resolveLang(raw) {
  const v = String(raw || '').toLowerCase();
  return supportedLangs.includes(v) ? v : defaultLang;
}

function resolveTheme(raw) {
  const v = String(raw || '').toLowerCase();
  return supportedThemes.includes(v) ? v : defaultTheme;
}

module.exports = {
  ui, hero, sections, docs, docsSection, release, beta, links, branding, meta,
  supportedLangs, defaultLang, supportedThemes, defaultTheme,
  pick, mediaPath, resolveLang, resolveTheme,
};
