const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const content = require('../content');
const { renderPage } = require('../render');
const { CSS } = require('../styles');

const SITE_ROOT = path.resolve(__dirname, '..');
const expectedSectionIds = [
  'overview', 'avfx', 'hdr', 'avfx-presets', 'laghax', 'weapon-predict',
  'movement-physics', 'sound', 'visuals', 'visual-highlights', 'overlays',
  'interface', 'startup-intro', 'cvars', 'network-play', 'voice', 'demos',
  'single-player-mods',
];

const expectedDocKeys = [
  'index', 'release', 'networkGod', 'network', 'avfxAdvanced',
  'avfxGenerator', 'avfxPresets', 'avfx', 'gameStatistics', 'startupIntro',
  'liveTimers', 'hdr', 'highlights', 'singleplayerMods', 'interface',
  'cvarReference', 'video', 'cvars', 'voice', 'controls', 'demos', 'sound',
];

function visibleText(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ');
}

function renderedMediaUrls(html) {
  return [...html.matchAll(/<img[^>]+src="([^"]+)"/g)]
    .map((match) => match[1].split('?')[0])
    .filter((url) => url.startsWith('/q2pro-x/media/'));
}

test('1.5 keeps every established section and adds Startup Intro', () => {
  assert.deepEqual(content.sections.map((section) => section.id), expectedSectionIds);
  assert.equal(content.navItems.length, 8, 'desktop header must stay compact');

  const ru = visibleText(renderPage({ lang: 'ru', theme: 'dark' }));
  const en = visibleText(renderPage({ lang: 'en', theme: 'light' }));

  for (const token of [
    'Бог сети', 'Ping Adaptation', 'библиотека пресетов',
    'Статистика игры', 'Startup Intro', 'Живые таймеры', 'Performance HUD',
    'Свободный мир',
  ]) {
    assert.ok(ru.includes(token), `missing RU release token: ${token}`);
  }
  for (const token of [
    'Network God', 'Ping Adaptation', 'preset library',
    'Game Statistics', 'Startup Intro', 'live item timers', 'Performance HUD',
    'Free World',
  ]) {
    assert.ok(en.includes(token), `missing EN release token: ${token}`);
  }

  assert.doesNotMatch(ru, /\b1\.[0-4]\b/, 'visible RU page contains pre-1.5 version ownership');
  assert.doesNotMatch(en, /\b1\.[0-4]\b/, 'visible EN page contains pre-1.5 version ownership');
  assert.match(content.meta.ogImage, /Q2PRO-X_1_5_release_cover\.png\?v=/);
  assert.ok(content.publicAssetExists(content.meta.ogImage), '1.5 social preview is missing');
  assert.equal(content.meta.ogImage.includes('logo_master_site'), false, 'stale release poster is still used');
});

test('visible copy describes extensible inventories without freezing their sizes', () => {
  const ru = visibleText(renderPage({ lang: 'ru', theme: 'dark' }));
  const en = visibleText(renderPage({ lang: 'en', theme: 'light' }));

  const volatileInventoryClaims = [
    /(?<![\d.])\b\d+\s+(?:поставляемых|уникальных|готовых)?\s*(?:AVFX-)?пресет/i,
    /(?<![\d.])\b\d+\s+(?:shipped|unique|built-in)?\s*(?:AVFX )?presets?/i,
    /(?<![\d.])\b\d+\s+публичн\w*\s+команд/i,
    /(?<![\d.])\b\d+\s+public commands?/i,
    /(?<![\d.])\b\d+\s+диагностическ\w*\s+запис/i,
    /(?<![\d.])\b\d+\s+diagnostic entries/i,
    /(?:одиннадцать|11)\s+независим\w+\s+(?:селектор|групп)/i,
    /(?:eleven|11)\s+independent\s+(?:selectors?|groups?)/i,
    /(?<![\d.])\b\d+\s+(?:русск\w*|английск\w*)\s+(?:документ|руководств)/i,
    /(?<![\d.])\b\d+\s+(?:Russian|English)\s+(?:documents?|guides?)/i,
    /шесть\s+(?:семейств\s+)?живых/i,
    /six\s+(?:families\s+of\s+)?living/i,
    /четыр[её]х\s+групп\w*\s+пользователь/i,
    /four\s+custom-preset\s+groups/i,
  ];

  for (const [lang, text] of [['ru', ru], ['en', en]]) {
    for (const pattern of volatileInventoryClaims) {
      assert.doesNotMatch(text, pattern, `${lang}: volatile inventory claim ${pattern}`);
    }
  }
});

test('AVFX preset copy never freezes preset names or category inventories', () => {
  const ru = visibleText(renderPage({ lang: 'ru', theme: 'dark' }));
  const en = visibleText(renderPage({ lang: 'en', theme: 'light' }));
  const presetSection = content.sections.find((section) => section.id === 'avfx-presets');
  const sectionCopy = JSON.stringify(presetSection);

  for (const presetName of [
    'Царство мха', 'Космос по домашнему', 'Изумрудный лоск',
    'Мокро, очень мокро', 'Жёлто-зелёное движение',
    'Moss Kingdom', 'Home-Style Space', 'Emerald Gloss',
    'Wet, Very Wet', 'Yellow-Green Motion',
    'Подсветка + визуал', 'Highlights + Visuals',
    'Rain + Storm Ultra Quality', 'Snow Ultra Quality',
  ]) {
    assert.equal(ru.includes(presetName) || en.includes(presetName), false,
      `visible page freezes a dynamic preset name: ${presetName}`);
  }

  for (const inventoryPattern of [
    /погод\w*,\s*поверхност\w*,\s*космос/i,
    /weather,\s*surfaces,\s*space/i,
    /Пакеты 1\.5:/i,
    /The 1\.5 packs:/i,
    /Категории включают/i,
    /Categories include/i,
    /нов\w+\s+категори\w+\s+«[^»]+»/i,
    /new\s+[\w-]+\s+category/i,
    /global\s+(?:и|and)\s+mod-related/i,
    /обычн\w+\s+и\s+музыкальн/i,
    /ordinary\s+and\s+music/i,
    /music\/ordinary.*global\/mod-related/i,
  ]) {
    assert.doesNotMatch(sectionCopy, inventoryPattern,
      `AVFX preset section freezes a category inventory: ${inventoryPattern}`);
  }
});

test('generator copy exposes independent surface-material and wildlife masters', () => {
  const ru = visibleText(renderPage({ lang: 'ru', theme: 'dark' }));
  const en = visibleText(renderPage({ lang: 'en', theme: 'light' }));

  for (const token of [
    'Материалы поверхностей', 'живность',
    'Рандом / Всегда / Выключено', 'зона покрытия',
  ]) {
    assert.ok(ru.includes(token), `missing RU generator contract: ${token}`);
  }
  for (const token of [
    'Surface Materials', 'Wildlife',
    'Random / Always / Off', 'coverage zone',
  ]) {
    assert.ok(en.includes(token), `missing EN generator contract: ${token}`);
  }
});

test('timer copy explains the anti-cheat default and server-admin opt-in plainly', () => {
  const ru = visibleText(renderPage({ lang: 'ru', theme: 'dark' }));
  const en = visibleText(renderPage({ lang: 'en', theme: 'light' }));

  assert.ok(ru.includes('автоматические таймеры по умолчанию запрещены на всех онлайн-серверах'));
  assert.ok(ru.includes('администраторы серверов могут самостоятельно разрешить'));
  assert.ok(ru.includes('Руководстве по живым таймерам'));
  assert.doesNotMatch(ru, /явного или подписанного разрешения/);

  assert.ok(en.includes('automatic timers are disabled by default on every online server'));
  assert.ok(en.includes('server administrators can enable them'));
  assert.ok(en.includes('Live Item Timers Guide'));
  assert.doesNotMatch(en, /explicit or signed authorization/);
});

test('interface copy exposes only the current Guided and multiplayer menus', () => {
  const section = content.sections.find((item) => item.id === 'interface');
  const copy = JSON.stringify({
    summary: section.summary,
    body: section.body,
    bullets: section.bullets,
    callout: section.callout,
    images: section.images.map((item) => ({ alt: item.alt, caption: item.caption })),
  });

  assert.match(copy, /Guided-меню/);
  assert.match(copy, /меню для мультиплеерных игроков/);
  assert.match(copy, /Guided menu/);
  assert.match(copy, /menu for multiplayer players/);
  assert.doesNotMatch(copy, /classic|классическ|guided\s*\/\s*modern|modern menu/i);
});

test('the complete visible page never presents Classic or modern as a selectable menu mode', () => {
  const ru = visibleText(renderPage({ lang: 'ru', theme: 'dark' }));
  const en = visibleText(renderPage({ lang: 'en', theme: 'light' }));
  const retiredMenuMode = /menu mode|режим\w*\s+меню|classic menu|modern menu|классическ\w*\s+меню|современн\w*\s+меню|guided\s*\/\s*(?:classic|modern)/i;

  assert.doesNotMatch(ru, retiredMenuMode);
  assert.doesNotMatch(en, retiredMenuMode);
});

test('documentation is exactly 22 RU plus 22 EN and can never appear partially', () => {
  assert.deepEqual(Object.keys(content.docs).sort(), [...expectedDocKeys].sort());
  assert.deepEqual(content.docsSection.items.map((item) => item.key).sort(), [...expectedDocKeys].sort());

  for (const key of expectedDocKeys) {
    for (const lang of ['ru', 'en']) {
      const links = content.docs[key][lang];
      assert.match(links.preview, /Q2PRO-X_1\.5_.*\.html\?v=/);
      assert.match(links.download, /Q2PRO-X_1\.5_.*\.docx\?v=/);
    }
  }

  for (const lang of ['ru', 'en']) {
    const available = expectedDocKeys.filter((key) => {
      const links = content.docs[key][lang];
      return content.publicAssetExists(links.preview)
        && content.publicAssetExists(links.download);
    });
    assert.ok(
      available.length === 0 || available.length === expectedDocKeys.length,
      `${lang} docs are partially installed: ${available.length}/${expectedDocKeys.length}`,
    );

    const html = renderPage({ lang, theme: 'dark' });
    const cards = (html.match(/class="qpx-doc-card"/g) || []).length;
    assert.equal(cards, available.length);
    assert.equal(
      html.includes('<p class="qpx-docs__pending">'),
      available.length === 0,
    );
  }
});

test('accepted 1.5 screenshots render from real files without broken media URLs', () => {
  const acceptedScreenshots = [
    '1-5-surface-world.jpg', '1-5-space-black-hole.jpg',
    '1-5-presets-categories.jpg', '1-5-user-presets.jpg',
    '1-5-generator-main.jpg',
    '1-5-cvar-browser.jpg', '1-5-cvar-browser-commands.jpg',
    '1-5-network-god-overlay-summary.jpg',
    '1-5-network-god-overlay-events.jpg',
    '1-5-network-god-overlay-actions.jpg',
    '1-5-network-god-overlay-help.jpg',
    '1-5-game-statistics.jpg', '1-5-startup-intro.jpg',
  ];
  const retiredScreenshots = [
    'avfx_presets_main_menu.jpg', 'avfx_presets_custom_slots.jpg',
    '01-cvar-browser.jpg', '02-cvar-browser.jpg',
    '02-cvar-commands.jpg', 'ui_cvar_browser_avfx.jpg',
    '1-5-network-god.jpg',
  ];

  for (const lang of ['ru', 'en']) {
    const html = renderPage({ lang, theme: 'dark' });
    const mediaUrls = renderedMediaUrls(html);
    for (const url of mediaUrls) {
      const file = path.resolve(SITE_ROOT, 'media', url.slice('/q2pro-x/media/'.length));
      assert.ok(fs.statSync(file).isFile(), `rendered media is missing: ${file}`);
    }
    for (const screenshot of acceptedScreenshots) {
      assert.equal(
        mediaUrls.filter((url) => path.basename(url) === screenshot).length,
        1,
        `accepted screenshot is not rendered exactly once: ${screenshot}`,
      );
    }
    for (const screenshot of retiredScreenshots) {
      assert.equal(
        mediaUrls.some((url) => path.basename(url) === screenshot),
        false,
        `retired screenshot is still rendered: ${screenshot}`,
      );
    }
    assert.equal(
      (html.match(/qpx-shot__frame qpx-shot__frame--cover/g) || []).length,
      acceptedScreenshots.length,
      'accepted release screenshots must fill their cards without decorative gaps',
    );
  }
});

test('gallery layout has no incomplete desktop row and collapses on mobile', () => {
  assert.match(CSS, /grid-template-columns:\s*repeat\(2, minmax\(0, 1fr\)\)/);
  assert.match(CSS, /\.qpx-images--odd \.qpx-shot:last-child\s*\{\s*grid-column:\s*1 \/ -1;/);
  assert.match(CSS, /@media \(max-width: 680px\)[\s\S]*?\.qpx-images \{ grid-template-columns: 1fr; \}/);

  const html = renderPage({ lang: 'ru', theme: 'dark' });
  assert.ok(html.includes('qpx-images--odd'), 'odd galleries are not marked');
});

test('release action resolves to a real archive or a safe status modal', () => {
  for (const lang of ['ru', 'en']) {
    const html = renderPage({ lang, theme: 'dark' });
    assert.equal(html.includes('href="" download'), false);
    if (content.release.available) {
      const archiveUrl = content.release.downloadHref;
      assert.equal(archiveUrl.split('?')[0], '/q2pro-x/downloads/q2pro-x-1.5.rar');
      assert.match(archiveUrl, /\?v=[^&]+$/, 'release URL must invalidate stale same-name archives');
      assert.ok(content.publicAssetExists(archiveUrl), `release archive is missing: ${archiveUrl}`);
      assert.ok(html.includes(`href="${archiveUrl}" download`), 'release CTA does not point to the archive');
    } else {
      assert.ok(html.includes('data-qpx-release-open'));
      assert.ok(visibleText(html).includes('1.5'));
    }
  }
});
