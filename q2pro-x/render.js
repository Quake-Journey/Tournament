// q2pro-x/render.js
//
// Server-side HTML renderer for the /q2pro-x page. Reads all text and
// section metadata from content.js; never hardcodes visible strings.
// Reads CSS from styles.js and inline JS from client.js.
//
// Render flow:
//   renderPage({ lang }) ->
//     <!DOCTYPE html>
//     <head>   meta + inline CSS
//     <body>
//       header (brand, nav, language switcher, docs CTA)
//       hero
//       for each section: <section> with body, bullets, callout, images
//       docs block
//       footer
//       modal (lightbox host, hidden by default)
//       inline <script>
//
// Rendering is a pure function of (content, lang) → HTML. No side effects,
// no IO.

const content = require('./content');
const { CSS } = require('./styles');
const { JS }  = require('./client');

/* ── tiny HTML helpers ── */

function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// Allow inline `code` backticks → <code>…</code> inside body paragraphs.
// Nothing else is permitted — no raw HTML from content.
function renderInline(text) {
  var s = esc(text);
  return s.replace(/`([^`]+)`/g, function (_, inner) {
    return '<code>' + inner + '</code>';
  });
}

/* ── component renderers ── */

function renderHeader(lang, themeMode) {
  var ui = content.ui;
  var theme = content.resolveTheme(themeMode);
  var release = content.release;
  var showBeta = !!(content.beta && content.beta.visible);

  function opt(value, label) {
    return '<option value="' + esc(value) + '"' + (theme === value ? ' selected' : '') + '>'
         + esc(label) + '</option>';
  }

  var themeSelect = ''
    + '<label class="qpx-theme" aria-label="' + esc(content.pick(ui.themeLabel, lang)) + '">'
    +   '<span class="qpx-theme__label">' + esc(content.pick(ui.themeLabel, lang)) + '</span>'
    +   '<select class="qpx-theme__select" data-qpx-theme-select title="' + esc(content.pick(ui.themeLabel, lang)) + '">'
    +     opt('auto', content.pick(ui.themeAuto, lang))
    +     opt('dark', content.pick(ui.themeDark, lang))
    +     opt('light', content.pick(ui.themeLight, lang))
    +   '</select>'
    + '</label>';

  var navItems = content.sections.filter(function (s) {
    return s.id !== 'overview';
  }).map(function (s) {
    return '<a class="qpx-nav__item" href="#' + esc(s.id) + '">'
         +   '<span class="qpx-nav__icon">' + esc(s.icon) + '</span>'
         +   esc(content.pick(s.title, lang))
         + '</a>';
  }).join('');

  var navActions = ''
    + (showBeta
        ? '<a class="qpx-nav__item qpx-nav__item--aux" href="#beta">'
          + '<span class="qpx-nav__icon">β</span>'
          + esc(content.pick(ui.betaCta, lang))
          + '</a>'
        : '')
    + '<a class="qpx-nav__item qpx-nav__item--aux" href="#' + esc(content.docsSection.id) + '">'
    +   '<span class="qpx-nav__icon">§</span>'
    +   esc(content.pick(ui.docsCta, lang))
    + '</a>'
    + '<button type="button" class="qpx-nav__item qpx-nav__item--aux qpx-nav__item--button" data-qpx-release-open '
    +         'data-qpx-release-title="' + esc(content.pick(release.sourcesPopupTitle, lang)) + '" '
    +         'data-qpx-release-message="' + esc(content.pick(release.sourcesPopupMessage, lang)) + '">'
    +   '<span class="qpx-nav__icon">⌘</span>'
    +   esc(content.pick(ui.sourcesCta, lang))
    + '</button>'
    + '<a class="qpx-nav__item qpx-nav__item--aux qpx-nav__item--release" href="' + esc(release.downloadHref) + '" download>'
    +   '<span class="qpx-nav__icon">↓</span>'
    +   esc(content.pick(ui.releaseCta, lang))
    + '</a>';

  var downloadHref = release.downloadHref || '#';

  return ''
    + '<a class="qpx-skip" href="#qpx-main">' + esc(content.pick(ui.skip, lang)) + '</a>'
    + '<header class="qpx-header">'
    +   '<div class="qpx-wrap qpx-header__inner">'
    +     '<div class="qpx-header__top">'
    +       '<a class="qpx-brand" href="#overview" aria-label="Q2PRO-X">'
    +         '<span class="qpx-brand__mark">QX</span>'
    +         '<span>' + esc(content.pick(ui.brand, lang)) + '</span>'
    +         '<span class="qpx-brand__tag">' + esc(content.pick(ui.tagline, lang)) + '</span>'
    +       '</a>'
    +       '<button class="qpx-nav-toggle" data-qpx-nav-toggle '
    +               'aria-controls="qpx-nav" aria-expanded="false" '
    +               'aria-label="' + esc(content.pick(ui.navToggle, lang)) + '">☰</button>'
    +       '<div class="qpx-header__right">'
    +         '<a class="qpx-social" href="' + esc(content.links.telegram) + '" target="_blank" rel="noopener noreferrer" '
    +              'aria-label="' + esc(content.pick(ui.telegramCta, lang)) + '">'
    +           '<span class="qpx-social__icon" aria-hidden="true">'
    +             '<svg viewBox="0 0 24 24" focusable="false"><path d="M21.4 4.2 3.7 11.1c-1.2.5-1.2 1.2-.2 1.5l4.5 1.4 1.8 5.6c.2.6.1.8.8.8.5 0 .8-.2 1.1-.5l2.5-2.4 5.1 3.8c.9.5 1.5.2 1.7-.8l3-14.1c.3-1.2-.4-1.8-1.4-1.2ZM9 13.5l9.6-6.1c.5-.3.9-.1.5.2l-7.9 7.2-.3 3.2Z"/></svg>'
    +           '</span>'
    +           '<span class="qpx-social__label">' + esc(content.pick(ui.telegramCta, lang)) + '</span>'
    +         '</a>'
    +         themeSelect
    +         '<div class="qpx-lang" role="group" aria-label="'
    +              esc(content.pick(ui.langLabel, lang)) + '">'
    +           '<button class="qpx-lang__btn" data-qpx-lang="ru" '
    +                   'aria-pressed="' + (lang === 'ru' ? 'true' : 'false') + '">RU</button>'
    +           '<button class="qpx-lang__btn" data-qpx-lang="en" '
    +                   'aria-pressed="' + (lang === 'en' ? 'true' : 'false') + '">EN</button>'
    +         '</div>'
    +         '<a class="qpx-docs-cta" href="#' + esc(content.docsSection.id) + '">'
    +           esc(content.pick(ui.docsCta, lang))
    +         '</a>'
    +         (showBeta
                ? '<a class="qpx-docs-cta" href="#beta">'
                  + esc(content.pick(ui.betaCta, lang))
                  + '</a>'
                : '')
    +         '<button type="button" class="qpx-docs-cta" data-qpx-release-open '
    +                 'data-qpx-release-title="' + esc(content.pick(release.sourcesPopupTitle, lang)) + '" '
    +                 'data-qpx-release-message="' + esc(content.pick(release.sourcesPopupMessage, lang)) + '">'
    +           esc(content.pick(ui.sourcesCta, lang))
    +         '</button>'
    +         '<a class="qpx-docs-cta qpx-docs-cta--release" href="' + esc(downloadHref) + '" download>'
    +           esc(content.pick(ui.releaseCta, lang))
    +         '</a>'
    +       '</div>'
    +     '</div>'
    +     '<nav class="qpx-nav" id="qpx-nav" data-qpx-nav '
    +          'aria-label="' + esc(content.pick(ui.navToggle, lang)) + '">'
    +       navItems
    +       navActions
    +     '</nav>'
    +     '</div>'
    +   '</div>'
    + '</header>';
}

function renderHero(lang) {
  var hero = content.hero;
  var branding = content.branding || {};
  var release = content.release;
  var downloadHref = release.downloadHref || '#';
  var birthday = hero.birthday
    ? '<div class="qpx-hero__birth">'
      + '<span class="qpx-hero__birth-icon" aria-hidden="true">' + esc(hero.birthday.icon || '✦') + '</span>'
      + '<span class="qpx-hero__birth-label">' + esc(content.pick(hero.birthday.label, lang)) + '</span>'
      + '<span class="qpx-hero__birth-value">' + esc(content.pick(hero.birthday.value, lang)) + '</span>'
      + '</div>'
    : '';
  var pillars = content.pick(hero.pillars, lang)
    .map(function (p) {
      return '<div class="qpx-pillar">'
           +   '<p class="qpx-pillar__title">' + esc(p.title) + '</p>'
           +   '<p class="qpx-pillar__desc">'  + esc(p.desc)  + '</p>'
           + '</div>';
    }).join('');

  return ''
    + '<section class="qpx-hero" id="overview">'
    +   '<div class="qpx-wrap qpx-hero__inner">'
    +     (branding.logoMain
            ? '<div class="qpx-hero__logo-wrap">'
            +   '<img class="qpx-hero__logo qpx-hero__logo--dark" src="' + esc(branding.logoMain) + '" alt="'
            +     esc(content.pick(branding.logoAlt || {}, lang) || 'Q2PRO-X') + '">'
            +   (branding.logoLight
                  ? '<img class="qpx-hero__logo qpx-hero__logo--light" src="' + esc(branding.logoLight) + '" alt="'
                  +     esc(content.pick(branding.logoAlt || {}, lang) || 'Q2PRO-X') + '">'
                  : '')
            + '</div>'
            : '')
    +     '<div class="qpx-hero__meta-row">'
    +       '<p class="qpx-hero__eyebrow">' + esc(content.pick(hero.eyebrow, lang)) + '</p>'
    +       birthday
    +     '</div>'
    +     '<h1 class="qpx-hero__title">'  + esc(content.pick(hero.title, lang))   + '</h1>'
    +     '<p class="qpx-hero__subtitle">' + esc(content.pick(hero.subtitle, lang)) + '</p>'
    +     '<p class="qpx-hero__release-note">' + esc(content.pick(hero.releaseNote, lang)) + '</p>'
    +     '<div class="qpx-hero__actions">'
    +       '<a class="qpx-docs-cta" href="#' + esc(content.docsSection.id) + '">'
    +         esc(content.pick(content.ui.docsCta, lang))
    +       '</a>'
    +       '<button type="button" class="qpx-docs-cta" data-qpx-release-open '
    +               'data-qpx-release-title="' + esc(content.pick(release.sourcesPopupTitle, lang)) + '" '
    +               'data-qpx-release-message="' + esc(content.pick(release.sourcesPopupMessage, lang)) + '">'
    +         esc(content.pick(content.ui.sourcesCta, lang))
    +       '</button>'
    +       '<a class="qpx-docs-cta qpx-docs-cta--release" href="' + esc(downloadHref) + '" download>'
    +         esc(content.pick(content.ui.releaseCta, lang))
    +       '</a>'
    +     '</div>'
    +     '<div class="qpx-hero__pillars">' + pillars + '</div>'
    +   '</div>'
    + '</section>';
}

function renderShotTile(section, img, lang) {
  var src = content.mediaPath(section, img.file);
  var alt = content.pick(img.alt, lang);
  var cap = content.pick(img.caption, lang);
  // If the file doesn't exist yet, the <img> simply fails to load and
  // the `.qpx-shot__frame` diagonal-stripe background stays visible —
  // that's the intentional "placeholder" look. No inline error handler
  // needed; the lightbox shows a dedicated "image not added yet" card
  // via client.js on open.
  return '<button type="button" class="qpx-shot" data-qpx-shot '
       +         'data-src="'     + esc(src) + '" '
       +         'data-alt="'     + esc(alt) + '" '
       +         'data-caption="' + esc(cap) + '" '
       +         'aria-label="'   + esc(alt || cap || img.file) + '">'
       +   '<div class="qpx-shot__frame">'
       +     '<img loading="lazy" src="' + esc(src) + '" alt="' + esc(alt) + '">'
       +   '</div>'
       +   (cap
           ? '<div class="qpx-shot__caption">' + esc(cap) + '</div>'
           : '')
       + '</button>';
}

function renderSection(section, lang) {
  var bodyParas = (content.pick(section.body, lang) || [])
    .map(function (p) { return '<p>' + renderInline(p) + '</p>'; })
    .join('');

  var bullets = content.pick(section.bullets, lang) || [];
  var bulletsHtml = bullets.length
    ? '<ul class="qpx-bullets">'
      + bullets.map(function (b) { return '<li>' + renderInline(b) + '</li>'; }).join('')
      + '</ul>'
    : '';

  var callout = section.callout
    ? '<aside class="qpx-callout">' + renderInline(content.pick(section.callout, lang)) + '</aside>'
    : '';

  var sectionImages = section.images || [];
  var images = sectionImages.map(function (img) {
    return renderShotTile(section, img, lang);
  }).join('');
  var imageClass = 'qpx-images' + (sectionImages.length === 1 ? ' qpx-images--single' : '');
  var imagesHtml = images
    ? '<div class="' + imageClass + '">' + images + '</div>'
    : '';

  return ''
    + '<section class="qpx-section" id="' + esc(section.id) + '">'
    +   '<div class="qpx-wrap">'
    +     '<div class="qpx-section__head">'
    +       '<span class="qpx-section__id">' + esc(section.icon) + ' · '
    +                                          esc(section.id.toUpperCase()) + '</span>'
    +       '<h2 class="qpx-section__title">' + esc(content.pick(section.title, lang)) + '</h2>'
    +       '<p class="qpx-section__summary">' + esc(content.pick(section.summary, lang)) + '</p>'
    +     '</div>'
    +     '<div class="qpx-section__body">' + bodyParas + '</div>'
    +     bulletsHtml
    +     callout
    +     imagesHtml
    +   '</div>'
    + '</section>';
}

function renderDocs(lang) {
  var ds = content.docsSection;
  var cards = ds.items.map(function (item) {
    var links = (content.docs[item.key] && (content.docs[item.key][lang] || content.docs[item.key][content.defaultLang])) || {};
    var previewHref = links.preview || links.download || '#';
    var downloadHref = links.download || previewHref;
    return '<a class="qpx-doc-card" href="' + esc(previewHref) + '" '
         +   'data-qpx-doc-card '
         +   'data-preview="' + esc(previewHref) + '" '
         +   'data-download="' + esc(downloadHref) + '" '
         +   'data-title="' + esc(content.pick(item.title, lang)) + '">'
         +   '<h3 class="qpx-doc-card__title">' + esc(content.pick(item.title, lang)) + '</h3>'
         +   '<p class="qpx-doc-card__desc">'   + esc(content.pick(item.desc,  lang)) + '</p>'
         +   '<span class="qpx-doc-card__open">' + esc(content.pick(content.ui.previewCta, lang)) + '</span>'
         + '</a>';
  }).join('');

  return ''
    + '<section class="qpx-section" id="' + esc(ds.id) + '">'
    +   '<div class="qpx-wrap">'
    +     '<div class="qpx-section__head">'
    +       '<span class="qpx-section__id">§ · DOCS</span>'
    +       '<h2 class="qpx-section__title">' + esc(content.pick(ds.title, lang)) + '</h2>'
    +       '<p class="qpx-section__summary">' + esc(content.pick(ds.summary, lang)) + '</p>'
    +     '</div>'
    +     '<div class="qpx-docs">' + cards + '</div>'
    +   '</div>'
    + '</section>';
}

function renderReleaseSection(lang) {
  var r = content.release;
  var quickStart = content.pick(r.quickStartSteps, lang) || [];
  var body = (content.pick(r.sectionBody, lang) || [])
    .map(function (p) { return '<p>' + renderInline(p) + '</p>'; })
    .join('');
  var quickStartHtml = quickStart.length
    ? '<div class="qpx-release__quickstart-wrap">'
      + '<div class="qpx-release__quickstart-head">'
      +   '<h3 class="qpx-release__quickstart-title">' + esc(content.pick(r.quickStartTitle, lang)) + '</h3>'
      +   '<p class="qpx-release__quickstart-summary">' + esc(content.pick(r.quickStartSummary, lang)) + '</p>'
      + '</div>'
      + '<div class="qpx-release__quickstart-grid">'
      +   quickStart.map(function (step) {
            return '<article class="qpx-release__quickstart-card">'
                 +   '<h4 class="qpx-release__quickstart-card-title">' + esc(step.title) + '</h4>'
                 +   '<p class="qpx-release__quickstart-card-text">' + renderInline(step.text) + '</p>'
                 + '</article>';
          }).join('')
      + '</div>'
      + '</div>'
    : '';

  return ''
    + '<section class="qpx-section qpx-release" id="download">'
    +   '<div class="qpx-wrap">'
    +     '<div class="qpx-section__head">'
    +       '<span class="qpx-section__id">↓ · RELEASE</span>'
    +       '<h2 class="qpx-section__title">' + esc(content.pick(r.sectionTitle, lang)) + '</h2>'
    +       '<p class="qpx-section__summary">' + esc(content.pick(r.sectionSummary, lang)) + '</p>'
    +     '</div>'
    +     '<div class="qpx-section__body">' + body + '</div>'
    +     '<div class="qpx-release__actions">'
    +       '<a class="qpx-docs-cta qpx-docs-cta--release qpx-release__button" href="' + esc(r.downloadHref || '#') + '" download>'
    +         esc(content.pick(content.ui.releaseCta, lang))
    +       '</a>'
    +       '<button type="button" class="qpx-docs-cta qpx-release__button" data-qpx-release-open '
    +               'data-qpx-release-title="' + esc(content.pick(r.sourcesPopupTitle, lang)) + '" '
    +               'data-qpx-release-message="' + esc(content.pick(r.sourcesPopupMessage, lang)) + '">'
    +         esc(content.pick(content.ui.sourcesCta, lang))
    +       '</button>'
    +     '</div>'
    +     quickStartHtml
    +   '</div>'
    + '</section>';
}

function renderBetaSection(lang) {
  var b = content.beta;
  var body = (content.pick(b.sectionBody, lang) || [])
    .map(function (p) { return '<p>' + renderInline(p) + '</p>'; })
    .join('');
  var currentBody = (content.pick(b.currentDescriptionBody, lang) || [])
    .map(function (p) { return '<p>' + renderInline(p) + '</p>'; })
    .join('');
  var notes = content.pick(b.notes, lang) || [];
  var notesHtml = notes.length
    ? '<ul class="qpx-bullets qpx-beta__notes">'
      + notes.map(function (item) { return '<li>' + renderInline(item) + '</li>'; }).join('')
      + '</ul>'
    : '';
  var actionHtml = b.available
    ? '<a class="qpx-docs-cta qpx-beta__button" href="' + esc(b.downloadHref || '#') + '" download>'
      + esc(content.pick(content.ui.betaCta, lang))
      + '</a>'
    : '<button type="button" class="qpx-docs-cta qpx-beta__button" data-qpx-release-open '
      +         'data-qpx-release-title="' + esc(content.pick(b.popupTitle, lang)) + '" '
      +         'data-qpx-release-message="' + esc(content.pick(b.popupMessage, lang)) + '">'
      +   esc(content.pick(content.ui.betaCta, lang))
      + '</button>';
  var betaSection = { slug: b.slug || 'beta' };
  var images = (b.images || []).map(function (img) {
    return renderShotTile(betaSection, img, lang);
  }).join('');
  var imagesHtml = images
    ? '<div class="qpx-images qpx-beta__images">' + images + '</div>'
    : '';

  return ''
    + '<section class="qpx-section qpx-beta" id="beta">'
    +   '<div class="qpx-wrap">'
    +     '<div class="qpx-section__head">'
    +       '<span class="qpx-section__id">β · BETA</span>'
    +       '<h2 class="qpx-section__title">' + esc(content.pick(b.sectionTitle, lang)) + '</h2>'
    +       '<p class="qpx-section__summary">' + esc(content.pick(b.sectionSummary, lang)) + '</p>'
    +     '</div>'
    +     '<div class="qpx-section__body">' + body + '</div>'
    +     imagesHtml
    +     '<div class="qpx-beta__grid">'
    +       '<article class="qpx-beta__card qpx-beta__card--current">'
    +         '<div class="qpx-beta__card-head">'
    +           '<p class="qpx-beta__eyebrow">Q2PRO-X BETA</p>'
    +           '<h3 class="qpx-beta__card-title">' + esc(content.pick(b.currentTitle, lang)) + '</h3>'
    +         '</div>'
    +         '<dl class="qpx-beta__meta">'
    +           '<div class="qpx-beta__meta-row">'
    +             '<dt>' + esc(content.pick(b.currentStatusLabel, lang)) + '</dt>'
    +             '<dd>' + esc(content.pick(b.currentStatusValue, lang)) + '</dd>'
    +           '</div>'
    +           '<div class="qpx-beta__meta-row">'
    +             '<dt>' + esc(content.pick(b.currentVersionLabel, lang)) + '</dt>'
    +             '<dd>' + esc(content.pick(b.currentVersionValue, lang)) + '</dd>'
    +           '</div>'
    +         '</dl>'
    +         '<div class="qpx-beta__body">'
    +           '<h4 class="qpx-beta__body-title">' + esc(content.pick(b.currentDescriptionTitle, lang)) + '</h4>'
    +           currentBody
    +         '</div>'
    +         '<div class="qpx-beta__actions">' + actionHtml + '</div>'
    +       '</article>'
    +       '<aside class="qpx-beta__card qpx-beta__card--notes">'
    +         '<h3 class="qpx-beta__card-title">' + esc(content.pick(b.notesTitle, lang)) + '</h3>'
    +         notesHtml
    +       '</aside>'
    +     '</div>'
    +   '</div>'
    + '</section>';
}

function renderDocsModal(lang) {
  var ui = content.ui;
  var ds = content.docsSection;
  return ''
    + '<div class="qpx-modal qpx-doc-modal" data-qpx-doc-modal role="dialog" aria-modal="true" '
    +      'aria-label="' + esc(content.pick(ds.title, lang)) + '">'
    +   '<div class="qpx-modal__inner qpx-doc-modal__inner">'
    +     '<div class="qpx-doc-modal__bar">'
    +       '<div class="qpx-doc-modal__meta">'
    +         '<p class="qpx-doc-modal__eyebrow">Q2PRO-X</p>'
    +         '<h3 class="qpx-doc-modal__title" data-qpx-doc-title></h3>'
    +       '</div>'
    +       '<div class="qpx-doc-modal__actions">'
    +         '<button type="button" class="qpx-modal__utility" data-qpx-doc-fullscreen '
    +                 'aria-label="' + esc(content.pick(ui.modalFullscreen, lang)) + '">⛶</button>'
    +         '<a class="qpx-doc-modal__download" data-qpx-doc-download href="#" download>'
    +           esc(content.pick(ui.downloadCta, lang))
    +         '</a>'
    +         '<button type="button" class="qpx-modal__close qpx-doc-modal__close" data-qpx-doc-close '
    +                 'aria-label="' + esc(content.pick(ui.modalClose, lang)) + '">×</button>'
    +       '</div>'
    +     '</div>'
    +     '<div class="qpx-doc-modal__frame">'
    +       '<iframe class="qpx-doc-modal__iframe" data-qpx-doc-iframe '
    +               'title="' + esc(content.pick(ds.title, lang)) + '" '
    +               'loading="lazy" referrerpolicy="no-referrer"></iframe>'
    +     '</div>'
    +   '</div>'
    + '</div>';
}

function renderFooter(lang) {
  var ui = content.ui;
  var authorLabel = content.pick(ui.author, lang);
  var qjLabel = 'QuakeJourney';
  return ''
    + '<footer class="qpx-footer">'
    +   '<div class="qpx-wrap">'
    +     '<p>' + esc(content.pick(ui.copyright, lang)) + '</p>'
    +     '<p>' + esc(authorLabel) + ' / '
    +       '<a href="' + esc(content.links.quakeJourney) + '" target="_blank" rel="noopener noreferrer">'
    +         esc(qjLabel)
    +       '</a>'
    +     '</p>'
    +     '<p>' + esc(content.pick(ui.builtWith, lang)) + '</p>'
    +   '</div>'
    + '</footer>';
}

function renderModal(lang) {
  var ui = content.ui;
  var placeholder = lang === 'ru' ? 'изображение ещё не добавлено' : 'image not added yet';
  return ''
    + '<div class="qpx-modal" data-qpx-modal role="dialog" aria-modal="true" '
    +      'aria-label="Screenshot" data-placeholder-text="' + esc(placeholder) + '">'
    +   '<div class="qpx-modal__inner">'
    +     '<div class="qpx-modal__toolbar">'
    +       '<button type="button" class="qpx-modal__utility" data-qpx-modal-fullscreen '
    +               'aria-label="' + esc(content.pick(ui.modalFullscreen, lang)) + '">⛶</button>'
    +       '<button type="button" class="qpx-modal__close" data-qpx-modal-close '
    +               'aria-label="' + esc(content.pick(ui.modalClose, lang)) + '">×</button>'
    +     '</div>'
    +     '<div class="qpx-modal__frame" data-qpx-modal-frame>'
    +       '<button type="button" class="qpx-modal__nav qpx-modal__nav--prev" data-qpx-modal-prev '
    +               'aria-label="' + esc(content.pick(ui.modalPrev, lang)) + '">‹</button>'
    +       '<img class="qpx-modal__img" data-qpx-modal-img alt="">'
    +       '<button type="button" class="qpx-modal__nav qpx-modal__nav--next" data-qpx-modal-next '
    +               'aria-label="' + esc(content.pick(ui.modalNext, lang)) + '">›</button>'
    +     '</div>'
    +     '<p class="qpx-modal__caption" data-qpx-modal-caption></p>'
    +   '</div>'
    + '</div>';
}

function renderReleaseModal(lang) {
  var ui = content.ui;
  var r = content.release;
  return ''
    + '<div class="qpx-modal qpx-note-modal" data-qpx-release-modal role="dialog" aria-modal="true" '
    +      'aria-label="' + esc(content.pick(r.popupTitle, lang)) + '">'
    +   '<div class="qpx-modal__inner qpx-note-modal__inner">'
    +     '<button type="button" class="qpx-modal__close qpx-note-modal__close" data-qpx-release-close '
    +             'aria-label="' + esc(content.pick(ui.modalClose, lang)) + '">×</button>'
    +     '<div class="qpx-note-modal__body">'
    +       '<p class="qpx-note-modal__eyebrow">Q2PRO-X</p>'
    +       '<h3 class="qpx-note-modal__title" data-qpx-release-title>' + esc(content.pick(r.popupTitle, lang)) + '</h3>'
    +       '<p class="qpx-note-modal__text" data-qpx-release-text>' + esc(content.pick(r.popupMessage, lang)) + '</p>'
    +     '</div>'
    +   '</div>'
    + '</div>';
}

/* ── main entry ── */

function renderPage(opts) {
  var lang  = content.resolveLang(opts && opts.lang);
  var themeMode = content.resolveTheme(opts && opts.theme);
  var title = content.pick(content.meta.title, lang);
  var desc  = content.pick(content.meta.description, lang);
  var ogImg = content.meta.ogImage || '';
  var favicon = (content.branding && (content.branding.favicon || content.branding.logoMain)) || ogImg || '';
  var initialTheme = themeMode === 'dark' ? 'dark' : themeMode === 'light' ? 'light' : 'dark';

  var sectionsHtml = content.sections
    .filter(function (s) { return s.id !== 'overview'; }) // overview is the hero
    .map(function (s) { return renderSection(s, lang); })
    .join('');

  return ''
    + '<!DOCTYPE html>'
    + '<html lang="' + esc(lang) + '" data-qpx-theme-mode="' + esc(themeMode) + '" data-qpx-theme="' + esc(initialTheme) + '">'
    + '<head>'
    +   '<meta charset="utf-8">'
    +   '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
    +   '<title>' + esc(title) + '</title>'
    +   '<meta name="description" content="' + esc(desc) + '">'
    +   '<meta property="og:title" content="' + esc(title) + '">'
    +   '<meta property="og:description" content="' + esc(desc) + '">'
    +   (ogImg ? '<meta property="og:image" content="' + esc(ogImg) + '">' : '')
    +   '<meta property="og:type" content="website">'
    +   '<meta name="twitter:card" content="summary_large_image">'
    +   '<meta name="color-scheme" content="dark light">'
    +   '<meta name="theme-color" content="' + (initialTheme === 'light' ? '#f4f7fc' : '#0a0d15') + '">'
    +   (favicon ? '<link rel="icon" href="' + esc(favicon) + '">' : '<link rel="icon" href="data:,">')
    +   (favicon ? '<link rel="apple-touch-icon" href="' + esc(favicon) + '">' : '')
    /* Language-restore bootstrap. Runs before the body paints so a
     * returning user doesn't see a flash of the wrong language when
     * their saved preference differs from the server's default.
     *
     * Logic:
     *   1. If URL has ?lang=…, save it to localStorage and stop.
     *   2. Else if localStorage has a saved lang that matches the
     *      whitelist and differs from what the server just rendered,
     *      redirect to ?lang=<saved> via location.replace (no history
     *      entry, no loop — subsequent visits see ?lang= in the URL
     *      and hit branch 1).
     *   3. Else do nothing — server's resolution stands.
     *
     * Wrapped in try/catch so private-mode / sandboxed localStorage
     * never breaks the page. */
    +   '<script>(function(){try{'
    +     'var u=new URL(window.location.href);'
    +     'if(u.searchParams.has("lang")){'
    +       'var q=u.searchParams.get("lang");'
    +       'if(/^(ru|en)$/.test(q))localStorage.setItem("qpx_lang",q);'
    +       'return;'
    +     '}'
    +     'var saved=localStorage.getItem("qpx_lang");'
    +     'if(!saved||!/^(ru|en)$/.test(saved))return;'
    +     'var cur=document.documentElement.getAttribute("lang")||"";'
    +     'if(saved===cur)return;'
    +     'u.searchParams.set("lang",saved);'
    +     'window.location.replace(u.toString());'
    +   '}catch(e){}})();</script>'
    +   '<script>(function(){try{'
    +     'var root=document.documentElement;'
    +     'var mode=root.getAttribute("data-qpx-theme-mode")||"auto";'
    +     'function resolveMode(v){return(v==="dark"||v==="light"||v==="auto")?v:"auto";}'
    +     'function resolved(v){'
    +       'if(v==="dark"||v==="light")return v;'
    +       'var mq=window.matchMedia&&window.matchMedia("(prefers-color-scheme: dark)");'
    +       'return mq&&mq.matches?"dark":"light";'
    +     '}'
    +     'function apply(v){'
    +       'v=resolveMode(v);'
    +       'root.setAttribute("data-qpx-theme-mode",v);'
    +       'var actual=resolved(v);'
    +       'root.setAttribute("data-qpx-theme",actual);'
    +       'var meta=document.querySelector(\'meta[name="theme-color"]\');'
    +       'if(meta)meta.setAttribute("content",actual==="light"?"#f4f7fc":"#0a0d15");'
    +     '}'
    +     'apply(mode);'
    +     'if(window.matchMedia){'
    +       'var mq=window.matchMedia("(prefers-color-scheme: dark)");'
    +       'var onChange=function(){if((root.getAttribute("data-qpx-theme-mode")||"auto")==="auto")apply("auto");};'
    +       'if(mq.addEventListener)mq.addEventListener("change",onChange);else if(mq.addListener)mq.addListener(onChange);'
    +     '}'
    +   '}catch(e){}})();</script>'
    +   '<style>' + CSS + '</style>'
    + '</head>'
    + '<body>'
    +   renderHeader(lang, themeMode)
    +   '<main id="qpx-main">'
    +     renderHero(lang)
    +     sectionsHtml
    +     renderReleaseSection(lang)
    +     (content.beta && content.beta.visible ? renderBetaSection(lang) : '')
    +     renderDocs(lang)
    +   '</main>'
    +   renderFooter(lang)
    +   renderModal(lang)
    +   renderDocsModal(lang)
    +   renderReleaseModal(lang)
    +   '<script>' + JS + '</script>'
    + '</body>'
    + '</html>';
}

module.exports = { renderPage };
