'use strict';
const path = require('path');
const express = require('express');
const site = require('./content');
const { CSS } = require('./styles');
const { CSS: pageCSS } = require('./mapgen-styles');
const { JS } = require('./client');
const { renderPreferenceBootstrap, renderModal, esc } = require('./render');
const { copy, links } = require('./mapgen-content');

const assetRoot = '/mapgen/assets';
function external(url, text, className) {
  return '<a href="' + esc(url) + '"' + (className ? ' class="' + esc(className) + '"' : '')
    + ' target="_blank" rel="noopener noreferrer">' + esc(text) + '</a>';
}
function screenshot(file, caption, lang, eager) {
  const src = assetRoot + '/screens/' + lang + '/' + file + '.png';
  return '<figure class="mg-screen"><button type="button" class="qpx-shot" data-qpx-shot'
    + ' data-src="' + src + '" data-alt="' + esc(caption) + '" data-caption="' + esc(caption) + '"'
    + ' aria-label="' + esc(caption) + '"><div class="qpx-shot__frame">'
    + '<img src="' + src + '" alt="' + esc(caption) + '" width="1600" height="900"'
    + (eager ? ' fetchpriority="high"' : ' loading="lazy"') + '></div>'
    + '<div class="qpx-shot__caption">' + esc(caption) + '</div></button></figure>';
}
function header(lang, theme, c) {
  const ui = site.ui;
  const pick = v => site.pick(v, lang);
  const select = ['auto', 'dark', 'light'].map(value => '<option value="' + value + '"'
    + (value === theme ? ' selected' : '') + '>'
    + esc(pick(ui[{auto: 'themeAuto', dark: 'themeDark', light: 'themeLight'}[value]])) + '</option>').join('');
  const nav = [['features',c.overview],['workflow',c.steps],['requirements',c.requirements],['community',c.community]];
  return '<a class="qpx-skip" href="#qpx-main">' + esc(pick(ui.skip)) + '</a>'
    + '<header class="qpx-header mg-header"><div class="qpx-wrap qpx-header__inner"><div class="qpx-header__top">'
    + '<a class="qpx-brand" href="/mapgen?lang=' + lang + '&amp;theme=' + theme + '" aria-label="MAPGEN Studio">'
    + '<img class="mg-wordmark mg-wordmark--dark" src="' + assetRoot + '/mapgen-logo-dark.svg" alt="MAPGEN Studio" width="246" height="54">'
    + '<img class="mg-wordmark mg-wordmark--light" src="' + assetRoot + '/mapgen-logo-light.svg" alt="MAPGEN Studio" width="246" height="54"></a>'
    + '<button class="qpx-nav-toggle" data-qpx-nav-toggle aria-controls="qpx-nav" aria-expanded="false" aria-label="'
    + esc(pick(ui.navToggle)) + '">☰</button><div class="qpx-header__right">'
    + '<a class="qpx-docs-cta" href="/q2pro-x?lang=' + lang + '&amp;theme=' + theme + '">' + c.back + ' ↗</a>'
    + '<label class="qpx-theme"><span class="qpx-theme__label">' + esc(pick(ui.themeLabel)) + '</span>'
    + '<select class="qpx-theme__select" data-qpx-theme-select aria-label="' + esc(pick(ui.themeLabel)) + '">' + select + '</select></label>'
    + '<div class="qpx-lang" role="group" aria-label="' + esc(pick(ui.langLabel)) + '">'
    + ['ru','en'].map(value => '<button class="qpx-lang__btn" data-qpx-lang="' + value + '" aria-pressed="'
      + (lang === value ? 'true' : 'false') + '">' + value.toUpperCase() + '</button>').join('')
    + '</div></div></div><nav class="qpx-nav" id="qpx-nav" data-qpx-nav aria-label="' + esc(pick(ui.navToggle)) + '">'
    + nav.map(([id,label]) => '<a class="qpx-nav__item" href="#' + id + '">' + esc(label) + '</a>').join('')
    + '</nav></div></header>';
}
function renderMapgen(opts) {
  const lang = site.resolveLang(opts && opts.lang);
  const theme = site.resolveTheme(opts && opts.theme);
  const initial = theme === 'light' ? 'light' : 'dark';
  const other = lang === 'ru' ? 'en' : 'ru';
  const c = copy[lang];
  const download = external(links.download, c.download, 'mg-button mg-button--primary');
  const guide = external(links.guide[lang], c.guide, 'mg-button');
  return '<!doctype html><html lang="' + lang + '" data-qpx-theme-mode="' + theme + '" data-qpx-theme="' + initial + '"><head>'
    + '<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
    + '<title>' + esc(c.title) + '</title><meta name="description" content="' + esc(c.description) + '">'
    + '<link rel="canonical" href="https://q2pro-x.com/mapgen?lang=' + lang + '">'
    + '<link rel="alternate" hreflang="ru" href="https://q2pro-x.com/mapgen?lang=ru">'
    + '<link rel="alternate" hreflang="en" href="https://q2pro-x.com/mapgen?lang=en">'
    + '<meta property="og:title" content="' + esc(c.title) + '"><meta property="og:description" content="' + esc(c.description) + '">'
    + '<meta property="og:type" content="website"><meta property="og:url" content="https://q2pro-x.com/mapgen?lang=' + lang + '">'
    + '<meta property="og:image" content="https://q2pro-x.com' + assetRoot + '/screens/' + lang + '/run_full.png">'
    + '<meta name="twitter:card" content="summary_large_image"><meta name="color-scheme" content="dark light">'
    + '<meta name="theme-color" content="' + (initial === 'light' ? '#f4f7fc' : '#0a0d15') + '">'
    + '<link rel="icon" href="' + assetRoot + '/mapgen_studio.ico">'
    + renderPreferenceBootstrap() + '<style>' + CSS + pageCSS + '</style></head><body>' + header(lang,theme,c)
    + '<main id="qpx-main"><section class="mg-hero" id="overview"><div class="qpx-wrap mg-hero-grid"><div>'
    + '<p class="mg-kicker">' + esc(c.project) + '</p><h1>' + esc(c.heading) + '</h1><p class="mg-intro">' + esc(c.intro)
    + '</p><p class="mg-base">' + esc(c.base) + '</p><div class="mg-actions">' + download + guide
    + '</div><p class="mg-download-note">' + esc(c.downloadNote) + '</p></div><div>'
    + screenshot('generate',c.heroCaption,lang,true) + '<p class="mg-hint">' + esc(c.enlarge) + '</p></div></div></section>'
    + '<section class="mg-section" id="features"><div class="qpx-wrap"><h2>' + esc(c.featuresTitle) + '</h2>'
    + '<p class="mg-section-intro">' + esc(c.featuresIntro) + '</p><div class="mg-feature-grid"><dl class="mg-feature-list">'
    + c.features.map(([title,text]) => '<div><dt>' + esc(title) + '</dt><dd>' + esc(text) + '</dd></div>').join('')
    + '</dl>' + screenshot('run_full',c.planCaption,lang,false) + '</div></div></section>'
    + '<section class="mg-section" id="studio"><div class="qpx-wrap"><h2>' + esc(c.galleryTitle) + '</h2>'
    + '<p class="mg-section-intro">' + esc(c.galleryIntro) + '</p><div class="qpx-images mg-gallery">'
    + screenshot('generate_options',c.optionsCaption,lang,false) + screenshot('map',c.mapCaption,lang,false) + '</div></div></section>'
    + '<section class="mg-section" id="workflow"><div class="qpx-wrap"><h2>' + esc(c.stepsTitle) + '</h2><ol class="mg-steps">'
    + c.stepItems.map(([title,text]) => '<li><h3>' + esc(title) + '</h3><p>' + esc(text) + '</p></li>').join('') + '</ol></div></section>'
    + '<section class="mg-section" id="requirements"><div class="qpx-wrap"><h2>' + esc(c.requirementsTitle) + '</h2>'
    + '<p class="mg-section-intro">' + esc(c.requirementsIntro) + '</p><div class="mg-requirements"><ul>'
    + c.requirementItems.map(([title,text]) => '<li><strong>' + esc(title) + '</strong><p>' + esc(text) + '</p></li>').join('')
    + '</ul><aside class="mg-note"><p>' + esc(c.limitation) + '</p><p><small>' + esc(c.pythonNote) + '</small></p></aside></div></div></section>'
    + '<section class="mg-section" id="resources"><div class="qpx-wrap"><h2>' + esc(c.resourcesTitle) + '</h2>'
    + '<p class="mg-section-intro">' + esc(c.resourcesIntro) + '</p><div class="mg-actions">' + download + guide
    + external(links.changes[lang],c.changes,'mg-button') + external(links.sources,c.sources,'mg-button') + '</div>'
    + '<div class="mg-resource-links">' + external(links.guide[other],c.alternateGuide) + external(links.changes[other],c.alternateChanges)
    + '</div><div class="mg-community" id="community"><h2>' + esc(c.community) + '</h2><div class="mg-actions">'
    + external(links.news,c.news,'mg-button') + external(links.community,c.group,'mg-button') + '</div></div></div></section></main>'
    + '<footer class="mg-footer"><div class="qpx-wrap"><span>' + esc(c.footer) + '</span><span>'
    + esc(site.pick(site.ui.copyright,lang)) + '</span></div></footer>' + renderModal(lang) + '<script>' + JS + '</script></body></html>';
}
function attachMapgenRoutes(app, pickLang, pickTheme) {
  app.use(assetRoot, express.static(path.resolve(__dirname, '..', 'public', 'mapgen'), {maxAge:'1d'}));
  // An explicit array supports strict-routing installations as well as the
  // current Express default. The address embedded in Studio stays unchanged.
  app.get(['/mapgen', '/mapgen/'], function(req,res) {
    const theme = pickTheme(req);
    if (req.query && Object.prototype.hasOwnProperty.call(req.query, 'theme')) {
      res.append('Set-Cookie','qpx_theme=' + encodeURIComponent(theme) + '; Max-Age=31536000; Path=/; SameSite=Lax');
    }
    res.set('Cache-Control','no-store');
    res.type('html').send(renderMapgen({lang:pickLang(req),theme}));
  });
}
module.exports = { renderMapgen, attachMapgenRoutes };
