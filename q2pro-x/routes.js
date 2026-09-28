// q2pro-x/routes.js
//
// Public entry point for the Q2PRO-X module. Mounted from site.js via:
//
//     const { attachQ2proxRoutes } = require('./q2pro-x/routes');
//     attachQ2proxRoutes(app);
//
// Mirrors the style of servers.js — one exported attacher that adds all
// the routes this module owns and nothing else. No database, no sockets,
// no IO except serving static files from ./media.
//
// Routes added:
//   GET  /q2pro-x              main page (SSR, bilingual, anchored)
//   GET  /q2pro-x/media/*      in-tree media (screenshots)
//   GET  /q2pro-x/docs/ru/*    Russian documentation (user, technical, changelog)
//   GET  /q2pro-x/docs/en/*    English documentation
//   GET  /q2pro-x/docs-preview/* generated HTML previews for modal viewing
//   GET  /q2pro-x/branding/*   branded assets (logo, og-image)
//   GET  /q2pro-x/downloads/*  release archives
//
// Language resolution order (server-side):
//   ?lang=xx query parameter   (explicit click from the language switcher)
//   Accept-Language header     (first-time visitors, whitelist-matched)
//   default 'ru'               (fallback)
//
// Client-side language restoration (`client.js` + inline head bootstrap
// in `render.js`) layers on top of this: if no ?lang= is present and
// localStorage has a previously-chosen language that differs from what
// the server just rendered, the client re-navigates to ?lang=<saved>.
// Explicit ?lang= always wins.

const path    = require('path');
const express = require('express');

const { renderPage } = require('./render');
const content        = require('./content');

const THEME_COOKIE = 'qpx_theme';
const THEME_PARAM  = 'theme';

function readCookie(req, name) {
  var header = String((req && req.headers && req.headers.cookie) || '');
  if (!header) return '';
  var parts = header.split(/;\s*/);
  for (var i = 0; i < parts.length; i++) {
    var p = parts[i];
    var eq = p.indexOf('=');
    if (eq <= 0) continue;
    var key = p.slice(0, eq).trim();
    if (key !== name) continue;
    try {
      return decodeURIComponent(p.slice(eq + 1));
    } catch (e) {
      return p.slice(eq + 1);
    }
  }
  return '';
}

function pickLangFromRequest(req) {
  // 1. explicit ?lang=xx
  var q = req.query && req.query.lang;
  if (q) return content.resolveLang(q);

  // 2. Accept-Language header — naive match, just looks for 'ru' prefix
  var al = String(req.headers['accept-language'] || '').toLowerCase();
  if (/\bru\b/.test(al) || al.startsWith('ru')) return 'ru';
  if (/\ben\b/.test(al) || al.startsWith('en')) return 'en';

  // 3. fallback
  return content.defaultLang;
}

function pickThemeFromRequest(req) {
  var q = req.query && req.query[THEME_PARAM];
  if (q) return content.resolveTheme(q);

  var cookieTheme = readCookie(req, THEME_COOKIE);
  if (cookieTheme) return content.resolveTheme(cookieTheme);

  return content.defaultTheme;
}

function attachQ2proxRoutes(app) {
  console.log('[q2pro-x] attachQ2proxRoutes called');

  // ── Static media (in-tree, maintained by site module) ───────────────
  // Served from O:\Claude\q2pro-x\media\<section>\<file>. Safe to leave
  // empty — the page shows placeholder tiles when a file 404s and picks
  // up real screenshots as they're added.
  var mediaDir = path.resolve(__dirname, 'media');
  app.use('/q2pro-x/media', express.static(mediaDir, {
    fallthrough: true,
    maxAge: '1h',
    setHeaders: function (res) {
      res.setHeader('Cache-Control', 'public, max-age=3600');
    },
  }));

  // ── Documentation (real files under public/q2pro-x/docs/{ru,en}/) ───
  // The follow-up pass swapped `#docs` placeholders in content.js for
  // real URLs under this mount. One static mount covers both languages
  // because the language lives in the folder name, not the filename.
  var docsDir = path.resolve(__dirname, '..', 'public', 'q2pro-x', 'docs');
  app.use('/q2pro-x/docs', express.static(docsDir, {
    fallthrough: true,
    maxAge: '1d',
    setHeaders: function (res, filePath) {
      res.setHeader('Cache-Control', 'public, max-age=86400');
      // Encourage "download / open in Word" UX for .docx instead of
      // leaving the browser to guess (it usually gets it right, but
      // an explicit Content-Type is friendlier).
      if (/\.docx$/i.test(filePath)) {
        res.setHeader('Content-Type',
          'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
      }
    },
  }));

  // ── Generated HTML doc previews for modal viewing ───────────────────
  // Produced by O:\Claude2\q2pro\build_q2prox_website_docs.py from the
  // source .docx files, then loaded into an iframe modal on the page.
  var docsPreviewDir = path.resolve(__dirname, '..', 'public', 'q2pro-x', 'docs-preview');
  app.use('/q2pro-x/docs-preview', express.static(docsPreviewDir, {
    fallthrough: true,
    maxAge: '1d',
    setHeaders: function (res, filePath) {
      res.setHeader('Cache-Control', 'public, max-age=86400');
      if (/\.html$/i.test(filePath)) {
        res.setHeader('Content-Type', 'text/html; charset=utf-8');
      }
    },
  }));

  // ── Branding (logo, og-image, other static brand assets) ────────────
  // Served from O:\Claude\public\q2pro-x\branding\. The site's og:image
  // meta tag points here so social crawlers resolve a real asset.
  var brandingDir = path.resolve(__dirname, '..', 'public', 'q2pro-x', 'branding');
  app.use('/q2pro-x/branding', express.static(brandingDir, {
    fallthrough: true,
    maxAge: '1d',
    setHeaders: function (res) {
      res.setHeader('Cache-Control', 'public, max-age=86400');
    },
  }));

  // ── Release downloads ────────────────────────────────────────────────
  var downloadsDir = path.resolve(__dirname, '..', 'public', 'q2pro-x', 'downloads');
  app.use('/q2pro-x/downloads', express.static(downloadsDir, {
    fallthrough: true,
    maxAge: '1d',
    setHeaders: function (res) {
      res.setHeader('Cache-Control', 'public, max-age=86400');
    },
  }));

  // Main page.
  app.get('/q2pro-x', function (req, res) {
    try {
      var lang = pickLangFromRequest(req);
      var theme = pickThemeFromRequest(req);
      if (req.query && Object.prototype.hasOwnProperty.call(req.query, THEME_PARAM)) {
        res.append('Set-Cookie',
          THEME_COOKIE + '=' + encodeURIComponent(theme) +
          '; Max-Age=31536000; Path=/; SameSite=Lax');
      }
      var html = renderPage({ lang: lang, theme: theme });
      res.set('Cache-Control', 'no-store');
      res.type('text/html; charset=utf-8').send(html);
    } catch (err) {
      console.error('[q2pro-x] render error:', err);
      res.status(500).type('text/plain').send('q2pro-x render error: ' + err.message);
    }
  });

  // Convenience redirect: /q2pro-x/ (trailing slash) → /q2pro-x
  app.get('/q2pro-x/', function (req, res) {
    var qs = req.url.indexOf('?') >= 0 ? req.url.slice(req.url.indexOf('?')) : '';
    res.redirect(301, '/q2pro-x' + qs);
  });
}

module.exports = { attachQ2proxRoutes };
