// q2pro-x/translate.js
//
// Q2PRO-X /en translation proxy. The Q2PRO-X game client (Phase 3
// of the RU console-input branch) sends a small JSON request here
// and expects a LibreTranslate-compatible response. The proxy holds
// the Yandex Cloud API key + folder ID server-side so the client
// player never has to obtain or configure any credentials —
// "player opens client, types /en привет, gets English back, no
// configuration" is the locked acceptance criterion.
//
// Mounted from site.js via:
//
//     const { attachQ2proxTranslateRoute } = require('./q2pro-x/translate');
//     attachQ2proxTranslateRoute(app);
//
// Public URL (per PO Q3 = A, 2026-04-27):
//
//     POST https://q2pro-x.com/api/translate
//
// Lives at the root path because the Q2PRO-X game client expects
// /api/translate, not /q2pro-x/api/translate.
//
// Backend: Yandex AI Studio Translate (per PO Q1 = Yandex, 2026-04-27).
// Endpoint: https://translate.api.cloud.yandex.net/translate/v2/translate
// Auth:     Authorization: Api-Key <env Q2PROX_YANDEX_API_KEY>
// Body:     { folderId, texts: [<q>], targetLanguageCode, [sourceLanguageCode] }
// Response: { translations: [ { text, detectedLanguageCode } ] }
//
// Required env vars (set on the production host, not committed):
//
//     Q2PROX_YANDEX_API_KEY      = <Yandex Api-Key>
//     Q2PROX_YANDEX_FOLDER_ID    = <Yandex folder id>
//
// Optional env vars (sane defaults applied):
//
//     Q2PROX_TRANSLATE_RPM       = 30        per-IP req/min
//     Q2PROX_TRANSLATE_TIMEOUT_MS= 3000      upstream timeout to Yandex
//     Q2PROX_TRANSLATE_MAX_QLEN  = 500       max input chars
//     Q2PROX_TRANSLATE_DEBUG     = 0|1       extra console line on errors
//
// Rate-limit storage: in-memory (express-rate-limit default). Per
// PO 2026-04-27 — single-process website backend, no Redis. Counters
// reset on website restart, which is acceptable for Q2PRO-X chat
// volume.
//
// Logging policy (PO Q4 = metrics-only, 2026-04-27): one JSON line
// per request to stdout. NO request body, NO response body. Fields:
// timestamp, hashed-IP, User-Agent, q-length, output-length, status
// label, HTTP code, latency. Truncated SHA-256 of req.ip for privacy.
//
// Security:
// - Yandex Api-Key + folder id NEVER returned to the client.
// - Yandex Api-Key NEVER logged in any code path.
// - Reject non-Q2PRO-X User-Agents with 403 (lazy-abuse blocker).
// - Reject non-application/json content type with 415.
// - Body size cap (4 KB).
// - Per-IP rate limit (in-memory).
// - Upstream timeout via AbortSignal.
// - Health endpoint does NOT call Yandex.

'use strict';

const express   = require('express');
const crypto    = require('crypto');
const rateLimit = require('express-rate-limit');

const YANDEX_URL =
  'https://translate.api.cloud.yandex.net/translate/v2/translate';

const RPM        = Number(process.env.Q2PROX_TRANSLATE_RPM || 30);
const TIMEOUT_MS = Number(process.env.Q2PROX_TRANSLATE_TIMEOUT_MS || 3000);
const MAX_QLEN   = Number(process.env.Q2PROX_TRANSLATE_MAX_QLEN || 500);
const DEBUG      = String(process.env.Q2PROX_TRANSLATE_DEBUG || '0') === '1';

function hashIp(ip) {
  return crypto.createHash('sha256').update(String(ip || '')).digest('hex').slice(0, 12);
}

function logMetric(req, res, t0, qlen, outlen, status, codeOverride) {
  // Q2PRO-X 2026-04-27: explicit code argument so metric line records
  // the FINAL response code rather than `res.statusCode` at logging
  // time. Earlier versions logged `code:200` when the actual response
  // was 502, because logMetric was called before `res.status(502)`.
  const line = JSON.stringify({
    ts:         new Date().toISOString(),
    ip_h:       hashIp(req.ip),
    ua:         String(req.get('User-Agent') || '').slice(0, 80),
    qlen:       qlen,
    outlen:     outlen,
    status:     status,
    code:       (typeof codeOverride === 'number') ? codeOverride : res.statusCode,
    latency_ms: Date.now() - t0,
  });
  process.stdout.write('[q2prox-translate] ' + line + '\n');
}

function attachQ2proxTranslateRoute(app) {
  const apiKey   = process.env.Q2PROX_YANDEX_API_KEY;
  const folderId = process.env.Q2PROX_YANDEX_FOLDER_ID;

  // Health probe — works even if Yandex creds are not yet set so a
  // monitor can verify "the website backend is up" independently of
  // upstream provider state.
  app.get('/api/translate/health', function (req, res) {
    res.json({ ok: true, configured: Boolean(apiKey && folderId) });
  });

  if (!apiKey || !folderId) {
    // Register a failing route so /api/translate doesn't 404
    // silently — gives a clear configuration message instead.
    app.post('/api/translate', function (_req, res) {
      res.status(503).json({ error: 'translate proxy not configured' });
    });
    process.stderr.write(
      '[q2prox-translate] Q2PROX_YANDEX_API_KEY / ' +
      'Q2PROX_YANDEX_FOLDER_ID missing — /api/translate disabled\n');
    return;
  }

  // Per-IP rate limit. In-memory store per PO Q5 = in-memory
  // (single-process website backend, no Redis).
  const limiter = rateLimit({
    windowMs:        60_000,
    max:             RPM,
    standardHeaders: true,
    legacyHeaders:   false,
  });

  // Local JSON body parser scoped to this route only — site.js
  // doesn't enable a global body parser so we don't surprise other
  // routes by reading bodies they don't expect.
  const parseJson = express.json({ limit: '4kb', type: 'application/json' });

  app.post('/api/translate', limiter, parseJson, async function (req, res) {
    const t0 = Date.now();

    // 1. Reject non-Q2PRO-X clients (UA is forgeable, but the lazy
    //    abuse vector is curl/wget against a discovered URL).
    const ua = String(req.get('User-Agent') || '');
    if (ua.indexOf('Q2PRO-X') !== 0) {
      logMetric(req, res, t0, 0, 0, 'forbidden_ua', 403);
      return res.status(403).json({ error: 'forbidden' });
    }

    // 2. Reject non-JSON bodies.
    const ct = String(req.get('Content-Type') || '').toLowerCase();
    if (!ct.startsWith('application/json')) {
      logMetric(req, res, t0, 0, 0, 'bad_ct', 415);
      return res.status(415).json({ error: 'expected application/json' });
    }

    // 3. Validate body shape.
    const body = req.body || {};
    const q      = body.q;
    const source = body.source;
    const target = body.target;
    const format = body.format;

    if (typeof q !== 'string' || q.length === 0 || q.length > MAX_QLEN) {
      logMetric(req, res, t0, q ? q.length : 0, 0, 'bad_q', 400);
      return res.status(400).json({ error: 'bad q' });
    }
    if (target && target !== 'en') {
      logMetric(req, res, t0, q.length, 0, 'bad_target', 400);
      return res.status(400).json({ error: 'bad target' });
    }
    if (format && format !== 'text') {
      logMetric(req, res, t0, q.length, 0, 'bad_format', 400);
      return res.status(400).json({ error: 'bad format' });
    }

    // 4. Build Yandex request body. Yandex auto-detects when source
    //    is omitted, which handles mixed RU+EN client text.
    const yandexBody = {
      folderId:           folderId,
      texts:              [q],
      targetLanguageCode: target || 'en',
    };
    if (source === 'ru')
      yandexBody.sourceLanguageCode = 'ru';

    // 5. Issue the upstream request with a hard timeout.
    let r, data;
    try {
      r = await fetch(YANDEX_URL, {
        method: 'POST',
        headers: {
          'Content-Type':  'application/json',
          'Authorization': 'Api-Key ' + apiKey,
        },
        body:   JSON.stringify(yandexBody),
        signal: AbortSignal.timeout(TIMEOUT_MS),
      });
    } catch (e) {
      const why = (e && e.name === 'TimeoutError') ? 'upstream_timeout' : 'upstream_error';
      const code = (why === 'upstream_timeout') ? 504 : 502;
      logMetric(req, res, t0, q.length, 0, why, code);
      if (DEBUG)
        process.stderr.write('[q2prox-translate] fetch error: ' + (e && e.message) + '\n');
      return res.status(code).json({ error: why });
    }

    if (!r.ok) {
      // Pull upstream body so DEBUG mode can show why Yandex rejected
      // the request. Body kept short to avoid log spam; never logged
      // outside DEBUG so production stays metrics-only per PO Q4.
      let upstreamBody = '';
      try { upstreamBody = await r.text(); } catch (_) {}
      const code = (r.status === 429) ? 429 : 502;
      logMetric(req, res, t0, q.length, 0, 'upstream_' + r.status, code);
      if (DEBUG && upstreamBody) {
        process.stderr.write(
          '[q2prox-translate] upstream HTTP ' + r.status + ' body: ' +
          upstreamBody.slice(0, 512) + '\n');
      }
      return res.status(code).json({ error: 'upstream ' + r.status });
    }

    try {
      data = await r.json();
    } catch (e) {
      logMetric(req, res, t0, q.length, 0, 'upstream_parse', 502);
      return res.status(502).json({ error: 'upstream parse' });
    }

    const translatedText =
      data && data.translations && data.translations[0] && data.translations[0].text;
    if (typeof translatedText !== 'string') {
      logMetric(req, res, t0, q.length, 0, 'upstream_shape', 502);
      return res.status(502).json({ error: 'upstream shape' });
    }

    // 6. Normalize Yandex response into LibreTranslate shape so the
    //    Q2PRO-X.exe parser (which scans for "translatedText") works
    //    without any client-side change.
    logMetric(req, res, t0, q.length, translatedText.length, 'ok', 200);
    res.json({ translatedText: translatedText });
  });
}

module.exports = {
  attachQ2proxTranslateRoute: attachQ2proxTranslateRoute,
};
