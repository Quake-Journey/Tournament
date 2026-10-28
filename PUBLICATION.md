# Public source layout

This repository contains the tournament service and the current Q2PRO-X website:

- `site.js`, `analytics.js`, `grid.js`, `servers.js`: website integration.
- `q2pro-x/`: bilingual Q2PRO-X pages, release information, HTML book chapters,
  Mapgen Studio page, translation proxy and live timer authorization API.
- `public/q2pro-x/`: public documentation, PDF books, branding and illustrations.
- `public/mapgen/`: public Mapgen Studio images.

BotStats is an independent service: https://github.com/Quake-Journey/BotStats .
The main menu links to https://q2pro-x.com/botstats/ .

## Private files

Create `.env` from `.env.example`. It is deliberately ignored, along with local
server allowlists, private signing keys, logs, screenshots, database content,
and local/production JSON configurations. `.env.countries` and `.env.nicks` are
optional deployment files and must not be committed with real player mappings.
The timer authorization sample contains documentation addresses; the test fixture
is test-only and must never be installed as a production signing key.

Large downloadable game/Studio packages in `public/**/downloads/` are hosted
separately and are not source files. Preserve those packages when deploying an
update; copying this repository is not a command to erase a server directory.
The public HTML documentation and PDF books are included.

## Development

Install dependencies with `npm ci`. Run `npm run test:timer-auth` for the signed
timer API. Configure MongoDB and `.env` before running `node site.js`.
The website uses the existing tournament database; it does not install BotStats
or manage its database. See `q2pro-x/TIMER_AUTHORIZATION_API_V1.md` for the optional
timer endpoint configuration.

Restart the existing website process after deploying changed source files.
Reverse-proxy routing and production secrets are managed outside this repository.

## Q2TV is spectator-only

`q2tv/spectator-policy.js` is used by the existing WS→UDP handler in `site.js`.
It applies equally to the desktop and mobile engines, the server browser and
BotStats embeds. Initial connect and userinfo updates force `spectator=1`;
join/team/ready and inventory activation (`invuse`, including Join in mod menus)
are not forwarded. Read-only camera/score commands and engine sign-on remain
available. The decoder accepts the engines' Quake II 34 and R1Q2 35 wire formats;
unknown extensions and malformed packets fail closed. Do not loosen it by
passing unknown commands through to the game server.

`q2tv/viewer-only.js` labels both player pages as spectator-only and removes
inapplicable mobile buttons. The authoritative restriction is at the gateway,
so console commands and stale cached HTML cannot bypass it. These small helpers
are source files, not copies of the engines or their game data. `Q2TV_WEBROOT`
and `Q2TV_MOBILE_WEBROOT` remain unchanged. Deploy `site.js` and the `q2tv/`
helpers together, then restart the existing Tournament website process.

Focused protocol tests: `node --test q2tv/spectator-policy.test.js`.

### HTTP download and preloaded maps

Deploy `q2tv/http-download.js` and `q2tv/http-client.js` together with `site.js`.
The existing gateway learns `dlserver=` from the game connection. The browser
requests only a resource path and game server; it cannot supply an arbitrary
HTTP destination. The endpoint validates resource paths, rejects private/local
destinations, pins the validated DNS address, revalidates redirects, and limits
transfer size/concurrency/time. Thus an HTTPS player also works with a game
server advertising a plain HTTP download mirror without browser mixed-content
or cross-origin exceptions.

Both rebuilt native clients first check their existing packs/files, then use
at most two HTTP transfers. A missing map stays a foreground dependency;
models, sounds and BSP textures already known to precache can fill the second
slot. A 404 retries that exact file through the existing UDP downloader. An
unreachable HTTP service disables further HTTP attempts for the connection;
UDP still downloads required files. No `dlserver` means direct native UDP.
Disconnect/map change cancels pending work. Launch arguments set `rate 250000`
after saved configuration; the game server may impose its own lower rate cap.

The shared `pak9.pak` contains 23 maps: q2duel1–8, ztn2dm1–5, ztn2dm1_c,
koldduel1, omf, q3t2/q3t2g, cor/cor_d/cor_f/corl/cor_ffa_d, and their runtime
dependencies. Its manifest has hashes for all 938 entries. Unused q3t2 entity
model fields are documented: OpenTDM-X/baseq2 ignore misc_model and use their
stock banner model for misc_banner. Existing base packs/configuration remain.

The JS/WASM/data trio must be deployed as one matching build in each existing
Q2TV webroot. Preserve the custom HTML, renderer side modules and other assets;
update matching `.gz` files too so an older compressed file cannot win content
negotiation. No second engine/data copy belongs inside BotStats.

Build sources are the existing WSL `~/webquake2` and `~/qwasm2` trees. The common
adapter is `q2tv/native-http.h`; `patch-native.py` records exact originals and
applies the minimal downloader hooks. Desktop compatibility changes for the
installed Emscripten 5.0.3 are explicit IDBFS linking, current linker settings,
SDL DOM targets, the existing SDL mutex object, and the EGL callback cast.
Use a full desktop object rebuild after changing compiler versions. The mobile
build uses its existing `GL4ES_PATH=~/gl4es_pic` configuration. Preserve custom
player HTML when the mobile makefile regenerates its example shell.

Tests: `node --test q2tv/spectator-policy.test.js q2tv/http-download.test.js`.
Real loopback-server browser checks cover HTTP success, HTTP 404, HTTP outage,
no HTTP advertisement, both native engines, rate 250000, and two simultaneous
missing texture downloads. Test-only fixture servers are not deployment files.

**Server auto-join:** per the PO decision, no game mod is changed. The shared
`native-viewer.h` checks native playerstate in every valid server frame, including
the first. If the server actually put this client into play, it requests
spectator mode and sends the read-only `spectate` command. Existing spectator
and chase-camera states are left alone; retries are limited to one per second.
Checks run only after WASM initialization. Closing the player also issues the
native disconnect. Tested on OpenFFA at q2.playground.ru:27910 and OpenTDM-X
at 91.227.18.67:27950, for both engines. The latter exercised auto-join followed
by one automatic exit and stable spectator state. See the review note for the
reason a command filter alone was insufficient.

The shared `q2tv/player-page.js` initializes both helpers before the engine,
preserves the desktop fetch loader and mobile shell, and versions JS/WASM/data
URLs together. It also updates the desktop displayed preload size from the
actual data file. Deploy this helper with site.js; old cached engine assets
must not be mixed with the updated data pack. HTML rendering errors use the
normal Express error handler rather than terminating the website process.

Input/lifecycle fixes: desktop must export `AL` alongside ccall/cwrap for its
existing click-to-resume-audio handler. The viewer helper focuses the canvas,
gives desktop controls natural height, and closes WebSockets on WASM abort.
`closeSpectatorUDP` sends this session's sequenced disconnects before releasing
UDP on socket close; a crashed tab must not linger until the server timeout.
`test/q2tv-input-browser.mjs` in BotStats verifies mouse, Enter/Space, fullscreen,
100/125/150% scale, spectator state and server-side removal after close/abort.

Q2TV 0.2 beta: both native console labels and HTML/loading/window labels use
the new product version. Desktop console stamps (version and clock) now align
using R_GetCharSize(), matching the configured scaled font, instead of fixed
8-pixel widths that placed their right edge outside the render area. Viewport
and canvas bounds were checked at widths 390/1280/1920/2560, inline fullscreen,
and the desktop Fullscreen button. This button expands the player document;
the game stays below its controls instead of covering them with a fixed canvas.
Source patches remain in patch-native.py;
existing map/config packs are byte-for-byte unchanged.

### Persistent Q2TV resource cache

`asset-manifest.js` hashes runtime files with bounded streaming reads and caches
the results while file identity is unchanged. Every JS/WASM/data resource has
its own SHA-256 URL; changing a helper or a timestamp cannot invalidate a map
package. HTML remains revalidated. Static responses support ETag/304 and reject
a requested content hash that no longer matches the deployed file.

`asset-cache.js` saves complete resources in the origin's `q2tv-assets-v1`
Cache Storage, keyed by filename and content hash, independent of server query
parameters. Both native engines, including mobile renderer modules, use it.
The engine script is loaded from this cache before execution; native resource
fetches keep the same player-directory paths. Stream backpressure limits extra
JS buffering, and a failed disk write continues the original network stream.
Partial downloads are not committed. Superseded versions of a file are removed
only after its replacement is stored. Live status, RCON, WebSockets and optional
game-server downloads are outside this static resource cache.

Browser storage can be cleared by the user or evicted by the browser. In that
case the next launch downloads normally. Separate domains/browser profiles have
separate storage. No service worker is registered over Tournament or BotStats.

Checks: real desktop/mobile engines in Chromium, including a browser process
restart, downloaded zero native bytes on subsequent launches. Chromium/Firefox
fixtures cover restart persistence, changed-file-only updates, timestamp-only
changes, truncated downloads and denied/full storage. The Windows Playwright
WebKit port loses even a five-byte standalone Cache Storage entry on navigation;
its same-document cache and failure fallback were checked, but persistence on
an Apple device is not claimed by this test.
