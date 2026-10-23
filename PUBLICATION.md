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
