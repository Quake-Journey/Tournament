# Website backup

Development remains local. This repository records the current website sources
and static resources when a Q2PRO-X beta/release backup is prepared.

Restore with `npm ci`, copy `.env.example` to `.env`, then fill the credentials
locally. Keep `.env.countries` and `.env.nicks`: these are alias mappings, not keys.
Do not commit tokens, database passwords, proxy credentials or TLS private keys.

This is a source backup, not a MongoDB database dump. Existing external MongoDB
data, Telegram/Yandex account access, DNS and server-side secrets must be restored
from their respective services. Download packages and the complete local source
snapshot are retained separately in the private Q2PRO-X recovery repository.

No website deployment or production database migration is performed by this sync.
