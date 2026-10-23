# Q2PRO-X Live Item Timer Authorization API v1

This document is the byte-level contract between the Q2PRO-X client and the
website backend. All integer fields are unsigned big-endian unless explicitly
marked as signed. Messages containing unknown values, bad lengths, non-zero
reserved fields, or trailing bytes must be rejected.

## Endpoint

`POST /api/q2prox/v1/live-item-timers/authorization`

- Request and successful response: `application/octet-stream`.
- Maximum request: 1024 bytes. Maximum response: 512 KiB.
- Redirects are forbidden in the client.
- Both `https://q2pro-x.com` and `https://quake2.pro` expose the same endpoint,
  policy, and signing key set.
- Non-200 responses are technical failures and are never authorization.

## Request: `Q2XTARQ1`

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `Q2XTARQ1` |
| 8 | 1 | protocol version = `1` |
| 9 | 1 | feature = `1` (live item timers) |
| 10 | 1 | target family: `4` or `6` |
| 11 | 1 | reserved = `0` |
| 12 | 2 | exact connected game-server port, 1..65535 |
| 14 | 16 | target address; IPv4 uses first 4 bytes and 12 zero bytes |
| 30 | 16 | fresh client CSPRNG nonce |
| 46 | 2 | normalized hostname length, 0..253 |
| 48 | 2 | client build string length, 0..96 |
| 50 | variable | lowercase ASCII A-label hostname, optional |
| next | variable | UTF-8 build string, optional |

The target address and port are the endpoint selected for the current client
connection generation, not merely the text originally entered by the player.
The client creates a new 16-byte nonce for every API attempt, including fallback.

## Signed response body: `Q2XTARS1`

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `Q2XTARS1` |
| 8 | 1 | protocol version = `1` |
| 9 | 1 | feature = `1` |
| 10 | 2 | signing key id |
| 12 | 1 | signed `force`: `0xff` = -1, `0`, or `1` |
| 13 | 1 | echoed target family |
| 14 | 2 | echoed target port |
| 16 | 16 | echoed target address in fixed form |
| 32 | 16 | echoed client nonce |
| 48 | 16 | fresh server CSPRNG nonce |
| 64 | 32 | SHA-256 of the exact request body |
| 96 | 32 | SHA-256 of the exact policy-file bytes |
| 128 | 8 | `issued_at`, Unix seconds |
| 136 | 8 | `accept_before`, Unix seconds |
| 144 | 2 | normalized policy entry count, 0..512 |
| 146 | variable | policy entries |

Every response has a fresh server nonce, so even a byte-for-byte replayed
request cannot produce an identical signed response.

Each policy entry is encoded as follows:

| Size | Field |
|---:|---|
| 2 | ASCII host length, 1..253 |
| 2 | exact port |
| 2 | UTF-8 description length, 1..256 |
| 1 | resolved address count, 0..16 |
| 1 | flags |
| variable | normalized host |
| variable | description |
| 17 each | family byte (`4`/`6`) and 16-byte fixed address |

Entry flags:

- `0x01`: source host is a literal IP address.
- `0x02`: DNS resolution failed; the address list is empty and grants nothing.
- `0x04`: DNS resolution was skipped because `force` is not zero.

For IPv6, the normalized host is eight lowercase hexadecimal groups without
zero compression. IPv4 is canonical dotted decimal. DNS names are lowercase
ASCII IDNA A-labels without a terminal dot.

## Envelope and signature

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | ASCII magic `Q2XTAEV1` |
| 8 | 4 | signed body length |
| 12 | variable | signed response body |
| next | 2 | signature length, exactly 64 |
| next | 64 | ECDSA signature as IEEE P1363 `r || s` |

Signature algorithm: ECDSA P-256 with SHA-256.

The exact bytes passed to SHA-256/ECDSA are:

```text
ASCII("Q2PRO-X LIVE TIMER AUTH V1") || 0x00 || signed_response_body
```

The signature does not cover HTTP headers. A client must verify the signature
before using any field from the response body.

## Client acceptance order

1. Require HTTPS with normal certificate and hostname validation; forbid redirects.
2. Require HTTP 200, the binary content type, and the response size cap.
3. Parse all bounds, fixed values, and trailing-byte rules.
4. Select only a compiled-in current or next public key by `key_id`.
5. Verify the ECDSA signature over the exact typed payload above.
6. Match request SHA-256, client nonce, target family/address/port, and the still-current
   connection generation.
7. Require `accept_before >= issued_at`, a validity span no greater than 30 seconds,
   and a response that is not expired.
8. Apply `force`: `-1` denies, `1` allows, `0` allows only when a signed entry has the
   exact target port and an exact binary target-address match.
9. Any parse, crypto, DNS-list, timeout, or lifecycle mismatch denies timers without
   interrupting connection to the game server.

## Policy file

The backend reads and validates the complete UTF-8 file on every request.
No Node.js restart is needed after a policy-only edit.

```text
force:0
q2.playground.ru:27910;PG DM
203.0.113.10:27910;Example IPv4 server
[2001:db8::10]:27910;Example IPv6 server
```

- `force:0`: use the exact endpoint list.
- `force:1`: allow all remote servers that reached website validation.
- `force:-1`: deny all remote servers that reached website validation.
- Blank lines and lines beginning with `#` are ignored.
- The first semicolon separates endpoint and description; later semicolons belong
  to the description.
- Any malformed or duplicate normalized endpoint invalidates the whole file and
  makes the API return HTTP 503.
- Under `force:0`, DNS is resolved on the backend and signed as binary A/AAAA
  addresses. Resolution failure for one row grants no address for that row.

## Key configuration

The private key is never stored under the website root and never returned by the
API. The runtime reads:

```text
Q2PROX_TIMER_AUTH_PRIVATE_KEY_FILE=<absolute private PKCS#8 PEM path>
Q2PROX_TIMER_AUTH_KEY_ID=1
Q2PROX_TIMER_AUTH_CONFIG=<optional absolute policy path>
Q2PROX_TIMER_AUTH_RPM=60
```

The client receives only the public P-256 coordinates and key id. Key rotation
uses compiled-in current and next public-key slots; the backend switches ids only
after the next public key has shipped to clients.
