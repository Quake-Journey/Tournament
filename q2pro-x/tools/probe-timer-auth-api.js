'use strict';

const crypto = require('crypto');
const fs = require('fs');

const timerAuth = require('../timer-authorization');

function argumentsFrom(argv) {
  const values = {};
  for (let index = 0; index < argv.length; index += 2) {
    if (!argv[index].startsWith('--') || index + 1 >= argv.length) {
      throw new Error('arguments must be --name value pairs');
    }
    values[argv[index].slice(2)] = argv[index + 1];
  }
  return values;
}

async function main() {
  const args = argumentsFrom(process.argv.slice(2));
  const endpoint = timerAuth.normalizeHost(args.address || '');
  const port = Number(args.port);
  if (!args.url || !args.public || !endpoint.family ||
      !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error(
      'usage: --url <https://host/api/...> --public <descriptor.json> ' +
      '--address <IPv4|IPv6> --port <1..65535> [--hostname <dns>]'
    );
  }
  const descriptor = JSON.parse(await fs.promises.readFile(args.public, 'utf8'));
  const nonce = crypto.randomBytes(16);
  const rawRequest = timerAuth.encodeRequest({
    family: endpoint.family,
    address: endpoint.address,
    port,
    nonce,
    hostname: args.hostname || '',
    build: 'Q2PRO-X timer API production probe',
  });
  const response = await fetch(args.url, {
    method: 'POST',
    redirect: 'error',
    headers: { 'Content-Type': 'application/octet-stream' },
    body: rawRequest,
  });
  if (response.status !== 200) throw new Error('API returned HTTP ' + response.status);
  if (String(response.headers.get('content-type') || '').split(';')[0] !== 'application/octet-stream') {
    throw new Error('API returned an unexpected content type');
  }
  const declaredLength = Number(response.headers.get('content-length') || 0);
  if (declaredLength > timerAuth.constants.MAX_RESPONSE_BYTES) throw new Error('response is too large');
  const envelope = Buffer.from(await response.arrayBuffer());
  if (envelope.length > timerAuth.constants.MAX_RESPONSE_BYTES) throw new Error('response is too large');
  const verified = timerAuth.verifyEnvelope(envelope, descriptor.publicKeyPem);
  if (!verified.valid) throw new Error('ECDSA signature verification failed');
  const body = verified.response;
  if (body.keyId !== descriptor.keyId) throw new Error('unexpected key id');
  if (!body.requestNonce.equals(nonce)) throw new Error('client nonce mismatch');
  if (!body.requestSha256.equals(crypto.createHash('sha256').update(rawRequest).digest())) {
    throw new Error('request hash mismatch');
  }
  if (body.family !== endpoint.family || body.port !== port || !body.address.equals(endpoint.address)) {
    throw new Error('target endpoint mismatch');
  }
  process.stdout.write(JSON.stringify({
    ok: true,
    keyId: body.keyId,
    force: body.force,
    entries: body.entries.length,
    issuedAt: body.issuedAt.toString(),
    acceptBefore: body.acceptBefore.toString(),
    configSha256: body.configSha256.toString('hex'),
  }, null, 2) + '\n');
}

main().catch(function (error) {
  process.stderr.write('Error: ' + error.message + '\n');
  process.exitCode = 1;
});
