'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const express = require('express');

const timerAuth = require('../timer-authorization');

function keyPair() {
  return crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
}

function makeRequest(overrides) {
  return timerAuth.encodeRequest(Object.assign({
    family: 4,
    port: 27910,
    address: Buffer.from([203, 0, 113, 25]),
    nonce: Buffer.from('00112233445566778899aabbccddeeff', 'hex'),
    hostname: 'q2.playground.ru',
    build: 'Q2PRO-X 1.5 test',
  }, overrides || {}));
}

test('request protocol round-trips and rejects ambiguous input', function () {
  const encoded = makeRequest({ hostname: 'Q2.PlayGround.RU.' });
  const decoded = timerAuth.parseRequest(encoded);
  assert.equal(decoded.family, 4);
  assert.equal(decoded.port, 27910);
  assert.deepEqual(decoded.address, Buffer.from([203, 0, 113, 25]));
  assert.equal(decoded.hostname, 'q2.playground.ru');
  assert.equal(decoded.build, 'Q2PRO-X 1.5 test');

  assert.throws(function () {
    timerAuth.parseRequest(Buffer.concat([encoded, Buffer.from([0])]));
  }, /trailing bytes/);

  const badPadding = Buffer.from(encoded);
  badPadding[20] = 1;
  assert.throws(function () { timerAuth.parseRequest(badPadding); }, /IPv4 padding/);
});

test('policy parser canonicalizes endpoints and rejects the whole invalid policy', function () {
  const raw = Buffer.from(
    '\ufeff# policy\r\nforce:0\r\nExample.COM.:27910;Primary; with note\r\n' +
    '[2001:0db8::1]:27911;IPv6 server\r\n203.0.113.9:27912;IPv4 server\r\n',
    'utf8'
  );
  const parsed = timerAuth.parsePolicyBuffer(raw);
  assert.equal(parsed.force, 0);
  assert.equal(parsed.entries.length, 3);
  assert.deepEqual(parsed.entries.map(function (entry) { return entry.host; }), [
    '2001:db8:0:0:0:0:0:1',
    '203.0.113.9',
    'example.com',
  ]);
  assert.equal(parsed.entries[2].description, 'Primary; with note');
  assert.equal(parsed.configSha256.length, 32);

  assert.throws(function () {
    timerAuth.parsePolicyBuffer(Buffer.from(
      'force:0\n[2001:db8::1]:27910;one\n[2001:0db8:0:0:0:0:0:1]:27910;two\n'
    ));
  }, /duplicate endpoint/);
  assert.throws(function () {
    timerAuth.parsePolicyBuffer(Buffer.from('force:0\n2001:db8::1:27910;bad\n'));
  }, /IPv6 requires brackets/);
  assert.throws(function () {
    timerAuth.parsePolicyBuffer(Buffer.from('force:2\n'));
  }, /first significant line/);
});

test('force:0 resolves DNS deterministically while DNS failure never broadens allow', async function () {
  const parsed = timerAuth.parsePolicyBuffer(Buffer.from(
    'force:0\nexample.com:27910;DNS\nbad.example:27911;Broken DNS\n203.0.113.9:27912;Literal\n'
  ));
  const resolved = await timerAuth.resolvePolicy(parsed, {
    lookup: async function (host) {
      if (host === 'bad.example') throw new Error('NXDOMAIN');
      return [
        { address: '2001:db8::2', family: 6 },
        { address: '203.0.113.8', family: 4 },
        { address: '203.0.113.8', family: 4 },
      ];
    },
  });
  const dnsEntry = resolved.entries.find(function (entry) { return entry.host === 'example.com'; });
  const failedEntry = resolved.entries.find(function (entry) { return entry.host === 'bad.example'; });
  const literalEntry = resolved.entries.find(function (entry) { return entry.host === '203.0.113.9'; });
  assert.deepEqual(dnsEntry.addresses.map(function (item) { return item.family; }), [4, 6]);
  assert.equal(failedEntry.addresses.length, 0);
  assert.ok(failedEntry.flags & timerAuth.constants.ENTRY_DNS_FAILED);
  assert.ok(literalEntry.flags & timerAuth.constants.ENTRY_LITERAL);
});

test('signed responses bind request and policy, verify, and differ for replayed requests', async function () {
  const keys = keyPair();
  const rawRequest = makeRequest();
  const request = timerAuth.parseRequest(rawRequest);
  const parsed = timerAuth.parsePolicyBuffer(Buffer.from('force:1\nexample.com:27910;Ignored list\n'));
  const policy = await timerAuth.resolvePolicy(parsed);
  const signing = { keyId: 7, privateKey: keys.privateKey };
  const first = timerAuth.signResponse({ request, rawRequest, policy, signing, now: 1783872000 });
  const second = timerAuth.signResponse({ request, rawRequest, policy, signing, now: 1783872000 });

  assert.notDeepEqual(first, second, 'fresh server nonce must change every response');
  const verified = timerAuth.verifyEnvelope(first, keys.publicKey);
  assert.equal(verified.valid, true);
  assert.equal(verified.response.keyId, 7);
  assert.equal(verified.response.force, 1);
  assert.equal(verified.response.acceptBefore - verified.response.issuedAt, 30n);
  assert.deepEqual(verified.response.requestNonce, request.nonce);
  assert.deepEqual(
    verified.response.requestSha256,
    crypto.createHash('sha256').update(rawRequest).digest()
  );

  const tampered = Buffer.from(first);
  tampered[tampered.length - 1] ^= 1;
  assert.equal(timerAuth.verifyEnvelope(tampered, keys.publicKey).valid, false);
});

test('HTTP API hot-reloads policy, signs binary responses, and fails closed', async function (t) {
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'q2prox-timer-auth-'));
  const configPath = path.join(directory, 'policy.txt');
  await fs.promises.writeFile(configPath, 'force:0\nexample.com:27910;Test server\n', 'utf8');
  t.after(async function () { await fs.promises.rm(directory, { recursive: true, force: true }); });

  const keys = keyPair();
  const app = express();
  app.set('trust proxy', 1);
  timerAuth.attachQ2proxTimerAuthorizationRoute(app, {
    configPath,
    signingKey: keys.privateKey,
    keyId: 23,
    rpm: 100,
    now: function () { return 1783872000; },
    lookup: async function () { return [{ address: '203.0.113.25', family: 4 }]; },
  });
  const server = await new Promise(function (resolve) {
    const instance = app.listen(0, '127.0.0.1', function () { resolve(instance); });
  });
  t.after(function () { return new Promise(function (resolve) { server.close(resolve); }); });
  const url = 'http://127.0.0.1:' + server.address().port + timerAuth.constants.ROUTE;
  const request = makeRequest();

  async function post(body, contentType) {
    return fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': contentType || 'application/octet-stream' },
      body,
    });
  }

  const firstHttp = await post(request);
  assert.equal(firstHttp.status, 200);
  assert.equal(firstHttp.headers.get('cache-control'), 'no-store');
  assert.equal(firstHttp.headers.get('x-q2prox-key-id'), '23');
  const first = Buffer.from(await firstHttp.arrayBuffer());
  const firstVerified = timerAuth.verifyEnvelope(first, keys.publicKey);
  assert.equal(firstVerified.valid, true);
  assert.equal(firstVerified.response.force, 0);
  assert.equal(firstVerified.response.entries[0].addresses.length, 1);

  await fs.promises.writeFile(configPath, 'force:-1\n', 'utf8');
  const reloadedHttp = await post(request);
  assert.equal(reloadedHttp.status, 200);
  const reloaded = timerAuth.verifyEnvelope(
    Buffer.from(await reloadedHttp.arrayBuffer()), keys.publicKey
  );
  assert.equal(reloaded.response.force, -1, 'policy must change without restarting Express');

  await fs.promises.writeFile(configPath, 'force:9\n', 'utf8');
  assert.equal((await post(request)).status, 503);
  assert.equal((await post(Buffer.from('{}'), 'application/json')).status, 415);
  assert.equal((await post(Buffer.from('broken'))).status, 400);
});

test('public key descriptor exposes only the client verification material', function () {
  const keys = keyPair();
  const descriptor = timerAuth.publicKeyDescriptor(keys.privateKey, 31);
  assert.equal(descriptor.keyId, 31);
  assert.equal(descriptor.curve, 'P-256');
  assert.equal(descriptor.xHex.length, 64);
  assert.equal(descriptor.yHex.length, 64);
  assert.match(descriptor.publicKeyPem, /BEGIN PUBLIC KEY/);
  assert.doesNotMatch(JSON.stringify(descriptor), /PRIVATE KEY/);
});

test('published key-1 interoperability fixture verifies byte-for-byte', async function () {
  const fixturePath = path.join(
    __dirname, 'fixtures', 'timer-authorization-v1-key1.json'
  );
  const fixture = JSON.parse(await fs.promises.readFile(fixturePath, 'utf8'));
  assert.doesNotMatch(JSON.stringify(fixture), /BEGIN (?:EC )?PRIVATE KEY/);
  const envelope = Buffer.from(fixture.envelopeHex, 'hex');
  const verified = timerAuth.verifyEnvelope(envelope, fixture.key.publicKeyPem);
  assert.equal(verified.valid, true);
  assert.equal(verified.response.keyId, fixture.key.keyId);
  assert.equal(verified.response.force, fixture.expected.force);
  assert.equal(verified.response.issuedAt, BigInt(fixture.expected.issuedAt));
  assert.equal(verified.response.acceptBefore, BigInt(fixture.expected.acceptBefore));
  assert.equal(verified.response.requestSha256.toString('hex'), fixture.requestSha256Hex);
  assert.equal(verified.response.configSha256.toString('hex'), fixture.expected.configSha256Hex);
  assert.equal(verified.response.serverNonce.toString('hex'), fixture.expected.serverNonceHex);
});
