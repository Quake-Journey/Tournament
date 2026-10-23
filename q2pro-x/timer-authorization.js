'use strict';

const crypto = require('crypto');
const dns = require('dns');
const fs = require('fs');
const net = require('net');
const path = require('path');
const { TextDecoder } = require('util');
const { domainToASCII } = require('url');

const express = require('express');
const rateLimit = require('express-rate-limit');

const ROUTE = '/api/q2prox/v1/live-item-timers/authorization';
const REQUEST_MAGIC = Buffer.from('Q2XTARQ1', 'ascii');
const RESPONSE_MAGIC = Buffer.from('Q2XTARS1', 'ascii');
const ENVELOPE_MAGIC = Buffer.from('Q2XTAEV1', 'ascii');
const SIGNING_CONTEXT = Buffer.from('Q2PRO-X LIVE TIMER AUTH V1\0', 'ascii');

const PROTOCOL_VERSION = 1;
const FEATURE_LIVE_ITEM_TIMERS = 1;
const MAX_REQUEST_BYTES = 1024;
const MAX_RESPONSE_BYTES = 512 * 1024;
const MAX_CONFIG_BYTES = 256 * 1024;
const MAX_ENTRIES = 512;
const MAX_HOST_BYTES = 253;
const MAX_DESCRIPTION_BYTES = 256;
const MAX_BUILD_BYTES = 96;
const MAX_RESOLVED_ADDRESSES = 16;
const RESPONSE_VALIDITY_SECONDS = 30;

const ENTRY_LITERAL = 1 << 0;
const ENTRY_DNS_FAILED = 1 << 1;
const ENTRY_DNS_SKIPPED = 1 << 2;

const strictUtf8 = new TextDecoder('utf-8', { fatal: true });
const keyCache = new Map();

class ProtocolError extends Error {}
class PolicyError extends Error {}
class ServiceError extends Error {}

class Reader {
  constructor(buffer) {
    this.buffer = buffer;
    this.offset = 0;
  }

  take(length) {
    if (!Number.isInteger(length) || length < 0 || this.offset + length > this.buffer.length) {
      throw new ProtocolError('truncated binary message');
    }
    const value = this.buffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return value;
  }

  u8() {
    return this.take(1)[0];
  }

  i8() {
    return this.take(1).readInt8(0);
  }

  u16() {
    return this.take(2).readUInt16BE(0);
  }

  u32() {
    return this.take(4).readUInt32BE(0);
  }

  u64() {
    return this.take(8).readBigUInt64BE(0);
  }

  done() {
    if (this.offset !== this.buffer.length) {
      throw new ProtocolError('trailing bytes in binary message');
    }
  }
}

class Writer {
  constructor() {
    this.parts = [];
    this.length = 0;
  }

  bytes(value) {
    const buffer = Buffer.from(value);
    this.parts.push(buffer);
    this.length += buffer.length;
  }

  u8(value) {
    if (!Number.isInteger(value) || value < 0 || value > 255) {
      throw new ProtocolError('u8 out of range');
    }
    const buffer = Buffer.allocUnsafe(1);
    buffer[0] = value;
    this.bytes(buffer);
  }

  i8(value) {
    if (!Number.isInteger(value) || value < -128 || value > 127) {
      throw new ProtocolError('i8 out of range');
    }
    const buffer = Buffer.allocUnsafe(1);
    buffer.writeInt8(value, 0);
    this.bytes(buffer);
  }

  u16(value) {
    if (!Number.isInteger(value) || value < 0 || value > 65535) {
      throw new ProtocolError('u16 out of range');
    }
    const buffer = Buffer.allocUnsafe(2);
    buffer.writeUInt16BE(value, 0);
    this.bytes(buffer);
  }

  u32(value) {
    if (!Number.isInteger(value) || value < 0 || value > 0xffffffff) {
      throw new ProtocolError('u32 out of range');
    }
    const buffer = Buffer.allocUnsafe(4);
    buffer.writeUInt32BE(value, 0);
    this.bytes(buffer);
  }

  u64(value) {
    const number = typeof value === 'bigint' ? value : BigInt(value);
    if (number < 0n || number > 0xffffffffffffffffn) {
      throw new ProtocolError('u64 out of range');
    }
    const buffer = Buffer.allocUnsafe(8);
    buffer.writeBigUInt64BE(number, 0);
    this.bytes(buffer);
  }

  finish() {
    return Buffer.concat(this.parts, this.length);
  }
}

function decodeUtf8(buffer, label) {
  try {
    return strictUtf8.decode(buffer);
  } catch (_error) {
    throw new ProtocolError(label + ' is not valid UTF-8');
  }
}

function parseIpv4(text) {
  if (!/^\d{1,3}(?:\.\d{1,3}){3}$/.test(text)) return null;
  const octets = text.split('.').map(Number);
  if (octets.some(function (value) { return value < 0 || value > 255; })) return null;
  return Buffer.from(octets);
}

function parseIpv6(text) {
  if (!text || text.includes('%')) return null;
  let source = text.toLowerCase();
  const doubleColon = source.indexOf('::');
  if (doubleColon !== -1 && source.indexOf('::', doubleColon + 2) !== -1) return null;

  const lastColon = source.lastIndexOf(':');
  const lastToken = source.slice(lastColon + 1);
  if (lastToken.includes('.')) {
    const ipv4 = parseIpv4(lastToken);
    if (!ipv4) return null;
    const replacement = ipv4.readUInt16BE(0).toString(16) + ':' +
      ipv4.readUInt16BE(2).toString(16);
    source = source.slice(0, lastColon + 1) + replacement;
  }

  const halves = source.split('::');
  const head = halves[0] ? halves[0].split(':') : [];
  const tail = halves.length === 2 && halves[1] ? halves[1].split(':') : [];
  const valid = function (token) { return /^[0-9a-f]{1,4}$/.test(token); };
  if (!head.every(valid) || !tail.every(valid)) return null;

  let words;
  if (halves.length === 1) {
    if (head.length !== 8) return null;
    words = head;
  } else {
    const zeroCount = 8 - head.length - tail.length;
    if (zeroCount < 1) return null;
    words = head.concat(new Array(zeroCount).fill('0'), tail);
  }

  const output = Buffer.alloc(16);
  words.forEach(function (word, index) {
    output.writeUInt16BE(parseInt(word, 16), index * 2);
  });
  return output;
}

function canonicalIpv6(address) {
  const words = [];
  for (let index = 0; index < 8; index += 1) {
    words.push(address.readUInt16BE(index * 2).toString(16));
  }
  return words.join(':');
}

function normalizeHost(input) {
  const value = String(input || '').trim();
  const ipv4 = parseIpv4(value);
  if (ipv4) {
    return {
      host: Array.from(ipv4).join('.'),
      family: 4,
      address: ipv4,
    };
  }

  const ipv6 = parseIpv6(value);
  if (ipv6) {
    return { host: canonicalIpv6(ipv6), family: 6, address: ipv6 };
  }

  const ascii = domainToASCII(value).toLowerCase().replace(/\.$/, '');
  if (!ascii || Buffer.byteLength(ascii, 'ascii') > MAX_HOST_BYTES) {
    throw new PolicyError('invalid or overlong DNS name');
  }
  const labels = ascii.split('.');
  if (labels.some(function (label) {
    return !label || label.length > 63 || !/^[a-z0-9-]+$/.test(label) ||
      label.startsWith('-') || label.endsWith('-');
  })) {
    throw new PolicyError('invalid DNS name');
  }
  return { host: ascii, family: 0, address: null };
}

function parseEndpoint(text) {
  const value = String(text || '').trim();
  let hostText;
  let portText;
  if (value.startsWith('[')) {
    const match = /^\[([^\]]+)\]:(\d+)$/.exec(value);
    if (!match) throw new PolicyError('invalid bracketed IPv6 endpoint');
    hostText = match[1];
    portText = match[2];
    if (!parseIpv6(hostText)) throw new PolicyError('brackets are only valid for IPv6');
  } else {
    const separator = value.lastIndexOf(':');
    if (separator <= 0 || value.indexOf(':') !== separator) {
      throw new PolicyError('endpoint must be host:port; IPv6 requires brackets');
    }
    hostText = value.slice(0, separator);
    portText = value.slice(separator + 1);
  }

  if (!/^\d{1,5}$/.test(portText)) throw new PolicyError('invalid endpoint port');
  const port = Number(portText);
  if (port < 1 || port > 65535) throw new PolicyError('endpoint port out of range');
  return Object.assign(normalizeHost(hostText), { port: port });
}

function addressToFixed(family, address) {
  if (family === 4) {
    if (!Buffer.isBuffer(address) || address.length !== 4) {
      throw new ProtocolError('invalid IPv4 address');
    }
    return Buffer.concat([address, Buffer.alloc(12)]);
  }
  if (family === 6) {
    if (!Buffer.isBuffer(address) || address.length !== 16) {
      throw new ProtocolError('invalid IPv6 address');
    }
    return Buffer.from(address);
  }
  throw new ProtocolError('unsupported address family');
}

function fixedToAddress(family, fixed) {
  if (family === 4) {
    if (!fixed.subarray(4).equals(Buffer.alloc(12))) {
      throw new ProtocolError('non-zero IPv4 padding');
    }
    return Buffer.from(fixed.subarray(0, 4));
  }
  if (family === 6) return Buffer.from(fixed);
  throw new ProtocolError('unsupported address family');
}

function encodeRequest(request) {
  const family = Number(request.family);
  const port = Number(request.port);
  const address = Buffer.from(request.address);
  const nonce = Buffer.from(request.nonce);
  const hostname = request.hostname ? normalizeHost(request.hostname).host : '';
  const hostnameBytes = Buffer.from(hostname, 'ascii');
  const buildBytes = Buffer.from(String(request.build || ''), 'utf8');

  if (port < 1 || port > 65535) throw new ProtocolError('invalid request port');
  if (nonce.length !== 16) throw new ProtocolError('request nonce must be 16 bytes');
  if (hostnameBytes.length > MAX_HOST_BYTES) throw new ProtocolError('hostname too long');
  if (buildBytes.length > MAX_BUILD_BYTES) throw new ProtocolError('build string too long');

  const writer = new Writer();
  writer.bytes(REQUEST_MAGIC);
  writer.u8(PROTOCOL_VERSION);
  writer.u8(FEATURE_LIVE_ITEM_TIMERS);
  writer.u8(family);
  writer.u8(0);
  writer.u16(port);
  writer.bytes(addressToFixed(family, address));
  writer.bytes(nonce);
  writer.u16(hostnameBytes.length);
  writer.u16(buildBytes.length);
  writer.bytes(hostnameBytes);
  writer.bytes(buildBytes);
  const output = writer.finish();
  if (output.length > MAX_REQUEST_BYTES) throw new ProtocolError('request too large');
  return output;
}

function parseRequest(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length > MAX_REQUEST_BYTES) {
    throw new ProtocolError('invalid request size');
  }
  const reader = new Reader(buffer);
  if (!reader.take(8).equals(REQUEST_MAGIC)) throw new ProtocolError('bad request magic');
  if (reader.u8() !== PROTOCOL_VERSION) throw new ProtocolError('unsupported request version');
  if (reader.u8() !== FEATURE_LIVE_ITEM_TIMERS) throw new ProtocolError('unsupported feature');
  const family = reader.u8();
  if (reader.u8() !== 0) throw new ProtocolError('reserved request byte is not zero');
  const port = reader.u16();
  if (port === 0) throw new ProtocolError('request port is zero');
  const address = fixedToAddress(family, reader.take(16));
  const nonce = Buffer.from(reader.take(16));
  const hostnameLength = reader.u16();
  const buildLength = reader.u16();
  if (hostnameLength > MAX_HOST_BYTES || buildLength > MAX_BUILD_BYTES) {
    throw new ProtocolError('request string too long');
  }
  const hostnameRaw = reader.take(hostnameLength);
  if (hostnameRaw.some(function (byte) { return byte > 0x7f; })) {
    throw new ProtocolError('request hostname is not ASCII');
  }
  const hostnameText = hostnameRaw.toString('ascii');
  let hostname = '';
  if (hostnameText) {
    try {
      hostname = normalizeHost(hostnameText).host;
    } catch (error) {
      throw new ProtocolError(error.message);
    }
  }
  const build = decodeUtf8(reader.take(buildLength), 'build string');
  if (/[\u0000-\u001f\u007f]/.test(build)) throw new ProtocolError('control byte in build string');
  reader.done();
  return { family, port, address, nonce, hostname, build };
}

function parsePolicyBuffer(raw) {
  if (!Buffer.isBuffer(raw) || raw.length > MAX_CONFIG_BYTES) {
    throw new PolicyError('policy file exceeds size limit');
  }
  let text;
  try {
    text = strictUtf8.decode(raw);
  } catch (_error) {
    throw new PolicyError('policy file is not valid UTF-8');
  }
  if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);

  const significant = text.split(/\r\n|\n|\r/).map(function (line, index) {
    return { text: line.trim(), line: index + 1 };
  }).filter(function (row) {
    return row.text && !row.text.startsWith('#');
  });

  if (!significant.length) throw new PolicyError('policy file is empty');
  const forceMatch = /^force\s*:\s*(-1|0|1)$/.exec(significant[0].text);
  if (!forceMatch) throw new PolicyError('first significant line must be force:-1, force:0 or force:1');
  const force = Number(forceMatch[1]);
  const entries = [];
  const seen = new Set();

  significant.slice(1).forEach(function (row) {
    const semicolon = row.text.indexOf(';');
    if (semicolon <= 0) throw new PolicyError('line ' + row.line + ': missing description separator');
    const endpointText = row.text.slice(0, semicolon).trim();
    const description = row.text.slice(semicolon + 1).trim();
    const descriptionBytes = Buffer.from(description, 'utf8');
    if (!description || descriptionBytes.length > MAX_DESCRIPTION_BYTES) {
      throw new PolicyError('line ' + row.line + ': empty or overlong description');
    }
    if (/[\u0000-\u001f\u007f]/.test(description)) {
      throw new PolicyError('line ' + row.line + ': control byte in description');
    }
    const endpoint = parseEndpoint(endpointText);
    const key = endpoint.host + ':' + endpoint.port;
    if (seen.has(key)) throw new PolicyError('line ' + row.line + ': duplicate endpoint ' + key);
    seen.add(key);
    entries.push(Object.assign(endpoint, { description, line: row.line }));
  });

  if (entries.length > MAX_ENTRIES) throw new PolicyError('too many policy entries');
  entries.sort(function (left, right) {
    return (left.host < right.host ? -1 : left.host > right.host ? 1 : 0) || left.port - right.port;
  });
  return {
    force,
    entries,
    configSha256: crypto.createHash('sha256').update(raw).digest(),
  };
}

function parseLookupAddress(result) {
  const family = Number(result.family) || net.isIP(result.address);
  if (family === 4) {
    const address = parseIpv4(result.address);
    if (address) return { family: 4, address };
  }
  if (family === 6) {
    const address = parseIpv6(result.address);
    if (address) return { family: 6, address };
  }
  return null;
}

function sortAndDedupeAddresses(addresses) {
  const byKey = new Map();
  addresses.forEach(function (item) {
    const key = item.family + ':' + item.address.toString('hex');
    byKey.set(key, item);
  });
  return Array.from(byKey.values()).sort(function (left, right) {
    return left.family - right.family || Buffer.compare(left.address, right.address);
  }).slice(0, MAX_RESOLVED_ADDRESSES);
}

function withTimeout(promise, timeoutMs) {
  let timer;
  const timeout = new Promise(function (_resolve, reject) {
    timer = setTimeout(function () { reject(new ServiceError('DNS lookup timed out')); }, timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(function () { clearTimeout(timer); });
}

async function resolvePolicy(policy, options) {
  const settings = options || {};
  const lookup = settings.lookup || dns.promises.lookup.bind(dns.promises);
  const concurrency = Math.max(1, Math.min(32, Number(settings.concurrency || 8)));
  const perLookupTimeoutMs = Math.max(50, Number(settings.perLookupTimeoutMs || 750));
  const overallTimeoutMs = Math.max(perLookupTimeoutMs, Number(settings.overallTimeoutMs || 2000));
  const deadline = Date.now() + overallTimeoutMs;
  const entries = policy.entries.map(function (entry) {
    const copy = Object.assign({}, entry, { addresses: [], flags: 0 });
    if (entry.family === 4 || entry.family === 6) {
      copy.flags |= ENTRY_LITERAL;
      copy.addresses = [{ family: entry.family, address: Buffer.from(entry.address) }];
    } else if (policy.force !== 0) {
      copy.flags |= ENTRY_DNS_SKIPPED;
    }
    return copy;
  });

  const tasks = entries.filter(function (entry) {
    return entry.family === 0 && policy.force === 0;
  });
  let next = 0;
  async function worker() {
    while (next < tasks.length) {
      if (Date.now() >= deadline) throw new ServiceError('global DNS resolution deadline exceeded');
      const entry = tasks[next];
      next += 1;
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new ServiceError('global DNS resolution deadline exceeded');
      try {
        const results = await withTimeout(
          Promise.resolve(lookup(entry.host, { all: true, verbatim: true })),
          Math.min(perLookupTimeoutMs, remaining)
        );
        entry.addresses = sortAndDedupeAddresses((results || []).map(parseLookupAddress).filter(Boolean));
        if (!entry.addresses.length) entry.flags |= ENTRY_DNS_FAILED;
      } catch (error) {
        if (Date.now() >= deadline) throw new ServiceError('global DNS resolution deadline exceeded');
        entry.flags |= ENTRY_DNS_FAILED;
        entry.addresses = [];
      }
    }
  }

  await Promise.all(new Array(Math.min(concurrency, tasks.length)).fill(0).map(worker));
  return { force: policy.force, entries, configSha256: policy.configSha256 };
}

function encodeSignedBody(data) {
  if (data.force !== -1 && data.force !== 0 && data.force !== 1) {
    throw new ServiceError('invalid policy force value');
  }
  if (!Buffer.isBuffer(data.serverNonce) || data.serverNonce.length !== 16 ||
      !Buffer.isBuffer(data.requestSha256) || data.requestSha256.length !== 32 ||
      !Buffer.isBuffer(data.policy.configSha256) || data.policy.configSha256.length !== 32) {
    throw new ServiceError('invalid signed digest or nonce length');
  }
  const writer = new Writer();
  writer.bytes(RESPONSE_MAGIC);
  writer.u8(PROTOCOL_VERSION);
  writer.u8(FEATURE_LIVE_ITEM_TIMERS);
  writer.u16(data.keyId);
  writer.i8(data.force);
  writer.u8(data.request.family);
  writer.u16(data.request.port);
  writer.bytes(addressToFixed(data.request.family, data.request.address));
  writer.bytes(data.request.nonce);
  writer.bytes(data.serverNonce);
  writer.bytes(data.requestSha256);
  writer.bytes(data.policy.configSha256);
  writer.u64(data.issuedAt);
  writer.u64(data.acceptBefore);
  writer.u16(data.policy.entries.length);

  data.policy.entries.forEach(function (entry) {
    const host = Buffer.from(entry.host, 'ascii');
    const description = Buffer.from(entry.description, 'utf8');
    writer.u16(host.length);
    writer.u16(entry.port);
    writer.u16(description.length);
    writer.u8(entry.addresses.length);
    writer.u8(entry.flags);
    writer.bytes(host);
    writer.bytes(description);
    entry.addresses.forEach(function (item) {
      writer.u8(item.family);
      writer.bytes(addressToFixed(item.family, item.address));
    });
  });

  const body = writer.finish();
  if (body.length > MAX_RESPONSE_BYTES - 14 - 64) {
    throw new ServiceError('signed response body exceeds size limit');
  }
  return body;
}

function decodeSignedBody(body) {
  const reader = new Reader(body);
  if (!reader.take(8).equals(RESPONSE_MAGIC)) throw new ProtocolError('bad response magic');
  const version = reader.u8();
  const feature = reader.u8();
  if (version !== PROTOCOL_VERSION || feature !== FEATURE_LIVE_ITEM_TIMERS) {
    throw new ProtocolError('unsupported response protocol');
  }
  const keyId = reader.u16();
  const force = reader.i8();
  if (force !== -1 && force !== 0 && force !== 1) throw new ProtocolError('invalid force value');
  const family = reader.u8();
  const port = reader.u16();
  const address = fixedToAddress(family, reader.take(16));
  const requestNonce = Buffer.from(reader.take(16));
  const serverNonce = Buffer.from(reader.take(16));
  const requestSha256 = Buffer.from(reader.take(32));
  const configSha256 = Buffer.from(reader.take(32));
  const issuedAt = reader.u64();
  const acceptBefore = reader.u64();
  if (port === 0 || acceptBefore < issuedAt ||
      acceptBefore - issuedAt > BigInt(RESPONSE_VALIDITY_SECONDS)) {
    throw new ProtocolError('invalid target port or response validity window');
  }
  const entryCount = reader.u16();
  if (entryCount > MAX_ENTRIES) throw new ProtocolError('too many response entries');
  const entries = [];
  const seenEntries = new Set();
  for (let index = 0; index < entryCount; index += 1) {
    const hostLength = reader.u16();
    const entryPort = reader.u16();
    const descriptionLength = reader.u16();
    const addressCount = reader.u8();
    const flags = reader.u8();
    if (hostLength > MAX_HOST_BYTES || descriptionLength > MAX_DESCRIPTION_BYTES ||
        addressCount > MAX_RESOLVED_ADDRESSES || entryPort === 0) {
      throw new ProtocolError('invalid response entry bounds');
    }
    const hostBytes = reader.take(hostLength);
    if (!hostLength || hostBytes.some(function (byte) { return byte > 0x7f; })) {
      throw new ProtocolError('invalid response entry host');
    }
    const host = hostBytes.toString('ascii');
    let normalizedHost;
    try {
      normalizedHost = normalizeHost(host).host;
    } catch (_error) {
      throw new ProtocolError('invalid normalized response host');
    }
    if (normalizedHost !== host) throw new ProtocolError('non-canonical response host');
    const entryKey = host + ':' + entryPort;
    if (seenEntries.has(entryKey)) throw new ProtocolError('duplicate response entry');
    seenEntries.add(entryKey);
    const description = decodeUtf8(reader.take(descriptionLength), 'entry description');
    if (!description || /[\u0000-\u001f\u007f]/.test(description) ||
        (flags & ~(ENTRY_LITERAL | ENTRY_DNS_FAILED | ENTRY_DNS_SKIPPED)) !== 0) {
      throw new ProtocolError('invalid response description or flags');
    }
    const addresses = [];
    const seenAddresses = new Set();
    for (let addressIndex = 0; addressIndex < addressCount; addressIndex += 1) {
      const entryFamily = reader.u8();
      const item = {
        family: entryFamily,
        address: fixedToAddress(entryFamily, reader.take(16)),
      };
      const addressKey = item.family + ':' + item.address.toString('hex');
      if (seenAddresses.has(addressKey)) throw new ProtocolError('duplicate response address');
      seenAddresses.add(addressKey);
      addresses.push(item);
    }
    entries.push({ host, port: entryPort, description, addresses, flags });
  }
  reader.done();
  return {
    version, feature, keyId, force, family, port, address, requestNonce, serverNonce,
    requestSha256, configSha256, issuedAt, acceptBefore, entries,
  };
}

function encodeEnvelope(body, signature) {
  if (!Buffer.isBuffer(signature) || signature.length !== 64) {
    throw new ServiceError('ECDSA signature is not 64-byte IEEE P1363');
  }
  const writer = new Writer();
  writer.bytes(ENVELOPE_MAGIC);
  writer.u32(body.length);
  writer.bytes(body);
  writer.u16(signature.length);
  writer.bytes(signature);
  const envelope = writer.finish();
  if (envelope.length > MAX_RESPONSE_BYTES) throw new ServiceError('response exceeds size limit');
  return envelope;
}

function decodeEnvelope(envelope) {
  if (!Buffer.isBuffer(envelope) || envelope.length > MAX_RESPONSE_BYTES) {
    throw new ProtocolError('invalid response envelope size');
  }
  const reader = new Reader(envelope);
  if (!reader.take(8).equals(ENVELOPE_MAGIC)) throw new ProtocolError('bad envelope magic');
  const bodyLength = reader.u32();
  const body = Buffer.from(reader.take(bodyLength));
  const signatureLength = reader.u16();
  if (signatureLength !== 64) throw new ProtocolError('unexpected signature length');
  const signature = Buffer.from(reader.take(signatureLength));
  reader.done();
  return { body, signature };
}

function validatePrivateKey(key) {
  const keyObject = key instanceof crypto.KeyObject ? key : crypto.createPrivateKey(key);
  if (keyObject.type !== 'private' || keyObject.asymmetricKeyType !== 'ec') {
    throw new ServiceError('signing key is not an EC private key');
  }
  const details = keyObject.asymmetricKeyDetails || {};
  const publicJwk = crypto.createPublicKey(keyObject).export({ format: 'jwk' });
  if ((details.namedCurve && details.namedCurve !== 'prime256v1') || publicJwk.crv !== 'P-256') {
    throw new ServiceError('signing key curve must be P-256/prime256v1');
  }
  return keyObject;
}

async function loadSigningContext(options) {
  if (options && options.signingKey) {
    const keyId = Number(options.keyId);
    if (!Number.isInteger(keyId) || keyId < 1 || keyId > 65535) {
      throw new ServiceError('invalid signing key id');
    }
    return { keyId, privateKey: validatePrivateKey(options.signingKey) };
  }

  const privateKeyFile = String(
    (options && options.privateKeyFile) || process.env.Q2PROX_TIMER_AUTH_PRIVATE_KEY_FILE || ''
  );
  const keyId = Number((options && options.keyId) || process.env.Q2PROX_TIMER_AUTH_KEY_ID || 0);
  if (!privateKeyFile || !Number.isInteger(keyId) || keyId < 1 || keyId > 65535) {
    throw new ServiceError('timer authorization signing key is not configured');
  }

  let stat;
  try {
    stat = await fs.promises.stat(privateKeyFile);
  } catch (_error) {
    throw new ServiceError('timer authorization private key is unavailable');
  }
  if (!stat.isFile()) throw new ServiceError('timer authorization private key path is not a file');
  const cacheKey = path.resolve(privateKeyFile) + ':' + keyId;
  const stamp = stat.size + ':' + stat.mtimeMs;
  const cached = keyCache.get(cacheKey);
  if (cached && cached.stamp === stamp) return cached.context;

  let pem;
  try {
    pem = await fs.promises.readFile(privateKeyFile);
  } catch (_error) {
    throw new ServiceError('timer authorization private key cannot be read');
  }
  const context = { keyId, privateKey: validatePrivateKey(pem) };
  keyCache.set(cacheKey, { stamp, context });
  return context;
}

function signResponse(input) {
  const now = Number.isInteger(input.now) ? input.now : Math.floor(Date.now() / 1000);
  const body = encodeSignedBody({
    keyId: input.signing.keyId,
    force: input.policy.force,
    request: input.request,
    serverNonce: input.serverNonce || crypto.randomBytes(16),
    requestSha256: crypto.createHash('sha256').update(input.rawRequest).digest(),
    policy: input.policy,
    issuedAt: now,
    acceptBefore: now + RESPONSE_VALIDITY_SECONDS,
  });
  const signature = crypto.sign(
    'sha256',
    Buffer.concat([SIGNING_CONTEXT, body]),
    { key: input.signing.privateKey, dsaEncoding: 'ieee-p1363' }
  );
  return encodeEnvelope(body, signature);
}

function verifyEnvelope(envelope, publicKey) {
  const decoded = decodeEnvelope(envelope);
  const valid = crypto.verify(
    'sha256',
    Buffer.concat([SIGNING_CONTEXT, decoded.body]),
    { key: publicKey, dsaEncoding: 'ieee-p1363' },
    decoded.signature
  );
  return {
    valid,
    response: valid ? decodeSignedBody(decoded.body) : null,
    body: decoded.body,
    signature: decoded.signature,
  };
}

function sendBinaryError(res, status) {
  res.status(status);
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Content-Length', '0');
  res.end();
}

function attachQ2proxTimerAuthorizationRoute(app, options) {
  const settings = options || {};
  const rpm = Math.max(1, Number(settings.rpm || process.env.Q2PROX_TIMER_AUTH_RPM || 60));
  const limiter = rateLimit({
    windowMs: 60 * 1000,
    limit: rpm,
    standardHeaders: 'draft-8',
    legacyHeaders: false,
    handler: function (_req, res) { sendBinaryError(res, 429); },
  });
  const rawParser = express.raw({ type: 'application/octet-stream', limit: MAX_REQUEST_BYTES });
  const parseBinary = function (req, res, next) {
    const type = String(req.headers['content-type'] || '').split(';', 1)[0].trim().toLowerCase();
    if (type !== 'application/octet-stream') return sendBinaryError(res, 415);
    rawParser(req, res, function (error) {
      if (error) return sendBinaryError(res, error.type === 'entity.too.large' ? 413 : 400);
      next();
    });
  };

  app.post(ROUTE, limiter, parseBinary, async function (req, res) {
    const started = Date.now();
    try {
      if (!Buffer.isBuffer(req.body)) throw new ProtocolError('missing binary request body');
      const request = parseRequest(req.body);
      const configPath = String(
        settings.configPath || process.env.Q2PROX_TIMER_AUTH_CONFIG ||
        path.join(__dirname, 'config', 'timer-authorization-servers.txt')
      );
      const rawPolicy = await fs.promises.readFile(configPath);
      const parsedPolicy = parsePolicyBuffer(rawPolicy);
      const policy = await resolvePolicy(parsedPolicy, {
        lookup: settings.lookup,
        concurrency: settings.dnsConcurrency,
        perLookupTimeoutMs: settings.dnsPerLookupTimeoutMs,
        overallTimeoutMs: settings.dnsOverallTimeoutMs,
      });
      const signing = await loadSigningContext(settings);
      const envelope = signResponse({
        request,
        rawRequest: req.body,
        policy,
        signing,
        serverNonce: settings.randomBytes ? settings.randomBytes(16) : undefined,
        now: settings.now ? settings.now() : undefined,
      });
      res.status(200);
      res.setHeader('Content-Type', 'application/octet-stream');
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Pragma', 'no-cache');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.setHeader('X-Q2PROX-Key-Id', String(signing.keyId));
      res.setHeader('Content-Length', String(envelope.length));
      res.end(envelope);
      process.stdout.write('[q2prox-timer-auth] ' + JSON.stringify({
        status: 'ok', force: policy.force, entries: policy.entries.length,
        latency_ms: Date.now() - started,
      }) + '\n');
    } catch (error) {
      const status = error instanceof ProtocolError ? 400 : 503;
      process.stderr.write('[q2prox-timer-auth] ' + JSON.stringify({
        status: status === 400 ? 'bad_request' : 'unavailable',
        reason: String(error && error.message || 'unknown').slice(0, 180),
        latency_ms: Date.now() - started,
      }) + '\n');
      sendBinaryError(res, status);
    }
  });
}

function publicKeyDescriptor(privateOrPublicKey, keyId) {
  const publicKey = crypto.createPublicKey(privateOrPublicKey);
  const jwk = publicKey.export({ format: 'jwk' });
  return {
    protocol: 'Q2PRO-X live item timer authorization v1',
    keyId: Number(keyId),
    algorithm: 'ECDSA P-256 with SHA-256, IEEE P1363 r||s',
    curve: 'P-256',
    xBase64Url: jwk.x,
    yBase64Url: jwk.y,
    xHex: Buffer.from(jwk.x, 'base64url').toString('hex'),
    yHex: Buffer.from(jwk.y, 'base64url').toString('hex'),
    publicKeyPem: publicKey.export({ type: 'spki', format: 'pem' }),
  };
}

module.exports = {
  attachQ2proxTimerAuthorizationRoute,
  encodeRequest,
  parseRequest,
  parsePolicyBuffer,
  resolvePolicy,
  signResponse,
  verifyEnvelope,
  decodeEnvelope,
  decodeSignedBody,
  publicKeyDescriptor,
  normalizeHost,
  parseEndpoint,
  constants: {
    ROUTE,
    REQUEST_MAGIC: REQUEST_MAGIC.toString('ascii'),
    RESPONSE_MAGIC: RESPONSE_MAGIC.toString('ascii'),
    ENVELOPE_MAGIC: ENVELOPE_MAGIC.toString('ascii'),
    SIGNING_CONTEXT: SIGNING_CONTEXT.toString('ascii'),
    PROTOCOL_VERSION,
    FEATURE_LIVE_ITEM_TIMERS,
    MAX_REQUEST_BYTES,
    MAX_RESPONSE_BYTES,
    RESPONSE_VALIDITY_SECONDS,
    ENTRY_LITERAL,
    ENTRY_DNS_FAILED,
    ENTRY_DNS_SKIPPED,
  },
  errors: { ProtocolError, PolicyError, ServiceError },
};
