'use strict';

const fs = require('fs');
const path = require('path');

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
  const privateFile = path.resolve(args.private || '');
  const configFile = path.resolve(args.config || '');
  const outputFile = path.resolve(args.output || '');
  const keyId = Number(args['key-id']);
  if (!args.private || !args.config || !args.output ||
      !Number.isInteger(keyId) || keyId < 1 || keyId > 65535) {
    throw new Error('usage: --private <pem> --config <policy> --output <json> --key-id <1..65535>');
  }
  if (fs.existsSync(outputFile)) throw new Error('refusing to overwrite fixture');

  const privateKey = await fs.promises.readFile(privateFile);
  const rawPolicy = await fs.promises.readFile(configFile);
  const policy = await timerAuth.resolvePolicy(timerAuth.parsePolicyBuffer(rawPolicy), {
    lookup: async function () { throw new Error('fixture must not depend on live DNS'); },
  });
  const rawRequest = timerAuth.encodeRequest({
    family: 4,
    port: 27910,
    address: Buffer.from([203, 0, 113, 25]),
    nonce: Buffer.from('00112233445566778899aabbccddeeff', 'hex'),
    hostname: 'q2.example.net',
    build: 'Q2PRO-X 1.5 protocol fixture',
  });
  const envelope = timerAuth.signResponse({
    request: timerAuth.parseRequest(rawRequest),
    rawRequest,
    policy,
    signing: { keyId, privateKey },
    serverNonce: Buffer.from('ffeeddccbbaa99887766554433221100', 'hex'),
    now: 1700000000,
  });
  const decoded = timerAuth.decodeEnvelope(envelope);
  const descriptor = timerAuth.publicKeyDescriptor(privateKey, keyId);
  const fixture = {
    warning: 'PUBLIC VERIFICATION FIXTURE ONLY. It contains no private key and is expired.',
    protocol: 'Q2PRO-X live item timer authorization v1',
    key: descriptor,
    requestHex: rawRequest.toString('hex'),
    requestSha256Hex: require('crypto').createHash('sha256').update(rawRequest).digest('hex'),
    envelopeHex: envelope.toString('hex'),
    signedBodyHex: decoded.body.toString('hex'),
    signatureP1363Hex: decoded.signature.toString('hex'),
    expected: {
      force: policy.force,
      targetFamily: 4,
      targetAddressHex: 'cb007119',
      targetPort: 27910,
      clientNonceHex: '00112233445566778899aabbccddeeff',
      serverNonceHex: 'ffeeddccbbaa99887766554433221100',
      issuedAt: 1700000000,
      acceptBefore: 1700000030,
      configSha256Hex: policy.configSha256.toString('hex'),
      entryCount: policy.entries.length,
    },
  };
  await fs.promises.mkdir(path.dirname(outputFile), { recursive: true });
  await fs.promises.writeFile(outputFile, JSON.stringify(fixture, null, 2) + '\n', { flag: 'wx' });
  process.stdout.write('Wrote public fixture: ' + outputFile + '\n');
}

main().catch(function (error) {
  process.stderr.write('Error: ' + error.message + '\n');
  process.exitCode = 1;
});
