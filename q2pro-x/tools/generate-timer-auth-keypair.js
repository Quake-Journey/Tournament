'use strict';

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const { publicKeyDescriptor } = require('../timer-authorization');

function fail(message) {
  process.stderr.write('Error: ' + message + '\n');
  process.exitCode = 1;
}

function parseArguments(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 1) {
    const name = argv[index];
    if (name === '--private' || name === '--public' || name === '--key-id') {
      if (index + 1 >= argv.length) throw new Error('missing value after ' + name);
      result[name.slice(2)] = argv[index + 1];
      index += 1;
    } else {
      throw new Error('unknown argument ' + name);
    }
  }
  return result;
}

function isWithin(parent, child) {
  const relative = path.relative(path.resolve(parent), path.resolve(child));
  return relative === '' || (!relative.startsWith('..' + path.sep) && relative !== '..' && !path.isAbsolute(relative));
}

async function main() {
  const args = parseArguments(process.argv.slice(2));
  const privateFile = args.private && path.resolve(args.private);
  const publicFile = args.public && path.resolve(args.public);
  const keyId = Number(args['key-id']);
  if (!privateFile || !publicFile || !Number.isInteger(keyId) || keyId < 1 || keyId > 65535) {
    throw new Error(
      'usage: node generate-timer-auth-keypair.js --private <outside-webroot.pem> ' +
      '--public <descriptor.json> --key-id <1..65535>'
    );
  }
  const websiteRoot = path.resolve(__dirname, '..', '..');
  if (isWithin(websiteRoot, privateFile)) {
    throw new Error('private key destination must be outside the website root: ' + websiteRoot);
  }
  if (fs.existsSync(privateFile) || fs.existsSync(publicFile)) {
    throw new Error('refusing to overwrite an existing key file');
  }

  const keys = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
  const privatePem = keys.privateKey.export({ type: 'pkcs8', format: 'pem' });
  const descriptor = Object.assign(
    { generatedAt: new Date().toISOString() },
    publicKeyDescriptor(keys.privateKey, keyId)
  );

  await fs.promises.mkdir(path.dirname(privateFile), { recursive: true, mode: 0o700 });
  await fs.promises.mkdir(path.dirname(publicFile), { recursive: true });
  await fs.promises.writeFile(privateFile, privatePem, { flag: 'wx', mode: 0o600 });
  try {
    await fs.promises.writeFile(publicFile, JSON.stringify(descriptor, null, 2) + '\n', { flag: 'wx' });
  } catch (error) {
    await fs.promises.unlink(privateFile).catch(function () {});
    throw error;
  }
  process.stdout.write(JSON.stringify({
    ok: true,
    keyId,
    privateKeyFile: privateFile,
    publicKeyFile: publicFile,
    xHex: descriptor.xHex,
    yHex: descriptor.yHex,
  }, null, 2) + '\n');
}

main().catch(function (error) { fail(error.message); });
