#!/usr/bin/env node

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

function parseAddress(value) {
  if (value.startsWith('[')) {
    const hostEnd = value.lastIndexOf(']');
    if (hostEnd < 0 || value[hostEnd + 1] !== ':') {
      throw new Error(`invalid address: ${value}`);
    }
    return { host: value.slice(1, hostEnd), port: Number(value.slice(hostEnd + 2)) };
  }
  const separator = value.lastIndexOf(':');
  if (separator < 1) {
    throw new Error(`invalid address: ${value}`);
  }
  return { host: value.slice(0, separator), port: Number(value.slice(separator + 1)) };
}

function findFile(root, predicate) {
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const entryPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      const match = findFile(entryPath, predicate);
      if (match) return match;
    } else if (predicate(entry.name)) {
      return entryPath;
    }
  }
  return null;
}

function call(client, method, ...args) {
  return new Promise((resolve, reject) => {
    client[method](...args, (error, result) => {
      if (error) reject(error);
      else resolve(result);
    });
  });
}

async function main() {
  if (process.argv.length !== 4) {
    throw new Error('usage: thrift_cross_language_smoke.cjs HOST:PORT GENERATED_DIR');
  }

  const { host, port } = parseAddress(process.argv[2]);
  const generatedDir = process.argv[3];
  const servicePath = findFile(generatedDir, (name) => name === 'BiubinService.js');
  const typesPath = findFile(generatedDir, (name) => name.endsWith('_types.js'));
  if (!servicePath || !typesPath) {
    throw new Error('Apache Thrift JS generator did not produce the expected service/types files');
  }

  const thrift = require('thrift');
  const service = require(servicePath);
  const types = require(typesPath);
  const connection = thrift.createConnection(host, port, {
    transport: thrift.TFramedTransport,
    protocol: thrift.TBinaryProtocol,
  });
  const connected = new Promise((resolve, reject) => {
    connection.once('connect', resolve);
    connection.once('error', reject);
  });
  const client = thrift.createClient(service, connection);

  await connected;
  try {
    const request = new types.EchoRequest();
    request.message = 'hello from node thrift';
    request.payload = Buffer.from('payload');
    const response = await call(client, 'echo', request);
    assert.equal(response.message, 'hello from node thrift');
    assert.equal(Buffer.from(response.payload).toString(), 'payload');

    const sum = await call(client, 'sum', [2, 3, 5, 7]);
    assert.equal(String(sum), '17');
  } finally {
    connection.end();
  }

  console.log('Node generated Thrift client smoke passed');
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
