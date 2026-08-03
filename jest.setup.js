const { webcrypto } = require('node:crypto');

if (!globalThis.crypto) {
  globalThis.crypto = webcrypto;
}
