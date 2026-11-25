import crypto from 'crypto';

export type KeyHashAlgorithm = 'md5' | 'sha256';

let keyHashAlgorithm: KeyHashAlgorithm | undefined;

/**
 * Algorithm for Cube's non-security hashes: cache, queue and pre-aggregation version keys.
 * MD5 wherever the runtime provides it, so existing keys and table versions stay the same.
 * SHA-256 where MD5 is disabled (OpenSSL in FIPS mode) and `createHash('md5')` throws.
 */
export function getKeyHashAlgorithm(): KeyHashAlgorithm {
  if (!keyHashAlgorithm) {
    try {
      crypto.createHash('md5');
      keyHashAlgorithm = 'md5';
    } catch {
      keyHashAlgorithm = 'sha256';
    }
  }

  return keyHashAlgorithm;
}

export function createKeyHash(): crypto.Hash {
  return crypto.createHash(getKeyHashAlgorithm());
}
