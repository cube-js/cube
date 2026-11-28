import crypto from 'crypto';

/**
 * Hash for Cube's non-security keys: cache, queue and pre-aggregation version keys.
 * MD5, so existing keys and table versions stay the same. SHA-256 in FIPS mode,
 * where OpenSSL does not provide MD5.
 */
export function createKeyHash(): crypto.Hash {
  return crypto.createHash(crypto.getFips() ? 'sha256' : 'md5');
}
