/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview Fingerprinting for driver configurations and security contexts.
 */

import crypto from 'crypto';

// Keyed per process, so a heap dump cannot test guessed passwords against a digest.
const FINGERPRINT_KEY = crypto.randomBytes(32);

/**
 * Deterministic JSON with sorted keys and stable placeholders for values JSON
 * cannot represent. Throws on a circular structure ("not fingerprintable").
 */
function stableStringify(value: unknown, seen: Set<unknown>): string {
  if (value === undefined || value === null) {
    return 'null';
  }

  const type = typeof value;

  if (type === 'string' || type === 'number' || type === 'boolean') {
    return JSON.stringify(value);
  }

  if (type === 'bigint') {
    return JSON.stringify((value as bigint).toString());
  }

  // Functions hash to a constant: a credential passed as a provider callback is
  // never seen to rotate, so a factory that needs rebuilds must return the value.
  if (type === 'function' || type === 'symbol') {
    return JSON.stringify(`[${type}]`);
  }

  if (value instanceof Date) {
    return JSON.stringify(value.toISOString());
  }

  if (seen.has(value)) {
    throw new Error('Circular structure cannot be fingerprinted');
  }

  seen.add(value);

  try {
    if (Array.isArray(value)) {
      return `[${value.map((item) => stableStringify(item, seen)).join(',')}]`;
    }

    // Own enumerable keys only: values behind prototype accessors are never seen to rotate.
    const entries = Object.keys(value as Record<string, unknown>)
      .sort()
      .reduce<string[]>((acc, key) => {
        const entry = (value as Record<string, unknown>)[key];

        // Match JSON.stringify: undefined-valued properties are absent, so
        // `{ a: undefined }` and `{}` fingerprint the same.
        if (entry !== undefined) {
          acc.push(`${JSON.stringify(key)}:${stableStringify(entry, seen)}`);
        }

        return acc;
      }, []);

    return `{${entries.join(',')}}`;
  } finally {
    seen.delete(value);
  }
}

/**
 * A short digest of `value`, stable within this process and keyed so no credential is recoverable.
 * `null` means "cannot tell", which callers must read as "assume unchanged".
 */
export function fingerprint(value: unknown): string | null {
  try {
    return crypto
      .createHmac('sha256', FINGERPRINT_KEY)
      .update(stableStringify(value, new Set()))
      // 32 hex chars = 128 bits, which is far more than an equality check over
      // the handful of configurations one process resolves needs, and keeps the
      // digest short enough to sit in a log line.
      .digest('hex')
      .slice(0, 32);
  } catch (e) {
    return null;
  }
}
