/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview Fingerprinting for driver configurations and security contexts.
 */

import crypto from 'crypto';

/**
 * Deterministic JSON used for fingerprinting. Object keys are emitted in sorted
 * order so two structures that differ only in property order hash the same, and
 * values JSON cannot represent are reduced to stable placeholders rather than
 * silently disappearing. Throws on a circular structure, which callers treat as
 * "not fingerprintable".
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

  // A closure's identity cannot be compared meaningfully across calls, so it
  // contributes a constant. Two configs differing only in a function body are
  // therefore treated as equal — deliberately conservative: it can only lead to
  // reusing a connection, never to swapping one out unnecessarily.
  //
  // The practical consequence is that a config carrying its credential as a
  // provider callback rather than a resolved value fingerprints identically
  // however the credential rotates, so such a driver is never rebuilt. A
  // `driverFactory` that needs rotation to be noticed has to return the
  // resolved value.
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

    // Own enumerable keys only, so a class instance holding its values behind
    // prototype accessors fingerprints as `{}` — constant, and therefore another
    // shape whose rotation goes unnoticed. Plain configs are unaffected.
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
 * A short, stable digest of `value`, or `null` when it cannot be fingerprinted.
 *
 * Hashed rather than kept verbatim because the values being compared include
 * database passwords and OAuth access tokens: a raw copy would live for the
 * lifetime of the process and surface in any heap dump. `null` means "cannot
 * tell whether this changed", and every caller must treat that as "assume it
 * did not" so behaviour falls back to the previous resolve-once semantics.
 */
export function fingerprint(value: unknown): string | null {
  try {
    return crypto
      .createHash('sha256')
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
