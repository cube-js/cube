/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The optional lifetime a `driverFactory` can put on the
 * configuration it returns.
 */

import type { DriverConfig } from './types';

/**
 * Above this a number is read as epoch milliseconds, below it as epoch seconds.
 * 1e11 milliseconds is 1973 and 1e11 seconds is the year 5138, so nothing
 * anyone can mean today is ambiguous. Both spellings are accepted because the
 * value is usually copied straight off a credential, and the ecosystems
 * disagree: JavaScript counts milliseconds, Python's `time.time()` seconds.
 */
const MILLISECONDS_THRESHOLD = 1e11;

/**
 * A driver lifetime as a POSIX timestamp in milliseconds, or undefined when
 * none was given or the value cannot be read as a moment in time.
 *
 * Unreadable input is dropped rather than rejected: an expiry is an
 * optimisation over comparing configurations, and failing a deployment's
 * queries over a malformed one would be a worse outcome than the behaviour it
 * had before the field existed.
 */
export function parseDriverExpiry(value: unknown): number | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }

  if (value instanceof Date) {
    const time = value.getTime();

    return Number.isNaN(time) ? undefined : time;
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0) {
      return undefined;
    }

    return value > MILLISECONDS_THRESHOLD ? value : value * 1000;
  }

  if (typeof value === 'string') {
    const text = value.trim();

    if (!text) {
      return undefined;
    }

    // A stringified timestamp, which `Date.parse` would read as a year.
    if (/^\d+(\.\d+)?$/.test(text)) {
      return parseDriverExpiry(Number(text));
    }

    const parsed = Date.parse(text);

    return Number.isNaN(parsed) ? undefined : parsed;
  }

  return undefined;
}

/**
 * The same configuration without its lifetime, for the two places that must not
 * see it: the fingerprint that decides whether the connection changed, and the
 * options handed to the driver's own constructor.
 *
 * Returns the input untouched when there is nothing to strip, so the common
 * case allocates nothing.
 */
export function withoutDriverExpiry(config: DriverConfig): DriverConfig {
  if (!config || typeof config !== 'object' || !('expiresAt' in config)) {
    return config;
  }

  const { expiresAt: _lifetime, ...rest } = config;

  return <DriverConfig>rest;
}
