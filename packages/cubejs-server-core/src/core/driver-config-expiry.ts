/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The optional lifetime a `driverFactory` can put on the
 * configuration it returns.
 */

import type { DriverConfig } from './types';

/**
 * Above this, epoch ms; below, epoch seconds (JS vs Python `time.time()`), so
 * neither is ambiguous for any real date.
 */
const MILLISECONDS_THRESHOLD = 1e11;

/**
 * A driver lifetime in epoch ms. Unreadable input yields `undefined` rather than
 * throwing, so a malformed expiry never fails queries.
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

/** Strip the lifetime before fingerprinting and before passing options to the driver constructor. */
export function withoutDriverExpiry(config: DriverConfig): DriverConfig {
  if (!config || typeof config !== 'object' || !('expiresAt' in config)) {
    return config;
  }

  const { expiresAt: _lifetime, ...rest } = config;

  return <DriverConfig>rest;
}
