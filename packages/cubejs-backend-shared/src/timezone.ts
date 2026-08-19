import moment from 'moment-timezone';

/**
 * Resolves `value` to a canonical tz-database zone name, matched case-insensitively,
 * or `null` when it is unset, empty or not a known zone.
 *
 * @throws {TypeError} when `value` is neither a string nor unset.
 */
export function canonicalTimezone(value?: string | null): string | null {
  // `''` is checked explicitly rather than left to moment.tz.zone(), which happens to
  // return null for it. Note `!value` would swallow non-strings that must throw below.
  if (value === undefined || value === null || value === '') {
    return null;
  }

  if (typeof value !== 'string') {
    throw new TypeError(`Timezone must be a string, got ${typeof value}`);
  }

  const zone = moment.tz.zone(value);

  return zone ? zone.name : null;
}
