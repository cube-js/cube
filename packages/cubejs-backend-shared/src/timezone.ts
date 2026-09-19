import moment from 'moment-timezone';

/**
 * Resolves `value` to a canonical tz-database zone name, matched case-insensitively,
 * or `null` when it is unset or not a known zone.
 *
 * @throws {TypeError} when `value` is an empty string, or is neither a string nor unset.
 */
export function canonicalTimezone(value?: string | null): string | null {
  if (value === undefined || value === null) {
    return null;
  }

  if (typeof value !== 'string') {
    throw new TypeError(`Timezone must be a string, got ${typeof value}`);
  }

  if (value === '') {
    throw new TypeError('Timezone must not be empty');
  }

  const zone = moment.tz.zone(value);

  return zone ? zone.name : null;
}
