import moment from 'moment-timezone';

/**
 * Resolves `value` to a canonical tz-database zone name, matched case-insensitively,
 * or `null` when it is not a known zone.
 */
export function canonicalTimezone(value: string | null): string | null {
  // Real callers are plain JS, so the signature is not enforced: moment.tz.zone() throws a
  // TypeError on a non-string, which would surface as a 500 instead of a validation error.
  if (typeof value !== 'string' || value === '') {
    return null;
  }

  const zone = moment.tz.zone(value);

  return zone ? zone.name : null;
}
