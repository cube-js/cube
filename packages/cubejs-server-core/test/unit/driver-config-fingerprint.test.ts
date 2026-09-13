import { fingerprint } from '../../src/core/driver-config-fingerprint';

describe('fingerprint', () => {
  test('is stable across property order', () => {
    expect(fingerprint({ type: 'databricks-jdbc', token: 'a', url: 'u' }))
      .toEqual(fingerprint({ url: 'u', token: 'a', type: 'databricks-jdbc' }));
  });

  test('changes when a nested value changes', () => {
    expect(fingerprint({ type: 'postgres', options: { password: 'one' } }))
      .not.toEqual(fingerprint({ type: 'postgres', options: { password: 'two' } }));
  });

  // The case this exists for: a rotated per-user OAuth token has to be visible
  // as a different configuration, or the cached driver is never rebuilt.
  test('changes when only the credential changes', () => {
    const base = { type: 'databricks-jdbc', url: 'jdbc:databricks://host', acceptPolicy: true };

    expect(fingerprint({ ...base, token: 'token-issued-at-09:00' }))
      .not.toEqual(fingerprint({ ...base, token: 'token-issued-at-10:00' }));
  });

  test('treats an absent property and an undefined one as equal', () => {
    expect(fingerprint({ type: 'postgres', catalog: undefined }))
      .toEqual(fingerprint({ type: 'postgres' }));
  });

  test('distinguishes arrays by order', () => {
    expect(fingerprint({ scopes: ['a', 'b'] })).not.toEqual(fingerprint({ scopes: ['b', 'a'] }));
  });

  test('handles dates, bigints and nested structures', () => {
    const value = {
      when: new Date('2026-07-31T12:00:00.000Z'),
      big: BigInt(42),
      nested: [{ a: 1 }, { b: [true, null] }],
    };

    expect(fingerprint(value)).toEqual(fingerprint({
      nested: [{ a: 1 }, { b: [true, null] }],
      big: BigInt(42),
      when: new Date('2026-07-31T12:00:00.000Z'),
    }));
    expect(fingerprint(value)).not.toEqual(fingerprint({ ...value, big: BigInt(43) }));
  });

  test('does not expose the value it hashes', () => {
    const digest = fingerprint({ type: 'postgres', password: 'super-secret' });

    expect(digest).not.toContain('super-secret');
    expect(digest).toMatch(/^[0-9a-f]{32}$/);
  });

  test('returns null for a circular structure rather than throwing', () => {
    const circular: Record<string, unknown> = { type: 'postgres' };
    circular.self = circular;

    expect(fingerprint(circular)).toBeNull();
  });

  test('returns a digest for null and undefined', () => {
    expect(fingerprint(null)).toEqual(fingerprint(undefined));
    expect(fingerprint(null)).not.toBeNull();
  });
});
