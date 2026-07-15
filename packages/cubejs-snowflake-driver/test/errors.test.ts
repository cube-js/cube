import { describe, expect, test } from 'vitest';

import { ConnectionError, SnowflakeError } from '../src/errors';

describe('ConnectionError', () => {
  test('turns the opaque snowflake-sdk `reading \'replace\'` TypeError into an actionable message', () => {
    // This is exactly what snowflake-sdk throws from inside its own error
    // formatting when authentication fails on some paths (CUB-1676) — an error
    // that tells the operator nothing about the real cause.
    const opaque = new TypeError("Cannot read properties of undefined (reading 'replace')");

    const err = new ConnectionError(opaque);

    expect(err).toBeInstanceOf(SnowflakeError);
    expect(err.name).toBe('ConnectionError');
    expect(err.message).toMatch(/^Unable to connect to Snowflake: /);
    // The operator is pointed at the credentials.
    expect(err.message).toMatch(/credential/i);
    // The original error is preserved for logs.
    expect(err.cause).toBe(opaque);
  });

  test('matches the opaque TypeError by shape, across SDK phrasings/fields', () => {
    // Older V8 phrasing ("property", singular) and a different dereferenced
    // field must both be caught — we match by shape, not an exact string.
    for (const message of [
      "Cannot read property 'replace' of undefined",
      "Cannot read properties of undefined (reading 'substring')",
    ]) {
      expect(new ConnectionError(new TypeError(message)).message).toMatch(/credential/i);
    }
  });

  test('keeps the original message for an opaque-shaped TypeError (so a real deref bug is not masked)', () => {
    // A genuine null-deref bug happens to share V8's shape. We must NOT throw
    // its pinpoint message away — append the hint, don't replace.
    const realBug = new TypeError("Cannot read properties of undefined (reading 'toUpperCase')");

    const err = new ConnectionError(realBug);

    expect(err.message).toContain("reading 'toUpperCase'");
    expect(err.message).toMatch(/credential/i);
  });

  test('preserves a real SDK error message (e.g. RequestFailedError / missing account)', () => {
    const real = new Error('Incorrect username or password was specified.');

    const err = new ConnectionError(real);

    expect(err.message).toBe(
      'Unable to connect to Snowflake: Incorrect username or password was specified.'
    );
    expect(err.cause).toBe(real);
  });

  test('does not swallow an unrelated TypeError that carries a real message', () => {
    const real = new TypeError('privateKey must be a string');
    const err = new ConnectionError(real);
    expect(err.message).toBe('Unable to connect to Snowflake: privateKey must be a string');
  });

  test('flattens an AggregateError into its underlying messages', () => {
    const aggregate = new AggregateError([
      new Error('getaddrinfo ENOTFOUND acme.snowflakecomputing.com'),
      new Error('connect ETIMEDOUT'),
    ]);

    const err = new ConnectionError(aggregate);

    expect(err.message).toBe(
      'Unable to connect to Snowflake: ' +
      'getaddrinfo ENOTFOUND acme.snowflakecomputing.com, connect ETIMEDOUT'
    );
  });

  test('falls back to the hint (no dangling colon) for an empty AggregateError', () => {
    const err = new ConnectionError(new AggregateError([]));
    expect(err.message).toMatch(/credential/i);
    expect(err.message).not.toMatch(/Snowflake:\s*$/);
  });

  test('does not repeat the hint when AggregateError sub-errors have empty messages', () => {
    const err = new ConnectionError(new AggregateError([new Error(''), new Error('')]));
    // Empty sub-messages are dropped, so the hint appears exactly once.
    expect(err.message.match(/verify the account/gi)?.length ?? 0).toBe(1);
  });

  test('extracts the message from a non-Error rejection object (SDK HTTP-layer shape)', () => {
    // snowflake-sdk's transitive layers can reject with a plain object.
    const err = new ConnectionError({ message: 'Incorrect username or password was specified.', code: 390144 });
    expect(err.message).toBe(
      'Unable to connect to Snowflake: Incorrect username or password was specified.'
    );
  });

  test('falls back to the hint for an empty or non-descriptive cause', () => {
    expect(new ConnectionError(undefined).message).toMatch(/credential/i);
    expect(new ConnectionError(new Error('')).message).toMatch(/credential/i);
    expect(new ConnectionError({ code: 390144 }).message).toMatch(/credential/i);
  });

  test('surfaces a plain string cause', () => {
    const err = new ConnectionError('Browser action timed out after 60000 ms.');

    expect(err.message).toBe(
      'Unable to connect to Snowflake: Browser action timed out after 60000 ms.'
    );
  });

  test('retains a non-Error cause object (so its diagnostic fields survive)', () => {
    const raw = { message: 'Incorrect username or password was specified.', code: 390144 };
    const err = new ConnectionError(raw);
    expect(err.cause).toBe(raw);
  });

  test('uses an AggregateError own message when it has no sub-errors', () => {
    const err = new ConnectionError(new AggregateError([], 'All Snowflake endpoints failed'));
    expect(err.message).toBe('Unable to connect to Snowflake: All Snowflake endpoints failed');
  });

  test('appends the hint to an opaque TypeError nested inside an AggregateError', () => {
    const aggregate = new AggregateError([
      new TypeError("Cannot read properties of undefined (reading 'replace')"),
    ]);
    expect(new ConnectionError(aggregate).message).toMatch(/credential/i);
  });

  test('does not double punctuation when appending the hint', () => {
    const err = new ConnectionError(
      new TypeError("Cannot read properties of undefined (reading 'replace').")
    );
    expect(err.message).not.toContain("').. ");
    expect(err.message).not.toMatch(/\.\s+\.\s/);
  });

  test('drops whitespace-only aggregate members without a dangling separator', () => {
    const err = new ConnectionError(new AggregateError([new Error('   '), new Error('ETIMEDOUT')]));
    expect(err.message).toBe('Unable to connect to Snowflake: ETIMEDOUT');
  });

  test('never throws itself when the cause is hostile (throwing message getter)', () => {
    const hostile = {
      get message(): string {
        throw new Error('boom');
      },
    };
    expect(() => new ConnectionError(hostile)).not.toThrow();
    expect(new ConnectionError(hostile).message).toMatch(/credential/i);
  });
});
