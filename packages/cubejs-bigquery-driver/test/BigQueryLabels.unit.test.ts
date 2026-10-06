import { QueryOptions } from '@cubejs-backend/base-driver';
import { BigQueryDriver } from '../src';

class BigQueryDriverOpen extends BigQueryDriver {
  public override buildQueryLabels(options?: QueryOptions): { [k: string]: string } | undefined {
    return super.buildQueryLabels(options);
  }
}

const driver = Object.create(BigQueryDriverOpen.prototype) as BigQueryDriverOpen;
const buildQueryLabels = (options?: QueryOptions) => driver.buildQueryLabels(options);

describe('BigQueryDriver.buildQueryLabels', () => {
  test('forwards the query UUID (with the -span-N suffix stripped) as the cube_request_id label', () => {
    expect(buildQueryLabels({ requestId: 'd94e2b1a-1c2d-4e5f-8a9b-0c1d2e3f4a5b-span-1' })).toEqual({
      cube_request_id: 'd94e2b1a-1c2d-4e5f-8a9b-0c1d2e3f4a5b',
    });
  });

  test('keeps the requestId as-is when there is no -span- suffix', () => {
    expect(buildQueryLabels({ requestId: 'd94e2b1a-1c2d-4e5f-8a9b-0c1d2e3f4a5b' })).toEqual({
      cube_request_id: 'd94e2b1a-1c2d-4e5f-8a9b-0c1d2e3f4a5b',
    });
  });

  test('sanitizes disallowed characters and lowercases', () => {
    expect(buildQueryLabels({ requestId: 'abc-DEF.123' })).toEqual({
      cube_request_id: 'abc-def_123',
    });
  });

  test('truncates values longer than 63 characters', () => {
    const requestId = 'a'.repeat(100);
    const result = buildQueryLabels({ requestId });
    expect(result?.cube_request_id).toHaveLength(63);
    expect(result?.cube_request_id).toBe('a'.repeat(63));
  });

  test('returns undefined when there is no requestId', () => {
    expect(buildQueryLabels(undefined)).toBeUndefined();
    expect(buildQueryLabels({})).toBeUndefined();
    expect(buildQueryLabels({ requestId: '' })).toBeUndefined();
    expect(buildQueryLabels({ queryTags: {} })).toBeUndefined();
  });

  test('replaces every disallowed character rather than dropping them', () => {
    expect(buildQueryLabels({ requestId: '....' })).toEqual({
      cube_request_id: '____',
    });
  });

  test('adds query tags next to cube_request_id', () => {
    expect(buildQueryLabels({
      requestId: 'd94e2b1a-1c2d-4e5f-8a9b-0c1d2e3f4a5b-span-1',
      queryTags: { user_id: 'u-42', tenant: 'acme' },
    })).toEqual({
      user_id: 'u-42',
      tenant: 'acme',
      cube_request_id: 'd94e2b1a-1c2d-4e5f-8a9b-0c1d2e3f4a5b',
    });
  });

  test('sanitizes query tag keys and values like cube_request_id', () => {
    expect(buildQueryLabels({ queryTags: { 'User.Email': 'John.Doe@Example.com', ['k'.repeat(70)]: 'v'.repeat(70) } }))
      .toEqual({
        user_email: 'john_doe_example_com',
        ['k'.repeat(63)]: 'v'.repeat(63),
      });
  });

  test('drops tag keys BigQuery would reject', () => {
    expect(buildQueryLabels({ requestId: 'abc', queryTags: { '1st_party': 'a', _tenant: 'b', '-x': 'c', '': 'd', ok: 'e' } }))
      .toEqual({ ok: 'e', cube_request_id: 'abc' });
  });

  test('logs the tags it drops with the reason', () => {
    const logger = jest.fn();
    const loggingDriver = Object.create(BigQueryDriverOpen.prototype) as BigQueryDriverOpen;
    loggingDriver.setLogger(logger);

    const validTags = Object.fromEntries(Array.from({ length: 64 }, (_, i) => [`k${i}`, 'v']));
    loggingDriver.buildQueryLabels({ requestId: 'abc', queryTags: { '1st_party': 'a', ...validTags } });

    expect(logger.mock.calls).toEqual([
      ['Query Tag Dropped', { key: '1st_party', reason: 'invalid_key', requestId: 'abc' }],
      ['Query Tag Dropped', { key: 'k63', reason: 'label_limit', requestId: 'abc' }],
    ]);
  });

  test('keeps at most 63 tags, so cube_request_id fits into the 64 labels of a job', () => {
    const queryTags = Object.fromEntries(Array.from({ length: 70 }, (_, i) => [`k${i}`, 'v']));
    const labels = buildQueryLabels({ requestId: 'abc', queryTags });

    expect(Object.keys(labels || {})).toHaveLength(64);
    expect(labels?.cube_request_id).toBe('abc');
  });

  test('keeps cube_request_id when a query tag uses the same key', () => {
    expect(buildQueryLabels({ requestId: 'abc', queryTags: { cube_request_id: 'spoofed' } })).toEqual({
      cube_request_id: 'abc',
    });
  });
});
