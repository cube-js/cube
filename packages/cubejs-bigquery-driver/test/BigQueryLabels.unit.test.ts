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
  });

  test('replaces every disallowed character rather than dropping them', () => {
    expect(buildQueryLabels({ requestId: '....' })).toEqual({
      cube_request_id: '____',
    });
  });
});
