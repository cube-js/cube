import { MAX_QUERY_TAGS, wrapQueryTagsFn } from '../../src/core/queryTags';
import { DriverContext, QueryTagsFn } from '../../src/core/types';

describe('wrapQueryTagsFn', () => {
  const context: DriverContext = { securityContext: {}, requestId: 'request-1', dataSource: 'default' };

  function wrap(queryTagsFn: QueryTagsFn) {
    const logger = jest.fn();
    return { queryTags: wrapQueryTagsFn(queryTagsFn, logger), logger };
  }

  test('passes the context to the hook and returns valid tags as they are', async () => {
    const hook = jest.fn(() => ({ user_id: 'u-42', 'tenant-name': 'Acme Corp' }));
    const { queryTags, logger } = wrap(hook);

    await expect(queryTags(context)).resolves.toEqual({ user_id: 'u-42', 'tenant-name': 'Acme Corp' });
    expect(hook).toHaveBeenCalledWith(context);
    expect(logger).not.toHaveBeenCalled();
  });

  test('stringifies tag values and drops missing ones', async () => {
    // A JS or Python hook can return anything the security context holds
    const { queryTags } = wrap(async () => ({ user_id: 42, admin: false, org: undefined, team: null }));

    await expect(queryTags(context)).resolves.toEqual({ user_id: '42', admin: 'false' });
  });

  test('returns undefined when the hook returns nothing or only missing values', async () => {
    await expect(wrap(() => undefined).queryTags(context)).resolves.toBeUndefined();
    await expect(wrap(() => ({ user_id: null })).queryTags(context)).resolves.toBeUndefined();
  });

  test('returns undefined and logs the error when the hook throws', async () => {
    const { queryTags, logger } = wrap(({ securityContext }) => ({ user_id: securityContext.missing.id }));

    await expect(queryTags(context)).resolves.toBeUndefined();
    expect(logger).toHaveBeenCalledWith('Query Tags Error', {
      requestId: 'request-1',
      error: expect.stringContaining('TypeError'),
    });
  });

  test('drops and logs keys that are not lowercase letters, digits, _ and - starting with a letter', async () => {
    const { queryTags, logger } = wrap(() => ({
      '1st_party': 'a', _tenant: 'b', '-x': 'c', '': 'd', 'User.Id': 'e', ['k'.repeat(64)]: 'f', ok: 'g',
    }));

    await expect(queryTags(context)).resolves.toEqual({ ok: 'g' });
    expect(logger.mock.calls.map(([, params]) => params.key))
      .toEqual(['1st_party', '_tenant', '-x', '', 'User.Id', 'k'.repeat(64)]);
    expect(logger).toHaveBeenCalledWith('Query Tag Dropped', {
      key: '1st_party', reason: 'invalid_key', requestId: 'request-1',
    });
  });

  test(`keeps at most ${MAX_QUERY_TAGS} tags and logs the rest`, async () => {
    const { queryTags, logger } = wrap(() => Object.fromEntries(Array.from({ length: 65 }, (_, i) => [`k${i}`, 'v'])));

    expect(Object.keys(await queryTags(context) || {})).toHaveLength(MAX_QUERY_TAGS);
    expect(logger.mock.calls).toEqual([
      ['Query Tag Dropped', { key: 'k63', reason: 'tag_limit', requestId: 'request-1' }],
      ['Query Tag Dropped', { key: 'k64', reason: 'tag_limit', requestId: 'request-1' }],
    ]);
  });
});
