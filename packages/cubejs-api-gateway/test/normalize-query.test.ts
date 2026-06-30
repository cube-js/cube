// eslint-disable-next-line import/no-extraneous-dependencies
import { normalizeQuery } from '../src/query';

const baseQuery = {
  measures: ['Foo.count'],
  timezone: 'UTC',
};

describe('responseFormat validation', () => {
  test.each(['default', 'compact', 'columnar'])(
    'accepts responseFormat=%s',
    (responseFormat) => {
      const result = normalizeQuery({ ...baseQuery, responseFormat }, false);
      expect(result.responseFormat).toBe(responseFormat);
    }
  );

  test('rejects unknown responseFormat', () => {
    expect(() => normalizeQuery({ ...baseQuery, responseFormat: 'arrow' }, false)).toThrow(/Invalid query format/);
  });
});
