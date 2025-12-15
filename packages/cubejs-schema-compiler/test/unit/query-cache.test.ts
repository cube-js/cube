import { fastComputeCacheKey, QueryCache } from '../../src/adapter/QueryCache';

describe('QueryCache', () => {
  let cache: QueryCache;

  beforeEach(() => {
    cache = new QueryCache();
  });

  it('caches function result', () => {
    let callCount = 0;
    const fn = () => {
      callCount++;
      return 'result';
    };

    const result1 = cache.cache(['key1'], fn);
    const result2 = cache.cache(['key1'], fn);

    expect(result1).toBe('result');
    expect(result2).toBe('result');
    expect(callCount).toBe(1);
  });

  it('differentiates between different keys', () => {
    let callCount = 0;
    const fn = () => {
      callCount++;
      return `result-${callCount}`;
    };

    const result1 = cache.cache(['key1'], fn);
    const result2 = cache.cache(['key2'], fn);

    expect(result1).toBe('result-1');
    expect(result2).toBe('result-2');
    expect(callCount).toBe(2);
  });

  it('fastComputeCacheKey', () => {
    expect(fastComputeCacheKey([])).toBe('');
    expect(fastComputeCacheKey(['hello'])).toBe('hello');
    expect(fastComputeCacheKey(['hello', 'world'])).toBe('hello:world');
    expect(fastComputeCacheKey(['key', 123, 'value', 456])).toBe('key:123:value:456');
    expect(fastComputeCacheKey([{ a: 1 }])).toBe('{"a":1}');
    expect(fastComputeCacheKey([
      'string',
      42,
      null,
      undefined,
      { obj: 'value' },
      [1, 2],
      true
    ])).toBe('string:42:null:u:{"obj":"value"}:[1,2]:true');
  });
});
