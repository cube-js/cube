import { PostgresQuery } from '../../src';
import { CompilerCache } from '../../src/compiler/CompilerCache';
import { prepareJsCompiler } from './PrepareCompiler';
import { createCubeSchema } from './utils';

// Daylight saving time ends in America/Los_Angeles at 2026-11-01T09:00:00Z: -07:00 becomes -08:00
const BEFORE_DST_END = new Date('2026-11-01T08:59:30.000Z');
const AFTER_DST_END = new Date('2026-11-01T09:00:30.000Z');
const PDT = -7 * 3600;
const PST = -8 * 3600;

describe('Refresh key UTC offset across a DST change', () => {
  const compilers = prepareJsCompiler(
    createCubeSchema({
      name: 'cards',
      preAggregations: `
        hourly: {
          type: 'rollup',
          measureReferences: [count],
          timeDimensionReference: createdAt,
          granularity: 'day',
          refreshKey: {
            every: '1 hour',
          },
        },
        daily: {
          type: 'rollup',
          measureReferences: [sum],
          timeDimensionReference: createdAt,
          granularity: 'day',
          refreshKey: {
            every: '0 0 * * *',
            timezone: 'America/Los_Angeles',
          },
        },
      `
    })
  );

  beforeAll(async () => {
    await compilers.compile();
  });

  beforeEach(() => {
    // Only the clock is faked: the compiler's own timers keep working
    jest.useFakeTimers({
      doNotFake: [
        'hrtime', 'nextTick', 'performance', 'queueMicrotask', 'requestAnimationFrame', 'cancelAnimationFrame',
        'requestIdleCallback', 'cancelIdleCallback', 'setImmediate', 'clearImmediate', 'setInterval',
        'clearInterval', 'setTimeout', 'clearTimeout',
      ],
    });
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  // A refresh worker plans the same query over and over, so its compiler query cache entry never expires
  const invalidateKeySql = (measure: string, timezone: string): string => {
    const query = compilers.createQuery(PostgresQuery, {
      measures: [measure],
      timeDimensions: [{ dimension: 'cards.createdAt', granularity: 'day' }],
      timezone,
    });
    const [preAggregation]: any[] = query.newPreAggregations().preAggregationsDescription();
    return preAggregation.invalidateKeyQueries[0][0];
  };

  it('regenerates an interval refresh key when the query time zone changes its offset', () => {
    jest.setSystemTime(BEFORE_DST_END);
    expect(invalidateKeySql('cards.count', 'America/Los_Angeles'))
      .toEqual(`SELECT FLOOR((${PDT} + EXTRACT(EPOCH FROM NOW())) / 3600) as refresh_key`);

    jest.setSystemTime(AFTER_DST_END);
    expect(invalidateKeySql('cards.count', 'America/Los_Angeles'))
      .toEqual(`SELECT FLOOR((${PST} + EXTRACT(EPOCH FROM NOW())) / 3600) as refresh_key`);
  });

  it('regenerates a cron refresh key when its own time zone changes its offset', () => {
    jest.setSystemTime(BEFORE_DST_END);
    expect(invalidateKeySql('cards.sum', 'UTC'))
      .toEqual(`SELECT FLOOR((${PDT} + EXTRACT(EPOCH FROM NOW()) - 0) / 86400) as refresh_key`);

    jest.setSystemTime(AFTER_DST_END);
    expect(invalidateKeySql('cards.sum', 'UTC'))
      .toEqual(`SELECT FLOOR((${PST} + EXTRACT(EPOCH FROM NOW()) - 0) / 86400) as refresh_key`);
  });

  it('keeps reusing a query cache entry while the offsets stay the same', () => {
    jest.setSystemTime(BEFORE_DST_END);
    const cache = new CompilerCache({});
    const untracked = cache.getQueryCache({ query: 1 });
    // A zone without DST leaves existing entries alone
    cache.trackUtcOffset('UTC');
    expect(cache.getQueryCache({ query: 1 })).toBe(untracked);

    cache.trackUtcOffset('America/Los_Angeles');
    const entry = cache.getQueryCache({ query: 1 });
    expect(cache.getQueryCache({ query: 1 })).toBe(entry);

    jest.setSystemTime(AFTER_DST_END);
    const afterDstEnd = cache.getQueryCache({ query: 1 });
    expect(afterDstEnd).not.toBe(entry);
    expect(cache.getQueryCache({ query: 1 })).toBe(afterDstEnd);
  });
});
