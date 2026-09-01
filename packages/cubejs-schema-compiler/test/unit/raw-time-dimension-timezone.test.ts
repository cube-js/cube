/* eslint-disable no-restricted-syntax */
import { BigqueryQuery } from '../../src/adapter/BigqueryQuery';
import { prepareJsCompiler } from './PrepareCompiler';

// A `type: time` dimension asked for without a granularity is converted into the
// query timezone once. A view re-exposing that dimension is not an extra
// conversion site: the value is read (and converted) on the owning cube.
const model = [
  'cube(\'orders\', {',
  '  sql: `SELECT * FROM orders`,',
  '  measures: {',
  '    count: {',
  '      type: `count`',
  '    }',
  '  },',
  '  dimensions: {',
  '    id: {',
  '      sql: `id`,',
  '      type: `number`,',
  '      primaryKey: true',
  '    },',
  '    createdAt: {',
  '      sql: `created_at`,',
  '      type: `time`',
  '    },',
  '    updatedAt: {',
  '      sql: `updated_at`,',
  '      type: `time`',
  '    },',
  '    epochSeconds: {',
  '      sql: `epoch_seconds`,',
  '      type: `number`',
  '    },',
  // Builds a timestamp out of a non-time member: the value is composed here,
  // not read from a column of this cube.
  '    fromEpoch: {',
  // eslint-disable-next-line no-template-curly-in-string
  '      sql: `TIMESTAMP_SECONDS(${CUBE.epochSeconds})`,',
  '      type: `time`',
  '    }',
  '  }',
  '});',
  '',
  'view(\'ordersView\', {',
  '  cubes: [{',
  '    joinPath: orders,',
  '    includes: [`count`, `createdAt`, `updatedAt`, `fromEpoch`]',
  '  }]',
  '});',
].join('\n');

const TIMEZONE = 'America/Chicago';

async function queryFor(useNativeSqlPlanner: boolean, options = {}) {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
  await compiler.compile();

  return new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
    timezone: TIMEZONE,
    convertTzForRawTimeDimension: true,
    useNativeSqlPlanner,
    ...options,
  });
}

// BigqueryQuery.convertTz wraps the field in `TIMESTAMP(DATETIME(field, '<tz>'))`,
// so the timezone literal appears once per conversion applied to the column.
// Counting the literal rather than matching the wrapper catches nested wrapping,
// where the outer call is no longer a flat `DATETIME(<column>, '<tz>')`.
function conversionsCount(sql: string) {
  return (sql.match(new RegExp(`'${TIMEZONE}'`, 'g')) || []).length;
}

describe.each([
  ['legacy planner', false],
  ['native planner', true],
])('raw time dimension timezone conversion (%s)', (_name, useNativeSqlPlanner) => {
  it('converts a cube time dimension once', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['orders.count'],
      dimensions: ['orders.createdAt'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(1);
  });

  it('converts a view time dimension once', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['ordersView.count'],
      dimensions: ['ordersView.createdAt'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(1);
  });

  it('converts a view time dimension with granularity once', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['ordersView.count'],
      timeDimensions: [{
        dimension: 'ordersView.createdAt',
        granularity: 'day',
      }],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(1);
  });

  it('converts each of two view time dimensions once', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['ordersView.count'],
      dimensions: ['ordersView.createdAt', 'ordersView.updatedAt'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(2);
  });

  // A dimension composed out of another member is not read from a column of
  // its own, so there is nothing to convert at this level.
  it('does not convert a dimension composed from a non-time member', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['ordersView.count'],
      dimensions: ['ordersView.fromEpoch'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(0);
  });

  // Filter bounds are normalized to the database timezone instead, so the
  // filtered column is compared as it is stored.
  it('does not convert a filtered column', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['ordersView.count'],
      filters: [{
        member: 'ordersView.createdAt',
        operator: 'afterDate',
        values: ['2026-08-04'],
      }],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(0);
  });

  it('does not convert without a timezone conversion request', async () => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['ordersView.count'],
      dimensions: ['ordersView.createdAt'],
      convertTzForRawTimeDimension: false,
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(0);
  });
});

// A bare date in a filter value is resolved in the query timezone, so what the
// query binds is the UTC instant of a boundary of that day in America/Chicago
// (UTC-5 in August).
const DAY_START = '2026-08-04T05:00:00.000000Z';
const DAY_END = '2026-08-05T04:59:59.999999Z';

// Which boundary of the day a single-date operator binds. The two planners
// disagree on `afterDate` and `beforeOrOnDate`: one treats the date as an
// instant, the other as the closed interval of the whole day. Both are pinned
// so that a change on either side shows up here.
const SINGLE_DATE_BOUNDS: Record<string, { legacy: string, native: string }> = {
  beforeDate: { legacy: DAY_START, native: DAY_START },
  beforeOrOnDate: { legacy: DAY_START, native: DAY_END },
  afterDate: { legacy: DAY_START, native: DAY_END },
  afterOrOnDate: { legacy: DAY_START, native: DAY_START },
};

describe.each([
  ['legacy planner', false],
  ['native planner', true],
])('date filter bound values (%s)', (_name, useNativeSqlPlanner) => {
  const boundsFor = async (options) => {
    const query = await queryFor(useNativeSqlPlanner, {
      measures: ['orders.count'],
      ...options,
    });
    const [, params] = query.buildSqlAndParams();

    return params;
  };

  it('takes the date range bounds from the query timezone', async () => {
    expect(await boundsFor({
      timeDimensions: [{
        dimension: 'orders.createdAt',
        dateRange: ['2026-08-01', '2026-08-07'],
      }],
    })).toEqual(['2026-08-01T05:00:00.000000Z', '2026-08-08T04:59:59.999999Z']);
  });

  it.each(Object.keys(SINGLE_DATE_BOUNDS))('takes the %s bound from the query timezone', async (operator) => {
    const expected = SINGLE_DATE_BOUNDS[operator][useNativeSqlPlanner ? 'native' : 'legacy'];

    expect(await boundsFor({
      filters: [{
        member: 'orders.updatedAt',
        operator,
        values: ['2026-08-04'],
      }],
    })).toEqual([expected]);
  });
});
