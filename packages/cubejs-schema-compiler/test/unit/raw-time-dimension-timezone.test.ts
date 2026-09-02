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

async function queryFor(options = {}) {
  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(model);
  await compiler.compile();

  return new BigqueryQuery({ joinGraph, cubeEvaluator, compiler }, {
    timezone: TIMEZONE,
    convertTzForRawTimeDimension: true,
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

describe('raw time dimension timezone conversion', () => {
  it('converts a cube time dimension once', async () => {
    const query = await queryFor({
      measures: ['orders.count'],
      dimensions: ['orders.createdAt'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(1);
  });

  it('converts a view time dimension once', async () => {
    const query = await queryFor({
      measures: ['ordersView.count'],
      dimensions: ['ordersView.createdAt'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(1);
  });

  it('converts a view time dimension with granularity once', async () => {
    const query = await queryFor({
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
    const query = await queryFor({
      measures: ['ordersView.count'],
      dimensions: ['ordersView.createdAt', 'ordersView.updatedAt'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(2);
  });

  // A dimension composed out of another member is not read from a column of
  // its own, so there is nothing to convert at this level.
  it('does not convert a dimension composed from a non-time member', async () => {
    const query = await queryFor({
      measures: ['ordersView.count'],
      dimensions: ['ordersView.fromEpoch'],
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(0);
  });

  // Filter bounds are normalized to the database timezone instead, so the
  // filtered column is compared as it is stored.
  it('does not convert a filtered column', async () => {
    const query = await queryFor({
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
    const query = await queryFor({
      measures: ['ordersView.count'],
      dimensions: ['ordersView.createdAt'],
      convertTzForRawTimeDimension: false,
    });
    const [sql] = query.buildSqlAndParams();

    expect(conversionsCount(sql)).toEqual(0);
  });
});
