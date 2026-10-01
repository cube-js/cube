import { prepareYamlCompiler } from '../../unit/PrepareCompiler';
import { ClickHouseQuery } from '../../../src/adapter/ClickHouseQuery';
import { ClickHouseDbRunner } from './ClickHouseDbRunner';

// https://github.com/cube-js/cube/issues/10326
// Time filters are cast with `parseDateTimeBestEffort`, which returns a second-precision
// `DateTime`. A range ending at `23:59:59.999` becomes `23:59:59`, so a `DateTime64` row at
// `23:59:59.500` drops out of the last day of the range (and out of every partition built
// with such an upper bound). On a running Cube v1.7.48 + ClickHouse 24.8, a `dateRange` of
// a single day returns 1 instead of 11.
describe('Issue #10326: ClickHouse time filters lose millisecond precision', () => {
  jest.setTimeout(200000);

  const dbRunner = new ClickHouseDbRunner();

  const noDataSet = async () => Promise.resolve();

  afterAll(async () => {
    await dbRunner.tearDown();
  });

  const compilers = prepareYamlCompiler(`
cubes:
  - name: events
    sql: >
      SELECT 1 AS id, toDateTime64('2026-01-20 10:00:00.000', 3, 'UTC') AS ts, 1 AS amount
      UNION ALL
      SELECT 2 AS id, toDateTime64('2026-01-20 23:59:59.500', 3, 'UTC') AS ts, 10 AS amount
      UNION ALL
      SELECT 3 AS id, toDateTime64('2026-01-21 00:00:00.000', 3, 'UTC') AS ts, 100 AS amount
    dimensions:
      - name: id
        sql: id
        type: number
        primary_key: true
      - name: ts
        sql: ts
        type: time
    measures:
      - name: amount
        sql: amount
        type: sum
`);

  beforeAll(async () => {
    await compilers.compiler.compile();
  });

  it('keeps rows in the last millisecond-precision second of a date range', async () => {
    const query = new ClickHouseQuery(compilers, {
      measures: ['events.amount'],
      timeDimensions: [{
        dimension: 'events.ts',
        dateRange: ['2026-01-20', '2026-01-20'],
      }],
      timezone: 'UTC',
    });

    const [row] = await dbRunner.testQuery(query.buildSqlAndParams(), noDataSet);

    expect(Number(row.events__amount)).toEqual(11);
  });
});
