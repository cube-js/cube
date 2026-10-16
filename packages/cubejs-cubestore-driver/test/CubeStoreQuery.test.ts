import { PostgresQuery, prepareCompiler } from '@cubejs-backend/schema-compiler';

import { CubeStoreQuery } from '../src/CubeStoreQuery';

const prepareJsCompiler = (content: string) => prepareCompiler({
  localPath: () => __dirname,
  dataSchemaFiles: () => Promise.resolve([{ fileName: 'main.js', content }]),
}, { adapter: 'postgres' });

// The interval helpers read no instance state, so a bare prototype is enough.
const bareQuery = (): any => Object.create(CubeStoreQuery.prototype);

describe('CubeStoreQuery', () => {
  describe('intervals', () => {
    it.each([
      ['3 month', 'DATE_SUB(x, INTERVAL \'3 MONTH\')'],
      ['3 month 14 day', 'DATE_SUB(x, INTERVAL \'3 MONTH 14 DAY\')'],
      ['14 day 6 hour', 'DATE_SUB(x, INTERVAL \'14 DAY 6 HOUR\')'],
    ])('subtracts %s', (interval, sql) => {
      expect(bareQuery().subtractInterval('x', interval)).toEqual(sql);
    });

    it('has no spelling for a sub-second interval', () => {
      expect(() => bareQuery().subtractInterval('x', '5 millisecond'))
        .toThrow(/Cannot transform interval expression/);
    });
  });

  describe('DATE_BIN', () => {
    // DATE_BIN takes a month component or a day/time one, never both, unlike DATE_ADD / DATE_SUB.
    it.each(['1 month 15 days', '3 month 3 days 3 hours'])('rejects the mixed interval %s', interval => {
      expect(() => bareQuery().dateBin(interval, 'src', '2024-01-01'))
        .toThrow(/Cannot transform interval expression/);
    });

    it.each(['6 months', '1 year', '2 weeks', '15 minutes', '1 week 2 day'])('bins by %s', interval => {
      expect(bareQuery().dateBin(interval, 'src', '2024-01-01'))
        .toContain('DATE_BIN(INTERVAL');
    });
  });

  describe('LIKE filter wildcard escaping', () => {
    const compilers = prepareJsCompiler(`
      cube('Names', {
        sql: \`SELECT 1 AS id, 'a' AS name\`,
        measures: {
          count: { type: 'count' }
        },
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          name: { sql: 'name', type: 'string' }
        }
      });
    `);

    // Cube Store's LIKE treats backslash as the escape character, and its parser rejects
    // `ESCAPE '\\'` outright, so an explicit clause would break every pre-aggregation query.
    it.each([['legacy', false], ['tesseract', true]])('%s planner escapes the value and emits no clause', async (_name, useNativeSqlPlanner) => {
      await compilers.compiler.compile();

      const query = new CubeStoreQuery(compilers, {
        dimensions: ['Names.id'],
        filters: [{ member: 'Names.name', operator: 'contains', values: ['%'] }],
        useNativeSqlPlanner,
      });
      const [sql, params] = query.buildSqlAndParams();

      expect(params).toEqual(['\\%']);
      expect(sql).not.toMatch(/ESCAPE/i);
    });
  });

  describe('external pre-aggregation SQL', () => {
    const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
      cube('sales', {
        sql: 'select * from public.sales',
        measures: {
          count: { type: 'count' },
        },
        dimensions: {
          soldAt: { sql: 'sold_at', type: 'time' },
        },
        preAggregations: {
          byDay: {
            measures: [CUBE.count],
            dimensions: [CUBE.soldAt],
          },
        },
      });
    `);

    beforeAll(() => compiler.compile());

    // A source dialect may cast both sides of a time dimension IN list; over a rollup the list
    // has to be rendered by Cube Store instead.
    it.each([['legacy', false], ['tesseract', true]])('%s planner leaves a time dimension list as is', (_name, useNativeSqlPlanner) => {
      const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
        measures: ['sales.count'],
        dimensions: ['sales.soldAt'],
        filters: [{ member: 'sales.soldAt', operator: 'in', values: ['2026-06-24T00:00:00.000Z', '2026-06-25T00:00:00.000Z'] }],
        timezone: 'UTC',
        useNativeSqlPlanner,
        externalQueryClass: CubeStoreQuery,
      });
      const [sql] = query.buildSqlAndParams();

      expect(sql).toContain('sales_by_day');
      expect(sql).toMatch(/sales__sold_at[`"] IN \(\?, \?\)/);
      expect(sql).not.toMatch(/cast\(/i);
    });
  });

  describe('custom granularities', () => {
    const compilers = prepareJsCompiler(`
      cube('orders', {
        sql: 'select * from orders',
        dimensions: {
          id: { sql: 'id', type: 'number', primaryKey: true },
          createdAt: {
            sql: 'created_at',
            type: 'time',
            granularities: {
              half_year: { interval: '6 months' },
              half_year_by_1st_april: { interval: '6 months', offset: '3 months' },
              half_year_by_1st_june: { interval: '6 months', origin: '2020-06-01 10:00:00' },
            }
          },
        },
        measures: {
          count: { type: 'count' },
          rollingCountByTrailing2Day: { type: 'count', rollingWindow: { trailing: '2 day' } },
          rollingCountByLeading2Day: { type: 'count', rollingWindow: { leading: '3 day' } },
        },
      });
    `);

    const granularities = ['half_year', 'half_year_by_1st_april', 'half_year_by_1st_june'];
    const expectations: [string, string, string][] = [
      ['orders.count', 'DATE_BIN(INTERVAL', 'interval \'6 month\''],
      ['orders.rollingCountByTrailing2Day', 'date_trunc(\'day\'', 'interval \'2 day\''],
      ['orders.rollingCountByLeading2Day', 'date_trunc(\'day\'', 'interval \'3 day\''],
    ];

    beforeAll(async () => {
      await compilers.compiler.compile();
    });

    describe.each(expectations)('measure "%s"', (measure, fragment, interval) => {
      it.each(granularities)('granularity "%s"', granularity => {
        const query = new CubeStoreQuery(compilers, {
          measures: [measure],
          timeDimensions: [{
            dimension: 'orders.createdAt',
            granularity,
            dateRange: ['2020-01-01', '2021-12-31'],
          }],
          timezone: 'Europe/Kyiv',
        });
        const [sql] = query.buildSqlAndParams();

        expect(sql).toContain(fragment);
        expect(sql.toLowerCase()).toContain(interval);
      });
    });
  });
});
