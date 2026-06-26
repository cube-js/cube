import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Rolling window measures queried WITHOUT a time dimension granularity (only a
// dateRange). The window is anchored by `offset`: 'start' anchors at the period
// start, 'end' anchors at the period end. With no granularity the result is a
// single aggregate row.
//
// The seed visitors table has one row dated 2016-09-07 (before the queried
// ranges) which distinguishes offset:'start' (accumulate everything before the
// period start) from offset:'end' (accumulate everything up to the period end).
describe('Rolling window offset without granularity', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
    cube(\`balances\`, {
      sql: \`select * from visitors\`,

      measures: {
        // trailing: 'unbounded'
        begBalance: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { trailing: 'unbounded', offset: 'start' }
        },
        endBalance: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { trailing: 'unbounded', offset: 'end' }
        },

        // leading: 'unbounded'
        leadingStart: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { leading: 'unbounded', offset: 'start' }
        },
        leadingEnd: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { leading: 'unbounded', offset: 'end' }
        },

        // finite trailing interval
        trailing5Start: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { trailing: '5 day', offset: 'start' }
        },
        trailing5End: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { trailing: '5 day', offset: 'end' }
        },
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      },
    })

    cube(\`balances_fp\`, {
      sql: \`select * from visitors WHERE \${FILTER_PARAMS.balances_fp.createdAt.filter('created_at')}\`,

      measures: {
        begBalance: {
          type: 'sum',
          sql: 'amount',
          rollingWindow: { trailing: 'unbounded', offset: 'start' }
        },
      },

      dimensions: {
        id: {
          type: 'number',
          sql: 'id',
          primaryKey: true
        },
        createdAt: {
          type: 'time',
          sql: 'created_at'
        },
      },
    })
  `);

  const runQuery = async (measures: string[], dateRange: [string, string]) => {
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures,
        timeDimensions: [
          {
            dimension: 'balances.createdAt',
            dateRange,
          },
        ],
        timezone: 'UTC',
      }
    );

    const queryAndParams = query.buildSqlAndParams();
    return dbRunner.testQuery(queryAndParams);
  };

  it('trailing: unbounded — offset start vs end', () => compiler.compile().then(async () => {
    // beg: amount where created_at < 2017-01-01  -> only the 2016-09-07 row (500)
    // end: amount where created_at <= 2017-01-30 -> all rows (2000)
    expect(await runQuery(
      ['balances.begBalance', 'balances.endBalance'],
      ['2017-01-01', '2017-01-30']
    )).toEqual([
      { balances__beg_balance: '500', balances__end_balance: '2000' },
    ]);
  }));

  it('leading: unbounded — offset start vs end', () => compiler.compile().then(async () => {
    // start: amount where created_at >= 2017-01-01 -> all 2017 rows in/after range (1500)
    // end:   amount where created_at >  2017-01-30 -> none (null)
    expect(await runQuery(
      ['balances.leadingStart', 'balances.leadingEnd'],
      ['2017-01-01', '2017-01-30']
    )).toEqual([
      { balances__leading_start: '1500', balances__leading_end: null },
    ]);
  }));

  it('finite trailing interval — offset start vs end', () => compiler.compile().then(async () => {
    // range 2017-01-06 .. 2017-01-10
    // start: created_at in [from - 5d, from)  = [2017-01-01, 2017-01-06) -> 100 + 200 = 300
    // end:   created_at in (to - 5d, to]      = (2017-01-05, 2017-01-10] -> 300 + 400 + 500 = 1200
    expect(await runQuery(
      ['balances.trailing5Start', 'balances.trailing5End'],
      ['2017-01-06', '2017-01-10']
    )).toEqual([
      { balances__trailing5_start: '300', balances__trailing5_end: '1200' },
    ]);
  }));

  // FILTER_PARAMS on the time dimension must receive the date-range bounds, not
  // the rolling window config — the window's trailing/leading/offset must never
  // leak into the filter as query parameters.
  it('FILTER_PARAMS does not leak rolling window config into params', () => compiler.compile().then(async () => {
    const query = new PostgresQuery(
      { joinGraph, cubeEvaluator, compiler },
      {
        measures: ['balances_fp.begBalance'],
        timeDimensions: [
          {
            dimension: 'balances_fp.createdAt',
            dateRange: ['2017-01-01', '2017-01-30'],
          },
        ],
        timezone: 'UTC',
      }
    );

    const [, params] = query.buildSqlAndParams();
    expect(params).not.toContain('unbounded');
    expect(params).not.toContain('start');
    expect(params).not.toContain('end');

    // Sanity check: the query still executes.
    await dbRunner.testQuery(query.buildSqlAndParams());
  }));
});
