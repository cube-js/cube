import { getEnv } from '@cubejs-backend/shared';
import { PostgresQuery } from '../../../src/adapter/PostgresQuery';
import { prepareJsCompiler } from '../../unit/PrepareCompiler';
import { dbRunner } from './PostgresDBRunner';

// Multi-stage measures over a day-partitioned rollup of the base sum read days
// before the requested one. Daily amounts in `visitors`: Jan 3 = 100,
// Jan 5 = 200, Jan 6 = 300, Jan 7 = 900.
describe('PreAggregations multi-stage rolling measures over partitions', () => {
  jest.setTimeout(200000);

  const { compiler, joinGraph, cubeEvaluator } = prepareJsCompiler(`
  cube(\`visitors_ms_rolling\`, {
    sql: \`select * from visitors\`,
    sqlAlias: 'vmsr',

    measures: {
      amount: {
        sql: 'amount',
        type: 'sum'
      },
      amountRolling3Day: {
        multiStage: true,
        sql: \`\${amount}\`,
        type: 'sum',
        rollingWindow: {
          trailing: '3 day'
        }
      },
      amountMonthToDate: {
        multiStage: true,
        sql: \`\${amount}\`,
        type: 'sum',
        rollingWindow: {
          type: 'to_date',
          granularity: 'month'
        }
      },
      amountPrevDay: {
        multiStage: true,
        sql: \`\${amount}\`,
        type: 'number',
        timeShift: [{
          interval: '1 day',
          type: 'prior'
        }]
      }
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
      }
    },

    preAggregations: {
      amountByDay: {
        type: 'rollup',
        measures: [CUBE.amount],
        timeDimension: CUBE.createdAt,
        granularity: 'day',
        partitionGranularity: 'day'
      }
    }
  })

  // The rollup lists the rolling measures themselves, and the cube reads its
  // partition range through FILTER_PARAMS.
  cube(\`visitors_ms_rolling_stored\`, {
    sql: \`select * from visitors WHERE \${FILTER_PARAMS.visitors_ms_rolling_stored.createdAt.filter('created_at')}\`,
    sqlAlias: 'vmsrs',

    measures: {
      amount: {
        sql: 'amount',
        type: 'sum'
      },
      amountRolling3Day: {
        multiStage: true,
        sql: \`\${amount}\`,
        type: 'sum',
        rollingWindow: {
          trailing: '3 day'
        }
      },
      amountMonthToDate: {
        multiStage: true,
        sql: \`\${amount}\`,
        type: 'sum',
        rollingWindow: {
          type: 'to_date',
          granularity: 'month'
        }
      }
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
      }
    },

    preAggregations: {
      rollingByDay: {
        type: 'rollup',
        measures: [CUBE.amountRolling3Day, CUBE.amountMonthToDate],
        timeDimension: CUBE.createdAt,
        granularity: 'day',
        partitionGranularity: 'day'
      }
    }
  })
  `);

  const evaluate = async (measures: string[], cube = 'visitors_ms_rolling', table = 'vmsr_amount_by_day') => {
    await compiler.compile();

    const query = new PostgresQuery({ joinGraph, cubeEvaluator, compiler }, {
      measures,
      timeDimensions: [{
        dimension: `${cube}.createdAt`,
        granularity: 'day',
        dateRange: ['2017-01-07', '2017-01-07']
      }],
      timezone: 'UTC',
      preAggregationsSchema: '',
      cubestoreSupportMultistage: true,
    });

    expect(query.buildSqlAndParams()[0]).toContain(table);

    return dbRunner.evaluateQueryWithPreAggregations(query);
  };

  if (getEnv('nativeSqlPlanner')) {
    it('a rolling window reads the days before the requested one', async () => {
      const res = await evaluate(['visitors_ms_rolling.amountRolling3Day']);
      expect(res).toEqual([{
        vmsr__created_at_day: '2017-01-07T00:00:00.000Z',
        vmsr__amount_rolling3_day: '1400',
      }]);
    });

    it('a to_date window reads from the start of the period', async () => {
      const res = await evaluate(['visitors_ms_rolling.amountMonthToDate']);
      expect(res).toEqual([{
        vmsr__created_at_day: '2017-01-07T00:00:00.000Z',
        vmsr__amount_month_to_date: '1500',
      }]);
    });

    it('a time shift reads the shifted day', async () => {
      const res = await evaluate(['visitors_ms_rolling.amount', 'visitors_ms_rolling.amountPrevDay']);
      expect(res).toEqual([{
        vmsr__created_at_day: '2017-01-07T00:00:00.000Z',
        vmsr__amount: '900',
        vmsr__amount_prev_day: '300',
      }]);
    });

    it('several usages each read their own days', async () => {
      const res = await evaluate([
        'visitors_ms_rolling.amountRolling3Day',
        'visitors_ms_rolling.amountMonthToDate',
        'visitors_ms_rolling.amountPrevDay',
      ]);
      expect(res).toEqual([{
        vmsr__created_at_day: '2017-01-07T00:00:00.000Z',
        vmsr__amount_rolling3_day: '1400',
        vmsr__amount_month_to_date: '1500',
        vmsr__amount_prev_day: '300',
      }]);
    });

    it('a rollup of the rolling measures themselves builds and reads its days', async () => {
      const res = await evaluate(
        ['visitors_ms_rolling_stored.amountRolling3Day', 'visitors_ms_rolling_stored.amountMonthToDate'],
        'visitors_ms_rolling_stored',
        'vmsrs_rolling_by_day',
      );
      expect(res).toEqual([{
        vmsrs__created_at_day: '2017-01-07T00:00:00.000Z',
        vmsrs__amount_rolling3_day: '1400',
        vmsrs__amount_month_to_date: '1500',
      }]);
    });
  } else {
    it.skip('multi-stage measures over pre-aggregations need Tesseract', () => {
      // Multi-stage measures are planned by Tesseract only.
    });
  }
});
