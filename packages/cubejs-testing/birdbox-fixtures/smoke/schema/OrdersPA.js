import { DATE_CAST } from './CAST';

// TODO(cristipp) Use test-time db type instead of hardcoded value.
const DB_TYPE = 'bigquery2';
const { DATE_PREFIX, DATE_SUFFIX } = DATE_CAST[DB_TYPE];

cube(`OrdersPA`, {
  sql: `
    select 1 as id, 100 as amount, 'new' as status, ${DATE_PREFIX}'2020-01-10'${DATE_SUFFIX} as date
    UNION ALL
    select 2 as id, 200 as amount, 'new' as status, ${DATE_PREFIX}'2020-01-10'${DATE_SUFFIX} as date
    UNION ALL
    select 3 as id, 300 as amount, 'processed' as status, ${DATE_PREFIX}'2020-01-10'${DATE_SUFFIX} as date
    UNION ALL
    select 4 as id, 500 as amount, 'processed' as status, ${DATE_PREFIX}'2020-01-10'${DATE_SUFFIX} as date
    UNION ALL
    select 5 as id, 600 as amount, 'shipped' as status, ${DATE_PREFIX}'2020-01-10'${DATE_SUFFIX} as date
  `,

  preAggregations: {
    orderStatus: {
      measures: [CUBE.totalAmount],
      dimensions: [CUBE.status],
      timeDimension: CUBE.date,
      granularity: 'day',
      partitionGranularity: 'month',
      indexes: {
        categoryIndex: {
          columns: [CUBE.status],
        },
      },
      // buildRangeStart: {
      //   sql: `SELECT DATE('2020-01-01')`,
      // },
      // buildRangeEnd: {
      //   sql: `SELECT DATE('2021-01-01')`,
      // },
      refreshKey: {
        every: `1 hour`,
      }
    },
  },

  measures: {
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    status: {
      sql: `status`,
      type: `string`,
    },

    date: {
      sql: `date`,
      type: `time`,
    }
  },
});
