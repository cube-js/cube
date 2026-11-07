// Blue/green pre-aggregations: two Cube Store clusters take turns.
//
// The ETL loads one business day at a time and records it in `etl_runs` when it
// finishes. Blue rebuilds only after the ETL of an even day, green only after the
// ETL of an odd one, so each cluster refreshes once every two days and keeps a
// fixed snapshot while the other one rebuilds. Users query the cluster whose
// snapshot is complete (`cube_store_id` in the security context) and move to the
// other one once it has rebuilt.
//
// A day's parity counts days since the epoch rather than the day of the month,
// so the clusters keep alternating across month ends (the 31st and the 1st are
// both odd days of the month).
const DAY_PARITY_BY_CUBE_STORE = { blue: 0, green: 1 };

const cubeStoreId = COMPILE_CONTEXT.securityContext.cube_store_id;
const dayParity = DAY_PARITY_BY_CUBE_STORE[cubeStoreId];

if (dayParity === undefined) {
  throw new Error(`Unknown cube_store_id: ${cubeStoreId}`);
}

// The last business day of this cluster's parity whose ETL has finished. It
// changes once every two days, right after that ETL completes, which makes it
// both the refresh key and the cut-off of the data this cluster serves. The
// refresh worker checks it every 10 seconds, which bounds how long after the
// ETL the rebuild starts.
const watermarkSql = `
  SELECT MAX(business_date)
  FROM public.etl_runs
  WHERE finished_at IS NOT NULL
    AND MOD(business_date - DATE '1970-01-01', 2) = ${dayParity}
`;

cube(`orders`, {
  // Rows past the watermark belong to the other cluster's snapshot, or to an ETL
  // still running: leaving them out keeps every pre-aggregation and partition of
  // this cluster at the same business day, whenever it happens to be built.
  sql: `SELECT * FROM public.orders WHERE business_date <= (${watermarkSql})`,

  measures: {
    count: {
      type: `count`,
    },

    total_amount: {
      sql: `amount`,
      type: `sum`,
    },

    last_business_date: {
      sql: `business_date`,
      type: `max`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primary_key: true,
    },

    status: {
      sql: `status`,
      type: `string`,
    },

    business_date: {
      sql: `business_date`,
      type: `time`,
    },
  },

  pre_aggregations: {
    totals: {
      measures: [CUBE.count, CUBE.total_amount, CUBE.last_business_date],
      scheduled_refresh: true,
      refresh_key: {
        sql: `${watermarkSql}`,
        every: `10 seconds`,
      },
    },

    daily: {
      measures: [CUBE.count, CUBE.total_amount, CUBE.last_business_date],
      dimensions: [CUBE.status],
      time_dimension: CUBE.business_date,
      granularity: `day`,
      partition_granularity: `month`,
      scheduled_refresh: true,
      refresh_key: {
        sql: `${watermarkSql}`,
        every: `10 seconds`,
      },
    },
  },
});
