// cube-js/cube#8745: trailing-unbounded rolling window, query without granularity.
// `OrderNoPa` is an identical cube without pre-aggregations (baseline).
const ORDER_SQL = `
    SELECT 1 AS id, '2023-07-01T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 2 AS id, '2023-07-05T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 3 AS id, '2023-07-11T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 4 AS id, '2023-07-15T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 5 AS id, '2023-07-21T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 6 AS id, '2023-07-25T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 7 AS id, '2023-07-29T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 8 AS id, '2023-08-02T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 9 AS id, '2023-08-06T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 10 AS id, '2023-08-12T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 11 AS id, '2023-08-16T00:00:00.000Z'::TIMESTAMP AS created_at UNION ALL
    SELECT 12 AS id, '2023-08-22T00:00:00.000Z'::TIMESTAMP AS created_at
  `;
cube(`Order`, {
  sql: ORDER_SQL,
  measures: {
    rollingCount: { sql: `id`, type: `count`, rollingWindow: { trailing: `unbounded` } },
  },
  dimensions: {
    createdAt: { sql: `created_at`, type: `time` },
  },
  preAggregations: {
    totalByDay: {
      measures: [Order.rollingCount],
      timeDimension: Order.createdAt,
      granularity: `day`,
      refreshKey: { every: `1 minute` },
    },
  }
});
cube(`OrderNoPa`, {
  sql: ORDER_SQL,
  measures: {
    rollingCount: { sql: `id`, type: `count`, rollingWindow: { trailing: `unbounded` } },
  },
  dimensions: {
    createdAt: { sql: `created_at`, type: `time` },
  },
});
