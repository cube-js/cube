// cube-js/cube#8580: rolling window with `offset: start` served from a pre-aggregation.
// `order_rolling_nopa` is an identical cube without pre-aggregations (baseline).
const OR_SQL = `
      SELECT 10 AS value, '2023-11-01'::TIMESTAMP AS date UNION ALL
      SELECT 10 AS value, '2023-12-01'::TIMESTAMP AS date UNION ALL
      SELECT 10 AS value, '2024-01-01'::TIMESTAMP AS date UNION ALL
      SELECT 30 AS value, '2024-02-01'::TIMESTAMP AS date UNION ALL
      SELECT 50 AS value, '2024-03-01'::TIMESTAMP AS date UNION ALL
      SELECT 80 AS value, '2024-04-01'::TIMESTAMP AS date
      `;
cube(`order_rolling`, {
  sql: OR_SQL,
  dimensions: {
    date: { sql: `date`, type: `time` },
  },
  measures: {
    current_month_sum: {
      sql: `value`,
      type: `sum`,
      rolling_window: { trailing: `3 month`, offset: `start` }
    },
  },
  preAggregations: {
    main: {
      measures: [order_rolling.current_month_sum],
      timeDimension: order_rolling.date,
      granularity: `day`,
      refresh_key: { every: `1 week` },
    }
  }
});

// identical cube without pre-aggregation for baseline
cube(`order_rolling_nopa`, {
  sql: OR_SQL,
  dimensions: { date: { sql: `date`, type: `time` } },
  measures: {
    current_month_sum: { sql: `value`, type: `sum`, rolling_window: { trailing: `3 month`, offset: `start` } },
  },
});
