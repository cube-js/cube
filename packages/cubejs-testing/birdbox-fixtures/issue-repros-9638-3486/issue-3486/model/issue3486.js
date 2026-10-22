cube(`Issue3486`, {
  sql: `SELECT * FROM public.issue_3486_orders`,
  measures: { count: { type: `count` } },
  dimensions: {
    id: { sql: `id`, type: `number`, primaryKey: true },
    status: { sql: `status`, type: `string` },
    createdAt: { sql: `created_at`, type: `time` },
  },
  preAggregations: {
    dup: {
      measures: [CUBE.count],
      dimensions: [CUBE.status, CUBE.status],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
    },
  },
});
