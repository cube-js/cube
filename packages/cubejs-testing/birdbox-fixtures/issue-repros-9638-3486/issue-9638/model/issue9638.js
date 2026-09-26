cube(`Issue9638`, {
  sql: `SELECT id, created_at AS "createdAt" FROM public.issue_9638_events`,
  measures: { count: { type: `count` } },
  dimensions: {
    createdAt: { sql: `${CUBE}."createdAt"`, type: `time` },
    id: { sql: `${CUBE}.id`, type: `string`, primaryKey: true, shown: true },
  },
  preAggregations: {
    lambda: { type: `rollup_lambda`, union_with_source_data: true, rollups: [CUBE.main] },
    main: {
      measures: [CUBE.count],
      dimensions: [CUBE.id, CUBE.createdAt],
      timeDimension: CUBE.createdAt,
      granularity: `hour`,
      partitionGranularity: `month`,
      refreshKey: { every: `55 minutes` },
    },
  },
});
