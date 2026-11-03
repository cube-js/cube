// https://github.com/cube-js/cube/issues/9638
// The batch rollup is built up to 2025-05-30T23:00:00Z. The lambda (source data)
// part must pick up everything after that, including the row at 00:30 UTC.
cube('lambda_tz', {
  sql: `
    SELECT '2025-01-15T10:23:45.000Z'::timestamp(3) AS "createdAt", '1' AS id UNION ALL
    SELECT '2025-03-10T11:34:56.000Z'::timestamp(3) AS "createdAt", '2' AS id UNION ALL
    SELECT '2025-05-20T09:10:20.000Z'::timestamp(3) AS "createdAt", '3' AS id UNION ALL
    SELECT '2025-05-30T22:30:00.000Z'::timestamp(3) AS "createdAt", '4' AS id UNION ALL
    SELECT '2025-05-31T00:30:00.000Z'::timestamp(3) AS "createdAt", '5' AS id
  `,
  measures: {
    count: { type: 'count' },
  },
  dimensions: {
    id: { sql: `${CUBE}.id`, type: 'string', primaryKey: true },
    createdAt: { sql: `${CUBE}."createdAt"`, type: 'time' },
  },
  preAggregations: {
    lambda: {
      type: 'rollup_lambda',
      union_with_source_data: true,
      rollups: [CUBE.main],
    },
    main: {
      measures: [CUBE.count],
      dimensions: [CUBE.id],
      timeDimension: CUBE.createdAt,
      granularity: 'hour',
      partitionGranularity: 'month',
      buildRangeStart: { sql: `SELECT '2025-01-01T00:00:00.000Z'::timestamp` },
      buildRangeEnd: { sql: `SELECT '2025-05-30T23:00:00.000Z'::timestamp` },
    },
  },
});
