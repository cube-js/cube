// A small cube with a rollup, used to check that SQL API streaming
// works for queries that are served from a Cube Store pre-aggregation.
cube(`StreamPreAggTest`, {
  sql: `
  SELECT 1 AS id, CAST('2026-09-01' AS TIMESTAMP) AS event_date, 10 AS sessions UNION ALL
  SELECT 2 AS id, CAST('2026-09-09' AS TIMESTAMP) AS event_date, 20 AS sessions UNION ALL
  SELECT 3 AS id, CAST('2026-09-16' AS TIMESTAMP) AS event_date, 30 AS sessions
  `,
  measures: {
    sessions: {
      sql: 'sessions',
      type: 'sum',
    },
  },
  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primaryKey: true,
    },
    event_date: {
      sql: 'event_date',
      type: 'time',
    },
  },
  preAggregations: {
    daily: {
      type: 'rollup',
      measures: [sessions],
      timeDimension: event_date,
      granularity: 'day',
    },
  },
});
