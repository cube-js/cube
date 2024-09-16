cube(`SegmentTest`, {
  sql: `
  SELECT 123 AS value, 1 AS segment UNION ALL
  SELECT 456 AS value, 1 AS segment UNION ALL
  SELECT 789 AS value, 2 AS segment UNION ALL
  SELECT 987 AS value, 2 AS segment
  `,
  measures: {
    count_distinct: {
      sql: 'value',
      type: 'count_distinct',
    },
  },
  dimensions: {
    value: {
      sql: `value`,
      type: `number`,
      primaryKey: true,
      public: true,
    },
  },
  segments: {
    segment_eq_1: {
      sql: "(segment = 1)",
    },
  },
});
