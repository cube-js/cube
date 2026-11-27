// Two cubes with rollups stored in the source database, for SQL API queries that
// join one cube with a grouped sub-query over the other
cube(`SubQueryRollupOrders`, {
  sql: `
  select 1 as id, 'new' as status
  UNION ALL
  select 2 as id, 'processed' as status
  UNION ALL
  select 3 as id, 'shipped' as status
  `,
  measures: {
    count: {
      type: `count`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
  },
  preAggregations: {
    byStatus: {
      external: false,
      dimensions: [CUBE.status],
      measures: [CUBE.count],
    },
  },
});

cube(`SubQueryRollupStatuses`, {
  sql: `
  select 1 as id, 'new' as status
  UNION ALL
  select 2 as id, 'shipped' as status
  `,
  measures: {
    count: {
      type: `count`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
  },
  preAggregations: {
    byStatus: {
      external: false,
      dimensions: [CUBE.status],
      measures: [CUBE.count],
    },
  },
});
