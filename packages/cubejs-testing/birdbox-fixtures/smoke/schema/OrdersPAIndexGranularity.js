cube(`OrdersPAIndexGranularity`, {
  sql: `
    select 1 as id, 100 as amount, 'new' status, '2024-01-01T10:00:00'::timestamp as created_at
    UNION ALL
    select 2 as id, 200 as amount, 'new' status, '2024-01-01T11:00:00'::timestamp as created_at
    UNION ALL
    select 3 as id, 300 as amount, 'processed' status, '2024-01-02T10:00:00'::timestamp as created_at
    UNION ALL
    select 4 as id, 500 as amount, 'processed' status, '2024-01-02T11:00:00'::timestamp as created_at
    UNION ALL
    select 5 as id, 600 as amount, 'shipped' status, '2024-01-03T10:00:00'::timestamp as created_at
  `,

  preAggregations: {
    ordersByDay: {
      measures: [CUBE.count, CUBE.totalAmount],
      dimensions: [CUBE.status],
      timeDimension: CUBE.createdAt,
      granularity: `day`,
      indexes: {
        time_index: {
          columns: [CUBE.createdAt.day],
        },
        time_and_status_index: {
          columns: [CUBE.createdAt.day, CUBE.status],
        },
      },
      refreshKey: {
        every: `1 hour`,
      },
    },
  },

  measures: {
    count: {
      type: `count`,
    },

    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
    },

    status: {
      sql: `status`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
