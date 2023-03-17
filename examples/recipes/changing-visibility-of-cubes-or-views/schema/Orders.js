cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  shown: false,

  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started
    main: {
      dimensions: [Users.company],
      measures: [CUBE.count],
    },
  },

  joins: {
    Users: {
      sql: `${CUBE}.user_id = ${Users}.id`,
      relationship: `belongsTo`
    }
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },

    totalRevenue: {
      sql: `number`,
      type: `sum`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    status: {
      sql: `status`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    }
  },

  dataSource: `default`
});
