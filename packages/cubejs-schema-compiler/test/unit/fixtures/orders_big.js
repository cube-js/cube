cube('orders', {
  sql: `SELECT * FROM orders`,

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },

    user_id: {
      sql: 'user_id',
      type: 'number',
    },

    status: {
      sql: 'status',
      type: 'string',
    },

    created_at: {
      sql: 'created_at',
      type: 'time',
    },

    completed_at: {
      sql: 'completed_at',
      type: 'time',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  joins: {
    order_users: {
      relationship: 'many_to_one',
      sql: `${CUBE}.user_id = ${order_users}.id`,
    }
  },

  segments: {
    sfUsers: {
      description: 'SF users segment from createCubeSchema',
      sql: `${CUBE}.location = 'San Francisco'`
    }
  },

  hierarchies: {
    hello: {
      title: 'World',
      levels: [status],
    },
  },

  preAggregations: {
    countCreatedAt: {
        type: 'rollup',
        measureReferences: [count],
        timeDimensionReference: created_at,
        granularity: `day`,
        partitionGranularity: `month`,
        refreshKey: {
          every: '1 hour',
        },
        scheduledRefresh: true,
    },
  },

  accessPolicy: [
    {
      role: "*",
      rowLevel: {
        allowAll: true
      }
    },
    {
      role: 'admin',
      conditions: [
        {
          if: `true`,
        }
      ],
      rowLevel: {
        filters: [
          {
            member: `${CUBE}.id`,
            operator: 'equals',
            values: [`1`, `2`, `3`]
          }
        ]
      },
      memberLevel: {
        includes: `*`,
        excludes: [`status`]
      },
    }
  ]
});
