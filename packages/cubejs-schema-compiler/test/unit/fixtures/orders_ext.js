cube('ordersExt', {
  extends: orders,

  dimensions: {
    city: {
      sql: 'city',
      type: 'string',
    },
  },

  measures: {
    count_distinct: {
      type: 'count_distinct',
      sql: 'status',
    },
  },

  segments: {
    anotherStatus: {
      description: 'Just another one',
      sql: `${CUBE}.status = 'Rock and Roll'`
    }
  },

  hierarchies: {
    ehlo: {
      title: 'UnderGround',
      levels: [status, city],
    },
  },

  preAggregations: {
    mainPreAggs: {
      type: 'rollup',
      measures: [count_distinct],
      dimensions: [city]
    }
  },

  accessPolicy: [
    {
      role: 'manager',
      conditions: [
        {
          if: security_context.userId === 1,
        }
      ],
      rowLevel: {
        filters: [
          {
            or: [
              {
                member: `location`,
                operator: 'startsWith',
                values: [`San`]
              },
              {
                member: `location`,
                operator: 'startsWith',
                values: [`Lon`]
              }
            ]
          }
        ]
      },
      memberLevel: {
        includes: `*`,
        excludes: [`min`, `max`]
      },
    },
  ]
});
