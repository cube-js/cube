cube('orders', {
  sql_table: 'public.orders',

  data_source: 'default',

  joins: {
    line_items: {
      relationship: 'one_to_many',
      sql: `${orders}.id = ${line_items}.order_id`,
    },
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },

    created_at: {
      sql: 'created_at',
      type: 'time',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  access_policy: [
    {
      role: '*',
      memberLevel: {
        // This cube is "private" by default and only accessible via views
        includes: [],
      },
      rowLevel: {
        filters: [
          {
            member: 'id',
            operator: 'equals',
            values: [1],
          },
        ],
      },
    },
    {
      role: 'admin',
      memberLevel: {
        // This cube is "private" by default and only accessible via views
        includes: [],
      },
      rowLevel: {
        filters: [
          {
            or: [
              {
                member: `${CUBE}.id`,
                operator: 'equals',
                values: [10],
              },
              {
                // Testing different ways of referencing cube members
                member: 'id',
                operator: 'equals',
                values: ['11'],
              },
            ],
          },
        ],
      },
    },
  ],
});
