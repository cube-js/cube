cube('line_items', {
  sql_table: 'public.line_items',

  data_source: 'default',

  joins: {
    orders: {
      relationship: 'many_to_one',
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

    price_dim: {
      sql: 'price',
      type: 'number',
    },
  },

  measures: {
    count: {
      type: 'count',
    },

    price: {
      sql: 'price',
      type: 'sum',
    },

    quantity: {
      sql: 'quantity',
      type: 'sum',
    },
  },

  accessPolicy: [
    {
      role: '*',
      rowLevel: {
        filters: [{
          member: 'id',
          operator: 'gt',
          // This is to test dynamic values based on security context
          values: [`${security_context.auth?.userAttributes?.minDefaultId || 20000}`],
        }]
      },
      memberLevel: {
        excludes: ['count', 'price', 'price_dim'],
      },
    },
    {
      role: 'admin',
      conditions: [
        {
          if: security_context.auth?.userAttributes?.region === 'CA',
        },
      ],
      rowLevel: {
        // The "allowAll" flag should negate the default `id` filter
        allowAll: true,
      },
      memberLevel: {
        excludes: ['price_dim'],
      },
    },
    {
      role: 'manager',
      conditions: [
        {
          if: security_context.auth?.userAttributes?.region === 'CA',
        },
        {
          // This condition should not match the one defined in the "manager" mock context
          if: security_context.auth?.userAttributes?.region === 'San Francisco',
        },
      ],
      rowLevel: {
        allowAll: true,
      },
      memberLevel: {
        excludes: ['created_at'],
      },
    },
  ],
});
