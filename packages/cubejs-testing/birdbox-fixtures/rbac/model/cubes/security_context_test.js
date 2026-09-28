cube('security_context_test', {
  sql: `
    SELECT * FROM line_items
    WHERE ${user_attributes.tenantId.filter('id')}
  `,

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
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
    total_price: {
      sql: 'price',
      type: 'sum',
    },
  },
});

cube('sc_array_filter_test', {
  sql: `
    SELECT * FROM line_items
    WHERE ${groups.filter('product_id')}
  `,

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },
});

cube('sc_interpolation_test', {
  sql: `SELECT * FROM line_items WHERE id > ${user_attributes.tenantId}`,

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },
});

cube('sc_ua_mask_test', {
  sql_table: 'public.line_items',

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    product_id: {
      sql: 'product_id',
      type: 'number',
    },
    masked_price: {
      sql: 'price',
      type: 'number',
      mask: {
        sql: `CAST(${userAttributes.tenantId} AS INTEGER)`,
      },
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  accessPolicy: [
    {
      role: '*',
      memberLevel: {
        includes: [],
      },
      memberMasking: {
        includes: '*',
      },
    },
  ],
});

cube('sc_cube_mask_test', {
  sql_table: 'public.line_items',

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    masked_product: {
      sql: `${CUBE}.product_id`,
      type: 'number',
      mask: {
        sql: `${CUBE}.product_id * -1`,
      },
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  accessPolicy: [
    {
      role: '*',
      memberLevel: {
        includes: [],
      },
      memberMasking: {
        includes: '*',
      },
    },
  ],
});

cube('sc_joined_mask_test', {
  sql_table: 'public.line_items',

  joins: {
    orders: {
      relationship: 'many_to_one',
      sql: `${orders}.id = ${sc_joined_mask_test}.order_id`,
    },
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    order_id: {
      sql: `${CUBE}.order_id`,
      type: 'number',
    },
    masked_order_id: {
      sql: `${CUBE}.order_id`,
      type: 'number',
      mask: {
        sql: `${orders.id}`,
      },
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  accessPolicy: [
    {
      role: '*',
      memberLevel: {
        includes: [],
      },
      memberMasking: {
        includes: '*',
      },
    },
  ],
});

cube('sc_groups_shorthand_test', {
  sql_table: 'public.line_items',

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    product_id: {
      sql: 'product_id',
      type: 'number',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  accessPolicy: [
    {
      role: '*',
      rowLevel: {
        filters: [{
          member: 'product_id',
          operator: 'equals',
          values: groups,
        }],
      },
    },
  ],
});
