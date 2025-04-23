cube(`Orders`, {
  sql: `
  select 1 as id, 100 as amount, 'new' status, '2024-01-01'::timestamptz created_at
  UNION ALL
  select 2 as id, 200 as amount, 'new' status, '2024-01-02'::timestamptz created_at
  UNION ALL
  select 3 as id, 300 as amount, 'processed' status, '2024-01-03'::timestamptz created_at
  UNION ALL
  select 4 as id, 500 as amount, 'processed' status, '2024-01-04'::timestamptz created_at
  UNION ALL
  select 5 as id, 600 as amount, 'shipped' status, '2024-01-05'::timestamptz created_at
  `,
  joins: {
    OrderItems: {
      relationship: 'one_to_many',
      sql: `${CUBE.id} = ${OrderItems.order_id}`
    },
  },
  measures: {
    count: {
      type: `count`,
    },
    orderCount: {
      type: `count_distinct`,
      sql: `CASE WHEN ${Orders.status} = 'shipped' THEN ${CUBE}.id END`
    },
    netCollectionCompleted: {
      type: `sum`,
      sql: `CASE WHEN ${Orders.status} = 'shipped' THEN ${CUBE}.amount END`
    },
    arpu: {
      type: `number`,
      sql: `1.0 * ${netCollectionCompleted} / ${orderCount}`
    },
    refundRate: {
      type: `number`,
      sql: `1.0 * ${refundOrdersCount} / ${overallOrders}`
    },
    refundOrdersCount: {
      type: `count_distinct`,
      sql: `CASE WHEN ${Orders.status} = 'refunded' THEN ${CUBE}.id END`
    },
    overallOrders: {
      type: `count_distinct`,
      sql: `CASE WHEN ${Orders.status} != 'cancelled' THEN ${CUBE}.id END`
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
    toRemove: {
      type: `count`,
    },
    numberTotal: {
      sql: `${totalAmount}`,
      type: `number`
    },
    amountRank: {
      multi_stage: true,
      type: `rank`,
      order_by: [{
        sql: `${totalAmount}`,
        dir: 'asc'
      }],
      reduce_by: [status],
    },
    amountReducedByStatus: {
      multi_stage: true,
      type: `sum`,
      sql: `${totalAmount}`,
      reduce_by: [status],
    },
    statusPercentageOfTotal: {
      multi_stage: true,
      sql: `${totalAmount} / NULLIF(${amountReducedByStatus}, 0)`,
      type: `number`,
    },
    amountRankView: {
      multi_stage: true,
      type: `number`,
      sql: `${amountRank}`,
    },
    amountRankDateMax: {
      multi_stage: true,
      sql: `${createdAt}`,
      type: `max`,
      filters: [{
        sql: `${amountRank} = 1`
      }]
    },
    amountRankDate: {
      multi_stage: true,
      sql: `${amountRankDateMax}`,
      type: `time`,
    },
    countAndTotalAmount: {
      type: "string",
      sql: `CONCAT(${count}, ' / ', ${totalAmount})`,
    },
    createdAtMax: {
      type: `max`,
      sql: `created_at`,
    },
    createdAtMaxProxy: {
      type: "time",
      sql: `${createdAtMax}`,
    },
  },
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      public: true,
    },

    status: {
      sql: `status`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },
});

cube(`OrderItems`, {
  sql: `
  select 1 as id, 1 as order_id, 'Phone' AS name, 'Electronics' AS type, '2024-01-01'::timestamptz created_at
  UNION ALL
  select 2 as id, 2 as order_id, 'Keyboard' AS name, 'Electronics' AS type, '2024-01-02'::timestamptz created_at
  UNION ALL
  select 3 as id, 3 as order_id, 'Glass' AS name, 'Home' AS type, '2024-01-03'::timestamptz created_at
  UNION ALL
  select 4 as id, 4 as order_id, 'Lamp' AS name, 'Home' AS type, '2024-01-04'::timestamptz created_at
  UNION ALL
  select 5 as id, 5 as order_id, 'Pen' AS name, 'Office' AS type, '2024-01-05'::timestamptz created_at
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
      public: true,
    },

    order_id: {
      sql: `order_id`,
      type: `number`,
      public: true,
    },

    name: {
      sql: `name`,
      type: `string`,
      public: true,
    },

    type: {
      sql: `type`,
      type: `string`,
      public: true,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
      public: true,
    }
  },
});

view(`OrdersView`, {
  cubes: [{
    joinPath: Orders,
    includes: `*`,
    excludes: [`toRemove`]
  }]
});

view(`OrdersItemsPrefixView`, {
  cubes: [{
    joinPath: Orders,
    includes: `*`,
    excludes: [`toRemove`],
    prefix: true
  },
  {
    joinPath: Orders.OrderItems,
    includes: `*`,
    prefix: true
  }]
});
