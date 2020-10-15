cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  joins: {
    Products: {
      sql: `${CUBE}.product_id = ${Products}.id`,
      relationship: `belongsTo`,
    },

    Users: {
      sql: `${CUBE}.user_id = ${Users}.id`,
      relationship: `belongsTo`,
    },
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt],
    },

    number: {
      sql: `number`,
      type: `sum`,
    },
  },

  dimensions: {
    status: {
      sql: `status`,
      type: `string`,
    },

    id: {
      shown: true,
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`,
    },

    size: {
      sql: `${LineItems.count}`,
      subQuery: true,
      type: 'number',
    },

    price: {
      sql: `${LineItems.price}`,
      subQuery: true,
      type: 'number',
    },
  },
});
