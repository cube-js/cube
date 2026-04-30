cube('Orders', {
  sql: `SELECT 1 as id, 100 as amount, 'secret123' as internal_code, 'premium' as tier`,

  measures: {
    count: {
      type: 'count',
    },
    totalAmount: {
      sql: 'amount',
      type: 'sum',
    },
  },

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primaryKey: true,
    },
    amount: {
      sql: 'amount',
      type: 'number',
    },
    internalCode: {
      sql: 'internal_code',
      type: 'string',
    },
    tier: {
      sql: 'tier',
      type: 'string',
    },
  },

  accessPolicy: [
    {
      role: '*',
      memberLevel: {
        includes: [],
      },
    },
    {
      role: 'tenant-a',
      memberLevel: {
        includes: '*',
        excludes: ['tier'],
      },
    },
    {
      role: 'tenant-b',
      memberLevel: {
        includes: '*',
        excludes: ['internalCode'],
      },
    },
  ],
});
