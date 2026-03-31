// Orders cube with RBAC-based field visibility using access policies
// This tests the GraphQL schema caching bug where different tenants should see different fields

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
    // This field should only be visible to tenant-a
    internalCode: {
      sql: 'internal_code',
      type: 'string',
    },
    // This field should only be visible to tenant-b
    tier: {
      sql: 'tier',
      type: 'string',
    },
  },

  // RBAC access policies - complete denial for default role
  accessPolicy: [
    {
      // Default: complete denial - no members accessible (triggers "You requested hidden member" error)
      role: '*',
      memberLevel: {
        includes: [],
      },
    },
    {
      // tenant-a: can access all EXCEPT tier
      role: 'tenant-a',
      memberLevel: {
        includes: '*',
        excludes: ['tier'],
      },
    },
    {
      // tenant-b: can access all EXCEPT internalCode
      role: 'tenant-b',
      memberLevel: {
        includes: '*',
        excludes: ['internalCode'],
      },
    },
  ],
});
