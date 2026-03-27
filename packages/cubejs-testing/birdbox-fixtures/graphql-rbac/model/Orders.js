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

  // RBAC access policies - these apply visibility masks at runtime
  accessPolicy: [
    {
      // Default policy for all roles - hide special fields
      role: '*',
      memberLevel: {
        excludes: ['internalCode', 'tier'],
      },
    },
    {
      // tenant-a can see internalCode (include all, exclude only tier)
      role: 'tenant-a',
      memberLevel: {
        includes: '*',
        excludes: ['tier'],
      },
    },
    {
      // tenant-b can see tier (include all, exclude only internalCode)
      role: 'tenant-b',
      memberLevel: {
        includes: '*',
        excludes: ['internalCode'],
      },
    },
  ],
});
