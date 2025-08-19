// Example cube showing mixed role-based and group-based access policies
// This demonstrates that contextToRoles and contextToGroups can coexist
// but individual policies must use either role OR group, not both
cube('MixedAccess', {
  sql_table: 'public.mixed_access',
  
  data_source: 'default',

  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    name: {
      sql: 'name',
      type: 'string',
    },
    department: {
      sql: 'department',
      type: 'string',
    },
    region: {
      sql: 'region',
      type: 'string',
    },
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  access_policy: [
    // ✅ Role-based policy
    {
      role: 'admin',
      memberLevel: {
        includes: ['*'],
      },
      rowLevel: {
        allowAll: true,
      },
    },
    
    // ✅ Another role-based policy
    {
      role: 'manager',
      memberLevel: {
        includes: ['*'],
      },
      rowLevel: {
        filters: [
          {
            member: 'region',
            operator: 'equals',
            values: () => COMPILE_CONTEXT.securityContext.region,
          },
        ],
      },
    },

    // ✅ Group-based policy (single group)
    {
      group: 'analytics',
      memberLevel: {
        includes: ['id', 'name', 'count'],
      },
      rowLevel: {
        filters: [
          {
            member: 'department',
            operator: 'equals',
            values: ['Analytics'],
          },
        ],
      },
    },

    // ✅ Group-based policy (multiple groups)
    {
      groups: ['finance', 'accounting'],
      memberLevel: {
        includes: ['id', 'name', 'department', 'count'],
      },
      rowLevel: {
        filters: [
          {
            member: 'department',
            operator: 'in',
            values: ['Finance', 'Accounting'],
          },
        ],
      },
    },

    // ❌ This would be invalid (mixing role and group in same policy):
    // {
    //   role: 'manager',
    //   group: 'hr',
    //   memberLevel: { includes: ['*'] }
    // }

    // Default policy for users without specific roles/groups
    {
      memberLevel: {
        includes: ['count'],
      },
      rowLevel: {
        filters: [
          {
            member: 'department',
            operator: 'equals',
            values: ['Public'],
          },
        ],
      },
    },
  ],
});