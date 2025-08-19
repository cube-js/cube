// Example cube showing group-based access policies
cube('Example', {
  sql_table: 'public.example',

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
  },

  measures: {
    count: {
      type: 'count',
    },
  },

  access_policy: [
    // Role-based policy (existing functionality)
    {
      role: 'admin',
      memberLevel: {
        includes: ['*'],
      },
      rowLevel: {
        allowAll: true,
      },
    },

    // Group-based policy (new functionality) - single group
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

    // Groups-based policy (plural - preferred for multiple groups)
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

    // Manager role policy (separate from group-based policies)
    {
      role: 'manager',
      memberLevel: {
        includes: ['*'],
      },
      rowLevel: {
        filters: [
          {
            member: 'department',
            operator: 'equals',
            values: ['Management'],
          },
        ],
      },
    },

    // HR groups policy (using 'groups' with single value)
    {
      groups: 'hr',
      memberLevel: {
        includes: ['id', 'name', 'department', 'count'],
      },
      rowLevel: {
        filters: [
          {
            member: 'department',
            operator: 'equals',
            values: ['HR'],
          },
        ],
      },
    },

    // Default policy for users with no specific role/group
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
