view('region_test_view', {
  cubes: [{
    join_path: region_test,
    includes: '*',
  }],
  accessPolicy: [
    {
      group: 'user_group',
      conditions: [
        {
          if: security_context.auth?.groups?.includes('region_group'),
        },
      ],
      memberLevel: {
        includes: '*',
      },
      rowLevel: {
        filters: [{
          member: 'product_id',
          operator: 'equals',
          values: security_context.auth?.userAttributes?.allowedProductIds,
        }],
      },
    },
    {
      group: 'user_group',
      conditions: [
        {
          if: !security_context.auth?.groups?.includes('region_group'),
        },
      ],
      memberLevel: {
        includes: '*',
      },
      rowLevel: {
        allowAll: true,
      },
    },
  ],
});
