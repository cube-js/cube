view('region_test_view', {
  cubes: [{
    join_path: region_test,
    includes: '*',
  }],
  accessPolicy: [
    {
      group: 'user_group',
      memberLevel: {
        includes: '*',
      },
      rowLevel: security_context.auth?.groups?.includes('region_group')
        ? {
          filters: [{
            member: 'product_id',
            operator: 'equals',
            values: security_context.auth?.userAttributes?.allowedProductIds,
          }],
        }
        : {
          allowAll: true,
        },
    },
  ],
});
