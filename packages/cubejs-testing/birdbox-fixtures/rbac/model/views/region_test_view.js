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
            member: 'city',
            operator: 'equals',
            values: [security_context.auth?.userAttributes?.region],
          }],
        }
        : {
          allowAll: true,
        },
    },
  ],
});
