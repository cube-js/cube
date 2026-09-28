view('users_view', {
  cubes: [{
    join_path: users,
    includes: '*',
  }],
  accessPolicy: [{
    group: '*',
    rowLevel: {
      filters: [{
        member: 'id',
        operator: 'gt',
        values: [10],
      }],
    },
  }]
});
