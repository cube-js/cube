cube('addresses', {
  sql: 'SELECT * FROM addresses',
  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    street: {
      sql: 'street',
      type: 'string',
    },
    zip_code: {
      sql: 'zip_code',
      type: 'string',
    },
  },
});

cube('users', {
  sql: 'SELECT * FROM users',
  joins: {
    addresses: {
      sql: `${CUBE}.address_id = ${addresses}.id`,
      relationship: 'many_to_one',
    },
  },
  hierarchies: {
    users_hierarchy: {
      levels: [
        'users.age',
        'city',
      ],
    },
  },
  dimensions: {
    age: {
      sql: 'age',
      type: 'number',
    },
    state: {
      sql: 'state',
      type: 'string',
    },
    city: {
      sql: 'city',
      type: 'string',
    },
    gender: {
      sql: 'gender',
      type: 'string',
    },
  },
});

cube('orders', {
  sql: 'SELECT * FROM orders',
  joins: {
    users: {
      sql: `${CUBE}.order_id = ${orders}.id`,
      relationship: 'many_to_one',
    },
  },
  measures: {
    count: {
      sql: 'id',
      type: 'count',
    },
  },
  dimensions: {
    id: {
      sql: 'id',
      type: 'number',
      primary_key: true,
    },
    number: {
      sql: 'number',
      type: 'number',
    },
    status: {
      sql: 'status',
      type: 'string',
    },
  },
  hierarchies: {
    orders_hierarchy: {
      levels: [
        `${CUBE}.status`,
        'number',
        'users.city',
      ],
    },
    some_other_hierarchy: {
      title: 'Some other hierarchy',
      levels: [
        'users.state',
        'users.city',
      ],
    },
  },
});

view('test_view', {
  cubes: [
    {
      join_path: orders,
      includes: '*',
    },
    {
      join_path: users,
      includes: [
        'age',
        'state',
        { name: 'gender', alias: 'renamed_gender' },
      ],
    },
  ],
  folders: [
    {
      name: 'folder1',
      includes: ['age', 'renamed_gender'],
    },
    {
      name: 'folder2',
      includes: '*',
    },
  ],
});

view('test_view2', {
  cubes: [
    {
      join_path: orders,
      alias: 'renamed_orders',
      prefix: true,
      includes: '*',
    },
    {
      join_path: users,
      prefix: true,
      includes: ['age', 'state', 'gender'],
    },
  ],
  folders: [
    {
      name: 'folder1',
      includes: ['users_age', 'users_state', 'renamed_orders_status'],
    },
  ],
});

view('test_view3', {
  extends: test_view2,
  cubes: [
    {
      join_path: users,
      prefix: true,
      includes: [
        'city',
        { name: 'gender', alias: 'renamed_in_view3_gender' },
      ],
    },
  ],
  folders: [
    {
      name: 'folder2',
      includes: ['users_city', 'users_renamed_in_view3_gender'],
    },
  ],
});

view('test_view4', {
  extends: test_view3,
  folders: [
    {
      name: 'folder3',
      includes: [
        'users_city',
        {
          name: 'inner folder 4',
          includes: ['renamed_orders_status'],
        },
        {
          name: 'inner folder 5',
          includes: '*',
        },
      ],
    },
  ],
});

view('test_view_join_path', {
  cubes: [
    {
      join_path: orders,
      prefix: true,
      includes: '*',
    },
    {
      join_path: users,
      prefix: true,
      includes: ['age', 'state', 'city', 'gender'],
    },
  ],
  folders: [
    {
      name: 'Orders Folder',
      includes: [{ join_path: orders }],
    },
    {
      name: 'Users Folder',
      includes: [{ join_path: users }],
    },
    {
      name: 'Mixed Folder',
      includes: [
        { join_path: orders },
        'users_age',
        'users_state',
      ],
    },
  ],
});

view('test_view_nested_join_path', {
  cubes: [
    {
      join_path: orders,
      prefix: true,
      includes: '*',
    },
    {
      join_path: orders.users,
      prefix: true,
      includes: ['age', 'state'],
    },
    {
      join_path: orders.users.addresses,
      prefix: true,
      includes: ['street', 'zip_code'],
    },
  ],
  folders: [
    {
      name: 'Orders Folder',
      includes: [{ join_path: orders }],
    },
    {
      name: 'Users via Orders',
      includes: [{ join_path: orders.users }],
    },
    {
      name: 'Addresses via Users',
      includes: [{ join_path: orders.users.addresses }],
    },
    {
      name: 'Mixed Nested Folder',
      includes: [
        { join_path: orders.users },
        { join_path: orders.users.addresses },
        'orders_status',
        'orders_count',
      ],
    },
  ],
});
