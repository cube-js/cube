cube(`Orders`, {
  sql: `
    select 1 as id, 100 as amount, 'new' as status
    UNION ALL
    select 2 as id, 200 as amount, 'processed' as status
  `,

  measures: {
    count: {
      type: `count`,
    },
    totalAmount: {
      sql: `amount`,
      type: `sum`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    status: {
      sql: `status`,
      type: `string`,
    },
  },
});

cube(`Customers`, {
  sql: `
    select 1 as id, 'Alice' as name
    UNION ALL
    select 2 as id, 'Bob' as name
  `,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    name: {
      sql: `name`,
      type: `string`,
    },
  },
});

cube(`Products`, {
  sql: `
    select 1 as id, 'Widget' as title
    UNION ALL
    select 2 as id, 'Gadget' as title
  `,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },
    title: {
      sql: `title`,
      type: `string`,
    },
  },
});

view(`RevenueView`, {
  cubes: [{
    joinPath: Orders,
    includes: `*`,
  }],
});

view(`CustomersView`, {
  viewGroup: `sales`,
  cubes: [{
    joinPath: Customers,
    includes: `*`,
  }],
});

view(`CatalogView`, {
  viewGroups: [`inventory`, `sales`],
  cubes: [{
    joinPath: Products,
    includes: `*`,
  }],
});

view_group(`sales`, {
  title: `Sales`,
  description: `Sales related views`,
  views: [`RevenueView`],
});

view_group(`inventory`, {
  title: `Inventory`,
  description: `Inventory related views`,
});
