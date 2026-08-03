cube(`Suppliers`, {
  sql: `
    SELECT 10 AS id, 'Fruits Inc' AS company
    UNION ALL SELECT 20 AS id, 'Orchards Inc' AS company
  `,

  preAggregations: {
    suppliersRollup: {
      type: `rollup`,
      external: true,
      dimensions: [CUBE.id, CUBE.company],
      indexes: {
        categoryIndex: {
          columns: [CUBE.id],
        },
      },
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    company: {
      sql: `company`,
      type: `string`,
    },
  },

  dataSource: 'suppliers',
});

view(`SuppliersView`, {
  cubes: [{
    joinPath: Suppliers,
    includes: `*`,
  }]
});
