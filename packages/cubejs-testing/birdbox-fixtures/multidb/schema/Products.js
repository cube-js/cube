cube(`Products`, {
  sql: `
    SELECT 1 AS id, 'apples' AS name, 10 AS supplier_id
    UNION ALL SELECT 2 AS id, 'bananas' AS name, 10 AS supplier_id
    UNION ALL SELECT 3 AS id, 'oranges' AS name, 20 AS supplier_id
  `,

  preAggregations: {
    productsRollup: {
      type: `rollup`,
      external: true,
      dimensions: [CUBE.name, CUBE.supplierId],
      indexes: {
        categoryIndex: {
          columns: [CUBE.supplierId],
        },
      },
    },

    combinedRollup: {
      type: `rollupJoin`,
      dimensions: [Suppliers.company, CUBE.name],
      rollups: [Suppliers.suppliersRollup, CUBE.productsRollup],
      external: true,
    },
  },

  joins: {
    Suppliers: {
      sql: `${CUBE.supplierId} = ${Suppliers.id}`,
      relationship: `belongsTo`,
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
    supplierId: {
      sql: `supplier_id`,
      type: `number`,
    },
  },

  dataSource: 'products',
});

view(`ProductsView`, {
  cubes: [{
    joinPath: Products,
    includes: `*`,
  }]
});
