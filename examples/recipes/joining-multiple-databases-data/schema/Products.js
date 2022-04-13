cube(`Products`, {
  sql: `SELECT * FROM public.products`,

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
      dimensions: [Suppliers.email, Suppliers.company, CUBE.name],
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

  dataSource: 'Products',
});
