cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  preAggregations: {
    // start part: productsRollup
    productsRollup: {
      type:`rollup`,
      external: true,
      dimensions: [CUBE.name, CUBE.supplierId],
      indexes: {
        categoryIndex: {
          columns: [CUBE.supplierId],
        },
      },
    // end part: productsRollup
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
      relationship: `belongsTo`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    name: {
      sql: `name`,
      type: `string`
    },
    supplierId: {
      sql: `supplier_id`,
      type: `number`
    }
  },

  dataSource: 'products'
});
