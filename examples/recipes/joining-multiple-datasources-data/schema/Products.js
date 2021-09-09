cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  preAggregations: {
    productsRollup: {
      type:`rollup`,
      external: true,
      measures:[CUBE.count],
      dimensions: [CUBE.name, CUBE.description, CUBE.supplierId],
    },

    combinedRollup: {
      type: `rollupJoin`,
      measures:[CUBE.count],
      dimensions: [Suppliers.id, Suppliers.address, CUBE.name, CUBE.description],
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
  measures: {
    count: {
      type: `count`
    },
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
    description: {
      sql: `description`,
      type: `string`
    },
    createdAt: {
      sql: `created_at`,
      type: `time`
    },
    supplierId: {
      sql: `supplier_id`,
      type: `number`
    },
  }
});
