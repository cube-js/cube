cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  preAggregations: {
    // suppliersRollup: {
    //   type:`rollup`,
    //   external: true,
    //   dimensions: [CUBE.id, CUBE.address]
    // },
  },

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown:true
    },

    address: {
      sql: `address`,
      type: `string`
    },

    email: {
      sql: `email`,
      type: `string`
    },

    company: {
      sql: `company`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },

  dataSource: 'suppliers'
});
