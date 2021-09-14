cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  preAggregations: {
    suppliersRollup: {
      type:`rollup`,
      external: true,
      dimensions: [CUBE.id, CUBE.company, CUBE.email],
      indexes: {
        categoryIndex: {
          columns: [CUBE.id],
        },
      },
    },
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
