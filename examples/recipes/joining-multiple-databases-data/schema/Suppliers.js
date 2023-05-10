cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  preAggregations: {
    suppliersRollup: {
      type: `rollup`,
      external: true,
      dimensions: [CUBE.id, CUBE.company, CUBE.email],
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

    email: {
      sql: `email`,
      type: `string`,
    },

    company: {
      sql: `company`,
      type: `string`,
    },
  },

  dataSource: 'suppliers',
});
