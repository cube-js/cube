cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,

  // start part: suppliersRollup
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
  // end part: suppliersRollup

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
