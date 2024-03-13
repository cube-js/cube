cube('Bar', {
  data_source: `postgres`,
  sql_table: `events`,
  
  dimensions: {
    dt: {
      sql: `dt`,
      type: `string`,
      primary_key: true
    },

    id: {
      sql: `id`,
      type: `number`,
      primary_key: true
    },
  },

  pre_aggregations: {
    main: {
      dimensions: [
        CUBE.dt,
        CUBE.id,
      ],
      refreshKey: {
        every: `1 second`,
      }
    }
  }
});