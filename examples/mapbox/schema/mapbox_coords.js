cube(`mapbox__coords`, {
  sql: `SELECT * FROM public.mapbox__coords`,
  dataSource: `mapbox__example`,

  joins: {},

  dimensions: {
    postal: {
      sql: 'postal',
      type: 'string',
    },

    coordinates: {
      sql: `coordinates`,
      type: 'string',
      primaryKey: true,
      shown: true,
    },
  },
});
