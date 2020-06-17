cube(`MapboxCoords`, {
  sql: `SELECT * FROM public.MapboxCoords`,
  dataSource: `mapbox__example`,
  joins: {},
  dimensions: {
    coordinates: {
      sql: `coordinates`,
      type: 'string',
      primaryKey: true,
      shown: true,
    },
  },
});
