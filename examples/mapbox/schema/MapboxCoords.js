cube(`MapboxCoords`, {
  sql: `SELECT * FROM public.MapboxCoords`,
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
