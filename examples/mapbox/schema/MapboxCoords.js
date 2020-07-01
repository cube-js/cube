cube(`MapboxCoords`, {
  sql: `SELECT * FROM public.MapboxCoords`,
  dimensions: {
    coordinates: {
      sql: `coordinates`,
      type: 'string',
      primaryKey: true,
      shown: true,
    },
  },
});
