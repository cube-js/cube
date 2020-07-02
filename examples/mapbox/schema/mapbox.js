cube(`Mapbox`, {
  sql: `SELECT * FROM public.Mapbox`,

  joins: {
    MapboxCoords: {
      sql: `${CUBE}.iso_a3 = ${MapboxCoords}.iso_a3`,
      relationship: `belongsTo`,
    },
  }
});

