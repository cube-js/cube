cube(`mapbox`, {
  sql: `SELECT * FROM public.mapbox`,

  joins: {
    mapbox__coords: {
      sql: `${CUBE}.iso_a3 = ${mapbox__coords}.iso_a3`,
      relationship: `belongsTo`,
    },
  },

  measures: {},

  dimensions: {
    name: {
      sql: 'name',
      type: 'string',
    },

    postal: {
      sql: 'postal',
      type: 'string',
      primaryKey: true,
    },

    geometry: {
      sql: 'geometry',
      type: 'string',
    },
  },
});
