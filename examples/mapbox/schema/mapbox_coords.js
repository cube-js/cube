cube(`mapbox__coords`, {
  sql: `SELECT * FROM public.mapbox__coords`,

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

    /*
      хотела сгруппировать координаты в мультиполигоны в subquery, не получилось
      concat: {
      sql: `string_agg(${coordinates}, 'NEWCOORDS')`,
      type: 'string',
    },*/
  },
});
