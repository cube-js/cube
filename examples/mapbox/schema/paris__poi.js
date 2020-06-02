cube(`paris__poi`, {
  sql: `SELECT * FROM public.paris__poi`,

  measures: {},

  dimensions: {
    rating: {
      sql: `rating`,
      type: 'number',
    },

    name: {
      sql: `name`,
      type: 'string',
    },

    lat: {
      sql: `lat`,
      type: 'string',
    },

    lng: {
      sql: `lng`,
      type: 'string',
    },
  },
});
