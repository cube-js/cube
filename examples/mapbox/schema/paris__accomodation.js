cube(`paris__accomodation`, {
  sql: `SELECT * FROM public.paris__accomodation`,

  measures: {},

  dimensions: {
    name: {
      sql: `name`,
      type: 'string',
    },

    address: {
      sql: `address`,
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
