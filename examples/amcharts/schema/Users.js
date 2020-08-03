cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  joins: {},

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true,
    },

    name: {
      sql: `name`,
      type: `string`,
    },

    image: {
      sql: `${CUBE}."profile.image_48"`,
      type: `string`,
    },

    updated: {
      sql: `TO_TIMESTAMP(updated)`,
      type: `time`,
    },
  },
});
