cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  joins: {

  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [name, id, createdAt]
    }
  },

  dimensions: {
    name: {
      sql: `name`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    description: {
      sql: `description`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
