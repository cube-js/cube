cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  joins: {

  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [lastName, firstName, city, id, createdAt]
    }
  },

  dimensions: {
    lastName: {
      sql: `last_name`,
      type: `string`
    },

    firstName: {
      sql: `first_name`,
      type: `string`
    },

    city: {
      sql: `city`,
      type: `string`
    },

    state: {
      sql: `state`,
      type: `string`
    },

    gender: {
      sql: `gender`,
      type: `string`
    },

    company: {
      sql: `company`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
