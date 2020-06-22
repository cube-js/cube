cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  joins: {
    Orders: {
      relationship: `hasOne`,
      sql: `${Users}.id = ${Orders}.user_id`
    }
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [city, id, createdAt]
    }
  },

  dimensions: {
    firstName: {
      sql: `first_name`,
      type: `string`
    },
    lastName: {
      sql: `last_name`,
      type: `string`
    },
    city: {
      sql: `city`,
      type: `string`
    },

    gender: {
      sql: `gender`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    company: {
      sql: `company`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    age: {
      sql: `age`,
      type: `number`
    }
  }
});
