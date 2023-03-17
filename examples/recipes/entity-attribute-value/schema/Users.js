cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  joins: {
    Orders: {
      relationship: 'hasMany',
      sql: `${CUBE}.id = ${Orders.userId}`,
    }
  },

  dimensions: {
    name: {
      sql: `first_name || ' ' || last_name`,
      type: `string`
    }
  }
});
