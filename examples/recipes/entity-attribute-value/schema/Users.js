cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  joins: {
    Orders: {
      relationship: 'hasMany',
      sql: `${Users}.id = ${Orders}.user_id`,
    }
  },
  
  dimensions: {
    name: {
      sql: `first_name || ' ' || last_name`,
      type: `string`
    }
  }
});
