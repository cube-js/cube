cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
      shown: true
    },
    
    name: {
      sql: `first_name || ' ' || last_name`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});
