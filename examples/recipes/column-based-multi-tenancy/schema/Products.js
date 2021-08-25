cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  joins: {
    Suppliers: {
      sql: `${Suppliers}.id = ${CUBE}.supplier_id`,
      relationship: `hasOne`
    }
  },
  
  dimensions: {
    name: {
      sql: `name`,
      type: `string`
    }
  }
});
