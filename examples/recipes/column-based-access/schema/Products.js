cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  joins: {
    Suppliers: {
      relationship: `belongsTo`,
      sql: `${CUBE}.supplier_id = ${Suppliers}.id`
    }
  },
  
  dimensions: {
    name: {
      sql: `name`,
      type: `string`
    }
  }
});
