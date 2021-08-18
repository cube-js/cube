cube(`Products`, {
  sql: `SELECT * FROM public.products`,

  joins: {
    Suppliers: {
      sql: `${Suppliers}.id = ${CUBE}.supplier_id`,
      relationship: `hasOne`
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    name: {
      sql: `name`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});