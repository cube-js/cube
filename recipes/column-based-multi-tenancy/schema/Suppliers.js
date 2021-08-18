cube(`Suppliers`, {
  sql: `SELECT * FROM public.suppliers`,
  

  joins: {
    Products: {
      sql: `${CUBE}.id = ${Products}.supplier_id`,
      relationship: `hasMany`
    }
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    email: {
      sql: `email`,
      type: `string`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  }
});