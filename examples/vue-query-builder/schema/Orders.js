cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
  joins: {
    Users: {
      sql: `${CUBE}.user_id = ${Users}.id`,
      relationship: `belongsTo`
    },
    
    Products: {
      sql: `${CUBE}.product_id = ${Products}.id`,
      relationship: `belongsTo`
    }
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },
    
    number: {
      sql: `number`,
      type: `sum`
    }
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },
    
    status: {
      sql: `status`,
      type: `string`
    },
    
    createdAt: {
      sql: `created_at`,
      type: `time`
    },
    
    completedAt: {
      sql: `completed_at`,
      type: `time`
    }
  }
});
