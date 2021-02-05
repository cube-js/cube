cube(`LineItems`, {
  sql: `SELECT * FROM public.line_items`,
  
  joins: {
    Orders: {
      sql: `${CUBE}.order_id = ${Orders}.id`,
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
    
    quantity: {
      sql: `quantity`,
      type: `sum`
    },
    
    price: {
      sql: `price`,
      type: `sum`
    }
  },
  
  dimensions: {
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
