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

    price: {
      sql: `${LineItems.price}`,
      subQuery: true,
      type: `number`,
      format: `currency`
    },

    priceRange: {
      type: `string`,
      case: {
        when: [
          { sql: `${price} < 101`, label: `$0 - $100` },
          { sql: `${price} < 201`, label: `$100 - $200` }
        ], else: {
          label: `$200+`
        }
      }
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
