cube(`LineItems`, {
  sql: `SELECT * FROM public.line_items`,
  
  // Copy me ↓
  preAggregations: {
    main: {
      measures: [ CUBE.count, CUBE.revenue, CUBE.price, CUBE.quantity ],
      timeDimension: CUBE.createdAt,
      granularity: 'day'
    }
  },
  // Copy me ↑
  
  measures: {
    count: {
      type: `count`
    },
    
    price: {
      sql: `price`,
      type: `sum`
    },
    
    quantity: {
      sql: `quantity`,
      type: `sum`
    },

    avgPrice: {
      sql: `${CUBE.price} / ${CUBE.quantity}`,
      type: `number`
    },

    revenue: {
      sql: `price`,
      type: `sum`,
      rollingWindow: {
        trailing: `unbounded`,
      },
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
  },
  
  dataSource: `default`
});
