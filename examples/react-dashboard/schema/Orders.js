cube(`Orders`, {
  sql: `select * from public.orders`,

  joins: {
    Products: {
      relationship: `belongsTo`,
      sql: `${Orders}.product_id = ${Products}.id`
    },
    LineItems: {
      relationship: `hasMany`,
      sql: `${Orders}.id = ${LineItems}.order_id`
    }
  },

  measures: {
    count: {
      type: `count`,
    },

    totalAmount: {
      sql: `${amount}`,
      type: `sum`,
      format: `currency`
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
      type: `string`,
      description: `Status of order`
    },


    userId: {
      sql: `user_id`,
      type: `number`,
      shown: false,
    },

    completedAt: {
      sql: `completed_at`,
      type: `time`
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    },

    amount: {
      sql: `${LineItems.totalAmount}`,
      type: `number`,
      format: `currency`,
      subQuery: true,
      shown: false
    },

    amountTier: {
      type: `string`,
      case: {
        when: [
          { sql: `${amount} < 100 OR ${amount} is NULL`, label: `$0 - $100` },
          { sql: `${amount} >= 100 AND ${amount} < 200`, label: `$100 - $200` },
          { sql: `${amount} >= 200`, label: `$200 +` }
        ],
        else: {
          label: `Unknown`
        }
      }
    }
  },

  segments: {
    completed: {
      sql: `status = 'completed'`
    },

    processing: {
      sql: `status = 'processing'`
    },

    shipped: {
      sql: `status = 'shipped'`,
    }
  },

  preAggregations: {
    someRollup: {
      type: `rollup`,
      measureReferences: [totalAmount, count],
      timeDimensionReference: completedAt,
      granularity: `day`,
      external: true
    },
    /*main: {
     type: `originalSql`
   },*/
 }
});
