cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  preAggregations: {// Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  

    /**
     * Demo 5
     * Pre-aggregations
     */
    // dailyOrdersByCompany: {
    //   measures: [Orders.count],
    //   dimensions: [Suppliers.company],
    //   timeDimension: Orders.createdAt,
    //   granularity: `day`,
    //   partitionGranularity: `day`,
    //   refreshKey: {
    //     every: `1 hour`,
    //     incremental: true,
    //     updateWindow: `7 day`,
    //   }
    // }
  },
  joins: {
    /**
     * Demo 2
     * Joins
     * An "Order" belongs to a "User"
     */
    Users: {
      sql: `${CUBE}.user_id = ${Users}.id`,
      relationship: `belongsTo`
    },
    Products: {
      sql: `${CUBE}.product_id = ${Products}.id`,
      relationship: `belongsTo`
    }
  },

  /**
   * Demo 3
   * Filtering with segments
   */
  // segments: {
  //   processingStatusOrders: {
  //     sql: `${CUBE}.status = 'processing'`
  //   },
  //   shippedStatusOrders: {
  //     sql: `${CUBE}.status = 'shipped'`
  //   },
  //   completedStatusOrders: {
  //     sql: `${CUBE}.status = 'completed'`
  //   },
  // },
  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },
    number: {
      sql: `number`,
      type: `sum`
    },

    /**
     * Data modeling
     * Demo 2
     * Custom measures
     */
    // processingStatusOrders: {
    //   type: `count`,
    //   filters: [{
    //     sql: `${CUBE}.status = 'processing'`
    //   }]
    // },
    // processingStatusPercentage: {
    //   type: `number`,
    //   sql: `ROUND(
    //     ${CUBE.processingStatusOrders}::numeric / ${CUBE.count}::numeric * 100.0, 2
    //   )`
    // },
    // shippedStatusOrders: {
    //   type: `count`,
    //   filters: [{
    //     sql: `${CUBE}.status = 'shipped'`
    //   }]
    // },
    // shippedStatusPercentage: {
    //   type: `number`,
    //   sql: `ROUND(
    //     ${CUBE.shippedStatusOrders}::numeric / ${CUBE.count}::numeric * 100.0, 2
    //   )`
    // },
    // completedStatusOrders: {
    //   type: `count`,
    //   filters: [{
    //     sql: `${CUBE}.status = 'completed'`
    //   }]
    // },
    // completedStatusPercentage: {
    //   type: `number`,
    //   sql: `ROUND(
    //     ${CUBE.completedStatusOrders}::numeric / ${CUBE.count}::numeric * 100.0, 2
    //   )`
    // }
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
