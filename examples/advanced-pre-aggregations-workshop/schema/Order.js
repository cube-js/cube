cube(`Order`, {
  sql: `SELECT * FROM tpc_h.order`,

  preAggregations: {
    /**
     * Step 1.
     * Using non-additive vs additive measures
     */

    /** Non-additive */
    // totalPriceAvgClerkCountDist: {
    //   measures: [
    //     Order.totalPriceAvg,
    //     Order.clerkCountDistinct
    //   ],
    //   dimensions: [
    //     Order.oOrderpriority
    //   ]
    // },

    /** Additive */
    // totalPriceAvgClerkCountDist: {
    //   measures: [
    //     Order.totalPriceSum,
    //     Order.count,
    //     Order.clerkCountDistinct
    //   ],
    //   dimensions: [
    //     Order.oOrderpriority
    //   ]
    // }

    /**
     * Step 2.
     * Using non-additive measures with dedicated pre-aggregations
     */
    /** With a dimension */
    // totalPriceAvgClerkCountDist1: {
    //   measures: [
    //     Order.totalPriceAvg,
    //     Order.clerkCountDistinct
    //   ],
    //   dimensions: [
    //     Order.oOrderpriority
    //   ]
    // },
    /** Without a dimension */
    // totalPriceAvgClerkCountDist2: {
    //   measures: [
    //     Order.totalPriceAvg,
    //     Order.clerkCountDistinct
    //   ]
    // },


    /**
     * Step 3.
     * Using dedicated pre-aggregations for large and complex queries
     */
    /** This is going to take ages to build...  */
    // dailyOrdersPerCustomer: {
    //   measures: [
    //     Order.count,
    //     Order.totalPriceSum
    //   ],
    //   dimensions: [
    //     Customer.cName
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`
    // }

    /** 
     * Step 4
     * Partitioning
     * Will partition the data into tables per year
     * Still an issue with tables that have >100k rows
     */
    // dailyOrdersPerCustomer: {
    //   measures: [
    //     Order.count,
    //     Order.totalPriceSum
    //   ],
    //   dimensions: [
    //     Customer.cName
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`
    // },

    /** 
     * Step 5
     * Cardinality
     * Break down complex pre-aggregations
     * Create pre-aggregations with definitions that fully match queries
     */
    // dailyOrdersPerCustomer: {
    //   measures: [
    //     Order.count,
    //     Order.totalPriceSum
    //   ],
    //   dimensions: [
    //     Customer.cName,
    //     Customer.cAcctbal
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`
    // },
    /** ==>  */
    // dailyOrderCountPerCustomer: {
    //   measures: [
    //     Order.count,
    //   ],
    //   dimensions: [
    //     Customer.cName
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`
    // },
    // dailyOrderPriceAvgPerCustomer: {
    //   measures: [
    //     Order.totalPriceAvg
    //   ],
    //   dimensions: [
    //     Customer.cName
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`
    // },

    /**
     * Step 6
     * Refresh tuning
     */
    // dailyOrdersPerCustomer: {
    //   measures: [
    //     Order.count,
    //     Order.totalPriceSum
    //   ],
    //   dimensions: [
    //     Customer.cName,
    //     Customer.cAcctbal
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`,
    //   refreshKey: {
    //     /**
    //      * Option 1 
    //      * Interval refresh key.
    //      * Defaul value is 1 hour.
    //      * Refreshes pre-aggregations based on a time interval.
    //      */
    //     // every: `1 minute`, // This will refresh every minute
    //     /**
    //      * Option 2
    //      * Refreshes pre-aggregations based on a CRON string.
    //      */
    //     // every: `* * * * *`, // This will refresh every minute
    //     /**
    //      * Option 3
    //      * Custom refresh check SQL
    //      * MAX(updated_at_timestamp) is a viable option
    //      * Examining a metadata table to see when it last ran
    //      * 
    //      * The default values for custom SQL refreshKey checks are:
    //      * => every: '2 minute' for BigQuery, Athena, Snowflake, and Presto
    //      * => every: '10 second' for all other databases
    //      */
    //     sql: `SELECT MAX(O_UPDATEDAT) FROM tpc_h.order;`,
    //     every: `1 minute`
    //   },
    // },

    /**
     * Step 7
     * Rollup-joins
     */
    /** Orders Rollup */
    // ordersRollup: {
    //   measures: [
    //     Order.count
    //   ],
    //   dimensions: [
    //     Order.oCustkey
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`,
    //   indexes: {
    //     customerIndex: {
    //       columns: [
    //         Order.oCustkey,
    //       ],
    //     },
    //   },
    // },
    /** Join Orders Rollup with Customers Rollup */
    // ordersCustomersRollupJoin: {
    //   type: `rollupJoin`,
    //   measures: [Order.count],
    //   dimensions: [Customer.cName],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `day`,
    //   partitionGranularity: `day`,
    //   rollups: [
    //     Customer.customersRollup,
    //     Order.ordersRollup,
    //   ],
    // },

    /**
     * Step 8
     * Rolling Window
     */
    // rollingCustomerCounts: {
    //   measures: [
    //     Order.monthlyCustomers,
    //     Order.weeklyCustomers,
    //     Order.dailyCustomers
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `year`
    // },

    /**
     * Step 9
     * Multi-tenancy
     */
    // regionCustomerOrderCount: {
    //   measures: [
    //     Order.count
    //   ],
    //   dimensions: [
    //     Customer.cName,
    //     Region.rName,
    //     Region.rRegionkey
    //   ],
    //   timeDimension: Order.oOrderdate,
    //   granularity: `month`
    // }
  },

  joins: {
    /**
     * Step 3.
     * Introducing joins for large queries and dedicated pre-aggregations
     */
    Customer: {
      relationship: `belongsTo`,
      sql: `${CUBE.oCustkey} = ${Customer.cCustkey}`,
    },
  },

  measures: {
    count: {
      type: `count`,
      drillMembers: [oOrderdate]
    },

    /**
     * Step 1.
     * Using non-additive vs additive measures
     */ 

    /** Non-Additive */
    // totalPriceAvg: {
    //   sql: `${CUBE}.O_TOTALPRICE`,
    //   type: `avg`
    // },
    // clerkCountDistinct: {
    //   sql: `${CUBE}.O_CLERK`,
    //   type: `countDistinct`
    // },
    
    /** Additive */
    // totalPriceAvg: {
    //   sql: `${CUBE.totalPriceSum} / ${CUBE.count}`,
    //   type: `number`,
    // },
    // totalPriceSum: {
    //   sql: `${CUBE}.O_TOTALPRICE`,
    //   type: `sum`,
    // },
    // clerkCountDistinct: {
    //   sql: `${CUBE}.O_CLERK`,
    //   type: `countDistinctApprox`
    // },

    /**
     * Step 2.
     * Using non-additive measures with dedicated pre-aggregations
     */
    // totalPriceAvg: {
    //   sql: `${CUBE}.O_TOTALPRICE`,
    //   type: `avg`
    // },
    // clerkCountDistinct: {
    //   sql: `${CUBE}.O_CLERK`,
    //   type: `countDistinct`
    // },

    /**
     * Step 3 & Step 4 & Step 5
     * Using dedicated pre-aggregations for large and complex queries
     */
    // totalPriceAvg: {
    //   sql: `${CUBE.totalPriceSum} / ${CUBE.count}`,
    //   type: `number`,
    // },
    // totalPriceSum: {
    //   sql: `${CUBE}.O_TOTALPRICE`,
    //   type: `sum`,
    // },

    /**
     * Step 8
     * Rolling Window
     */
    // monthlyCustomers: {
    //   sql: `${CUBE}.O_CUSTKEY`,
    //   type: `countDistinctApprox`,
    //   rollingWindow: {
    //     trailing: `30 day`,
    //     offset: `start`,
    //   },
    // },
    // weeklyCustomers: {
    //   sql: `${CUBE}.O_CUSTKEY`,
    //   type: `countDistinctApprox`,
    //   rollingWindow: {
    //     trailing: `7 day`,
    //     offset: `start`,
    //   },
    // },
    // dailyCustomers: {
    //   sql: `${CUBE}.O_CUSTKEY`,
    //   type: `countDistinctApprox`,
    //   rollingWindow: {
    //     trailing: `1 day`,
    //     offset: `start`,
    //   },
    // },
  },

  dimensions: {
    /**
     * Step 3.
     * Introducing joins for large queries and dedicated pre-aggregations
     */
    oCustkey: {
      sql: `${CUBE}.O_CUSTKEY`,
      type: `number`,
      primaryKey: true
    },

    oOrderstatus: {
      sql: `${CUBE}.O_ORDERSTATUS`,
      type: `string`
    },
    
    oTotalprice: {
      sql: `${CUBE}.O_TOTALPRICE`,
      type: `string`
    },
    
    oOrderpriority: {
      sql: `${CUBE}.O_ORDERPRIORITY`,
      type: `string`
    },
    
    oClerk: {
      sql: `${CUBE}.O_CLERK`,
      type: `string`
    },
    
    oComment: {
      sql: `${CUBE}.O_COMMENT`,
      type: `string`
    },
    
    /**
     * Step 0
     * Cast the date as timestamp
     */
    oOrderdate: {
      sql: `CAST(${CUBE}.\`O_ORDERDATE\` AS TIMESTAMP)`,
      type: `time`
    }
  }
});