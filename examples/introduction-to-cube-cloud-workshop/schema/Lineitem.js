cube(`Lineitem`, {
  sql: `SELECT * FROM public.lineitem`,
  
  /**
   * Demo: Performance -> Pre-aggregations
   */
  preAggregations: {
    // lineitemsWithPricePerDay: {
    //   measures: [Lineitem.count],
    //   dimensions: [Lineitem.lExtendedprice],
    //   timeDimension: Lineitem.lReceiptdate,
    //   granularity: `day`,
      
    //   // /**
    //   //  * Demo Performance -> Pre-aggregations -> Partitioning
    //   //  */
    //   // partitionGranularity: `month`, // adds partitioning by month
    //   // refreshKey: {
    //   //   every: `1 hour`,
    //   //   updateWindow: `7 day`, // refresh partitions in this timeframe
    //   //   incremental: true // only refresh the most recent partition
    //   // },
    // }
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [lReceiptdate, lShipdate, lCommitdate]
    },
    
    lLinenumber: {
      sql: `l_linenumber`,
      type: `sum`
    }
  },
  
  dimensions: {
    lShipmode: {
      sql: `l_shipmode`,
      type: `string`
    },
    
    lLinestatus: {
      sql: `l_linestatus`,
      type: `string`
    },
    
    lDiscount: {
      sql: `l_discount`,
      type: `string`
    },
    
    lExtendedprice: {
      sql: `l_extendedprice`,
      type: `string`
    },
    
    lTax: {
      sql: `l_tax`,
      type: `string`
    },
    
    lQuantity: {
      sql: `l_quantity`,
      type: `string`
    },
    
    lShipinstruct: {
      sql: `l_shipinstruct`,
      type: `string`
    },
    
    lReturnflag: {
      sql: `l_returnflag`,
      type: `string`
    },
    
    lComment: {
      sql: `l_comment`,
      type: `string`
    },
    
    lReceiptdate: {
      sql: `l_receiptdate`,
      type: `time`
    },
    
    lShipdate: {
      sql: `l_shipdate`,
      type: `time`
    },
    
    lCommitdate: {
      sql: `l_commitdate`,
      type: `time`
    }
  },
  
  dataSource: `default`
});
