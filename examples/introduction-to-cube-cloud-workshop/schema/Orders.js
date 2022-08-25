cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  
  preAggregations: {

    /**
     * Demo: Dev Tooling - Dev mode
     * 
     */
    // ordersByDay: {
    //   measures: [Orders.count],
    //   timeDimension: Orders.oOrderdate,
    //   granularity: `day`
    // }
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [oOrderdate]
    }
  },
  
  dimensions: {
    oTotalprice: {
      sql: `o_totalprice`,
      type: `string`
    },
    
    oComment: {
      sql: `o_comment`,
      type: `string`
    },
    
    oOrderstatus: {
      sql: `o_orderstatus`,
      type: `string`
    },
    
    oOrderpriority: {
      sql: `o_orderpriority`,
      type: `string`
    },
    
    oClerk: {
      sql: `o_clerk`,
      type: `string`
    },
    
    oOrderdate: {
      sql: `o_orderdate`,
      type: `time`
    }
  },
  
  dataSource: `default`
});
