cube(`Supplier`, {
  sql: `SELECT * FROM public.supplier`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [sName]
    }
  },
  
  dimensions: {
    sComment: {
      sql: `s_comment`,
      type: `string`
    },
    
    sAcctbal: {
      sql: `s_acctbal`,
      type: `string`
    },
    
    sPhone: {
      sql: `s_phone`,
      type: `string`
    },
    
    sName: {
      sql: `s_name`,
      type: `string`
    },
    
    sAddress: {
      sql: `s_address`,
      type: `string`
    }
  },
  
  dataSource: `default`
});
