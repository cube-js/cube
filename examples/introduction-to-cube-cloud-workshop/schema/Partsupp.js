cube(`Partsupp`, {
  sql: `SELECT * FROM public.partsupp`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: []
    },
    
    psAvailqty: {
      sql: `ps_availqty`,
      type: `sum`
    }
  },
  
  dimensions: {
    psComment: {
      sql: `ps_comment`,
      type: `string`
    },
    
    psSupplycost: {
      sql: `ps_supplycost`,
      type: `string`
    }
  },
  
  dataSource: `default`
});
