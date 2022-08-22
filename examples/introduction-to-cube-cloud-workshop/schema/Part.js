cube(`Part`, {
  sql: `SELECT * FROM public.part`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [pName]
    }
  },
  
  dimensions: {
    pName: {
      sql: `p_name`,
      type: `string`
    },
    
    pType: {
      sql: `p_type`,
      type: `string`
    },
    
    pComment: {
      sql: `p_comment`,
      type: `string`
    },
    
    pMfgr: {
      sql: `p_mfgr`,
      type: `string`
    },
    
    pBrand: {
      sql: `p_brand`,
      type: `string`
    },
    
    pRetailprice: {
      sql: `p_retailprice`,
      type: `string`
    },
    
    pContainer: {
      sql: `p_container`,
      type: `string`
    }
  },
  
  dataSource: `default`
});
