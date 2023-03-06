cube(`Nation`, {
  sql: `SELECT * FROM tpc_h.nation`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    /**
     * Step 9.
     * Multi-tenancy
     */
    Region: {
      relationship: `belongsTo`,
      sql: `${CUBE.nRegionkey} = ${Region.rRegionkey}`,
    },
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [nName]
    }
  },
  
  dimensions: {
    /**
     * Step 9
     * Multi-tenancy
     */
    nNationkey: {
      sql: `${CUBE}.\`N_NATIONKEY\``,
      type: `number`,
      primaryKey: true
    },
    nRegionkey: {
      sql: `${CUBE}.\`N_REGIONKEY\``,
      type: `number`
    },
    /**
     * Step 9 end
     */

    nName: {
      sql: `${CUBE}.\`N_NAME\``,
      type: `string`
    },
    
    nComment: {
      sql: `${CUBE}.\`N_COMMENT\``,
      type: `string`
    }
  }
});
