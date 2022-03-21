cube(`Region`, {
  sql: `SELECT * FROM tpc_h.region`,
  
  preAggregations: {
    // Pre-Aggregations definitions go here
    // Learn more here: https://cube.dev/docs/caching/pre-aggregations/getting-started  
  },
  
  joins: {
    
  },

  /**
   * Step 9.
   * Multi-tenancy
   */
  segments: {
    africa: {
      sql: `${CUBE}.\`R_REGIONKEY\` = 0`,
    },
    america: {
      sql: `${CUBE}.\`R_REGIONKEY\` = 1`,
    },
    asia: {
      sql: `${CUBE}.\`R_REGIONKEY\` = 2`,
    },
    europe: {
      sql: `${CUBE}.\`R_REGIONKEY\` = 3`,
    },
    middleeast: {
      sql: `${CUBE}.\`R_REGIONKEY\` = 4`,
    },
  },
  
  measures: {
    count: {
      type: `count`,
      drillMembers: [rName]
    }
  },
  
  dimensions: {
    rRegionkey: {
      sql: `${CUBE}.\`R_REGIONKEY\``,
      type: `number`,
      primaryKey: true,
      shown: true
    },

    rName: {
      sql: `${CUBE}.\`R_NAME\``,
      type: `string`
    },
    
    rComment: {
      sql: `${CUBE}.\`R_COMMENT\``,
      type: `string`
    }
  }
});
