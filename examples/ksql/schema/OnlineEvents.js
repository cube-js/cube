cube('OnlineUsers', {
  sql: `SELECT * FROM USERS_LAST_TIMESTAMP`,

  preAggregations: {
    original: {
      external: true,
      type: `originalSql`,
      uniqueKeyColumns: [ '`ANONYMOUSID`' ]
    }
  },

  measures: {
    count: {
      type: `countDistinct`,
      sql: `ANONYMOUSID`
    }
  },

  dimensions: {
    lastSeen: {
      type: `time`,
      sql: `KSQL_COL_0`
    }
  }
});
