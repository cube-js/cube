cube('OnlineUsers', {
  sql: `SELECT * FROM USERS_LAST_TIMESTAMP_NEW`,

  preAggregations: {
    main: {
      measures: [count],
      dimensions: [anonymousId],
      timeDimension: lastSeen,
      granularity: `second`
    }
  },

  measures: {
    count: {
      type: `count`,
      sql: `ANONYMOUSID`
    }
  },

  dimensions: {
    anonymousId: {
      sql: `ANONYMOUSID`,
      type: `string`
    },

    lastSeen: {
      type: `time`,
      sql: `MAX_TIME`
    }
  }
});