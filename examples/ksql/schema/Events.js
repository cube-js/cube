cube(`Events`, {
  sql: `SELECT * FROM EVENTS`,

  preAggregations: {
    main: {
      measures: [count],
      dimensions: [type, anonymousId],
      timeDimension: time,
      granularity: `second`
    },
    userCountHour: {
      measures: [userCount],
      timeDimension: time,
      granularity: `hour`
    }
  },

  measures: {
    count: {
      type: `count`
    },

    userCount: {
      sql: `COUNT_DISTINCT(ANONYMOUSID)`,
      type: `number`
    },
  },

  dimensions: {
    time: {
      sql: `TIMESTAMP`,
      type: `time`
    },

    anonymousId: {
      sql: `ANONYMOUSID`,
      type: `string`
    },

    type: {
      sql: `TYPE`,
      type: `string`
    }
  }
});