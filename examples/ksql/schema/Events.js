cube(`Events`, {
  sql: `SELECT * FROM EVENTS`,

  preAggregations: {
    main: {
      measures: [count],
      dimensions: [type],
      timeDimension: time,
      granularity: `second`
    }
  },

  measures: {
    count: {
      type: `count`
    },

    userCount: {
      sql: `ANONYMOUSID`,
      type: `countDistinct`
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