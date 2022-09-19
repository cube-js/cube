cube(`Events`, {
  sql: `SELECT * FROM EVENTS`,

  measures: {
    count: {
      type: `count`
    }
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
  },

  preAggregations: {
    original: {
      external: true,
      type: `originalSql`,
      uniqueKeyColumns: [ '`MESSAGEID`' ]
    }
  }
})