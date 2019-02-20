cube(`Users`, {
  sql: `select distinct user_fingerprint from ${Events.sql()}`,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    maxTimestamp: {
      sql: `${Events.maxTime}`,
      type: `time`,
      subQuery: true
    },

    lastSeen: {
      sql: `
        CASE
          WHEN date_diff ('hour', ${maxTimestamp}, now()) <= 23 THEN cast(date_diff('hour', ${maxTimestamp}, now()) as varchar) || ' hours ago'
          WHEN date_diff ('hour', ${maxTimestamp}, now()) > 23 THEN cast(date_diff('day', ${maxTimestamp}, now()) as varchar) || ' days ago'
          ELSE 'Unknown'
        END
      `,
      type: `string`
    },

    referrer: {
      sql: `${FirstTouch.referrer}`,
      type: `string`
    },

    id: {
      sql: `user_fingerprint`,
      type: `string`,
      primaryKey: true,
      shown: true
    }
  }
});
