cube(`IntraEvents`, {
  sql: `SELECT * FROM "hn-insights".intra_events`,

  measures: {
    count: {
      type: `count`
    },

    scorePerMinute: {
      sql: `scorediff / 60.0`,
      type: `sum`,
      rollingWindow: {
        trailing: `1 hour`
      }
    },

    score: {
      sql: `scorediff`,
      type: `sum`
    },

    commentsCount: {
      sql: `commentscountdiff`,
      type: `sum`
    }
  },

  dimensions: {
    title: {
      sql: `title`,
      type: `string`
    },

    user: {
      sql: `user`,
      type: `string`
    },

    href: {
      sql: `href`,
      type: `string`
    },

    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true
    },

    event: {
      sql: `event`,
      type: `string`
    },

    timestamp: {
      sql: `from_iso8601_timestamp(timestamp)`,
      type: `time`
    },

    page: {
      sql: `page`,
      type: `string`
    }
  }
});
