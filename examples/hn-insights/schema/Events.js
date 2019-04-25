cube(`Events`, {
  sql: `SELECT * FROM "hn-insights".intra_events`,

  refreshKey: {
    sql: `select current_timestamp`
  },

  measures: {
    count: {
      type: `count`
    },

    scorePerMinute: {
      sql: `${scoreChange} / ${eventTimeSpan}`,
      type: `number`
    },

    eventTimeSpan: {
      sql: `date_diff('second', ${prevSnapshotTimestamp}, ${snapshotTimestamp}) / 60.0`,
      type: `sum`
    },

    scoreChange: {
      sql: `score_diff`,
      type: `sum`
    },

    commentsCount: {
      sql: `comments_count_diff`,
      type: `sum`
    },

    topRank: {
      sql: `rank`,
      type: `min`
    },

    addedToFrontPage: {
      sql: `${timestamp}`,
      type: `min`,
      filters: [{
        sql: `event = 'added'`
      }, {
        sql: `page = 'front'`
      }],
      shown: false
    },

    postedTime: {
      sql: `${timestamp}`,
      type: `min`,
      filters: [{
        sql: `event = 'added'`
      }, {
        sql: `page = 'newest'`
      }],
      shown: false
    },

    commentsBeforeAddedToFrontPage: {
      sql: `comments_count_diff`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} < ${Stories.addedToFrontPage}`
      }, {
        sql: `page = 'newest'`
      }]
    },

    scoreChangeBeforeAddedToFrontPage: {
      sql: `score_diff`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} < ${Stories.addedToFrontPage}`
      }, {
        sql: `page = 'newest'`
      }]
    },

    eventTimeSpanBeforeAddedToFrontPage: {
      sql: `date_diff('second', ${prevSnapshotTimestamp}, ${snapshotTimestamp}) / 60.0`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} < ${Stories.addedToFrontPage}`
      }, {
        sql: `page = 'newest'`
      }],
      shown: false
    },

    scorePerMinuteWhenAddedToFrontPage: {
      sql: `${scoreChangeBeforeAddedToFrontPage} / ${eventTimeSpanBeforeAddedToFrontPage}`,
      type: `number`
    },
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
      sql: `id || timestamp`,
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

    snapshotTimestamp: {
      sql: `from_iso8601_timestamp(snapshot_timestamp)`,
      type: `time`
    },

    prevSnapshotTimestamp: {
      sql: `from_iso8601_timestamp(prev_snapshot_timestamp)`,
      type: `time`
    },

    page: {
      sql: `page`,
      type: `string`
    }
  }
});
