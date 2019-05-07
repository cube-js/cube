cube(`Events`, {
  sql: `
  SELECT * FROM hn_insights.events 
  WHERE ${FILTER_PARAMS.Events.timestamp.filter(`from_iso8601_timestamp(dt || ':00:00.000')`)}
  `,

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

    scoreChangeLastHour: {
      sql: `score_diff`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} + interval '60' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    scoreChangePrevHour: {
      sql: `score_diff`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} + interval '60' minute < now()`
      }, {
        sql: `${timestamp} + interval '120' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    commentsChangeLastHour: {
      sql: `comments_count_diff`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} + interval '60' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    commentsChangePrevHour: {
      sql: `comments_count_diff`,
      type: `sum`,
      filters: [{
        sql: `${timestamp} + interval '60' minute < now()`
      }, {
        sql: `${timestamp} + interval '120' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    rankHourAgo: {
      sql: `rank`,
      type: `min`,
      filters: [{
        sql: `${timestamp} + interval '60' minute < now()`
      }, {
        sql: `${timestamp} + interval '65' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    currentRank: {
      sql: `rank`,
      type: `min`,
      filters: [{
        sql: `${timestamp} + interval '5' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    currentScore: {
      sql: `score`,
      type: `max`,
      filters: [{
        sql: `${timestamp} + interval '5' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    currentComments: {
      sql: `comments_count`,
      type: `max`,
      filters: [{
        sql: `${timestamp} + interval '5' minute > now()`
      }, {
        sql: `${page} = 'front'`
      }]
    },

    commentsChange: {
      sql: `comments_count_diff`,
      type: `sum`
    },

    topRank: {
      sql: `rank`,
      type: `min`,
      filters: [{
        sql: `${page} = 'front'`
      }]
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

    minutesOnFirstPage: {
      sql: `date_diff('second', ${prevSnapshotTimestamp}, ${snapshotTimestamp}) / 60.0`,
      type: `sum`,
      filters: [{
        sql: `${rank} < 31`
      }, {
        sql: `${page} = 'front'`
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
    },

    rank: {
      sql: `rank`,
      type: `number`
    }
  },

  preAggregations: {
    perStory: {
      type: `rollup`,
      measureReferences: [scoreChange, commentsChange, topRank],
      dimensionReferences: [Stories.id, Events.page],
      timeDimensionReference: timestamp,
      granularity: `hour`,
      partitionGranularity: `day`
    }
  }
});
