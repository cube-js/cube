cube(`Stories`, {
  sql: `SELECT distinct id FROM ${Events.sql()}`,

  refreshKey: {
    sql: `select current_timestamp`
  },

  joins: {
    Events: {
      sql: `${Stories}.id = ${Events}.id`,
      relationship: `hasMany`
    }
  },

  measures: {
    count: {
      type: `count`
    },

    scorePerMinute: {
      sql: `${scoreChange} / ${eventTimeSpan}`,
      type: `number`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true
    },

    addedToFrontPage: {
      sql: `${Events.addedToFrontPage}`,
      type: `time`,
      subQuery: true
    },

    postedTime: {
      sql: `${Events.postedTime}`,
      type: `time`,
      subQuery: true
    },

    minutesToFrontPage: {
      sql: `date_diff('minute', ${postedTime}, ${addedToFrontPage})`,
      type: `number`
    }
  }
});
