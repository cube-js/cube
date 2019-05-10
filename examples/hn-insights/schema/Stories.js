cube(`Stories`, {
  sql: `SELECT distinct stories.id, last_event.title, last_event.href, last_event.user FROM (
    SELECT id, max(from_iso8601_timestamp(timestamp)) last_timestamp FROM ${Events.sql()} GROUP BY 1
  ) stories
  LEFT JOIN ${Events.sql()} last_event 
  ON last_event.id = stories.id AND from_iso8601_timestamp(last_event.timestamp) = stories.last_timestamp
  `,

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

    avgMinutesToFrontPage: {
      sql: `${minutesToFrontPage}`,
      type: `avg`
    }
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `string`,
      primaryKey: true,
      shown: true
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

    minutesToFrontPage: {
      sql: `date_diff('minute', ${postedTime}, ${addedToFrontPage})`,
      type: `number`
    }
  }
});
