cube(`Stories`, {
  sql: `SELECT 
    distinct stories.id, last_event.title, last_event.href, last_event.user,
    last_front_event.rank, last_front_event.rank_score, last_event.score, last_event.comments_count,
    last_front_event.timestamp as last_front_event_timestamp
  FROM (
    SELECT id, max(from_iso8601_timestamp(timestamp)) last_timestamp FROM ${Events.sql()} GROUP BY 1
  ) stories
  LEFT JOIN ${Events.sql()} last_event
  ON last_event.id = stories.id AND from_iso8601_timestamp(last_event.timestamp) = stories.last_timestamp
  LEFT JOIN (
    SELECT id, max(from_iso8601_timestamp(timestamp)) last_timestamp FROM ${Events.sql()} WHERE page = 'front' GROUP BY 1
  ) last_front_event_time ON last_front_event_time.id = stories.id
  LEFT JOIN ${Events.sql()} last_front_event
  ON last_front_event.id = last_front_event_time.id AND 
  from_iso8601_timestamp(last_front_event.timestamp) = last_front_event_time.last_timestamp AND
  last_front_event.page = 'front'
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

    addedToFrontHour: {
      sql: `hour(${addedToFrontPage})`,
      type: `number`
    },

    addedToFrontDay: {
      case: {
        when: [{
          sql: `day_of_week(${addedToFrontPage}) = 1`,
          label: `1. Monday`
        }, {
          sql: `day_of_week(${addedToFrontPage}) = 2`,
          label: `2. Tuesday`
        }, {
          sql: `day_of_week(${addedToFrontPage}) = 3`,
          label: `3. Wednesday`
        }, {
          sql: `day_of_week(${addedToFrontPage}) = 4`,
          label: `4. Thursday`
        }, {
          sql: `day_of_week(${addedToFrontPage}) = 5`,
          label: `5. Friday`
        }, {
          sql: `day_of_week(${addedToFrontPage}) = 6`,
          label: `6. Saturday`
        }, {
          sql: `day_of_week(${addedToFrontPage}) = 7`,
          label: `7. Sunday`
        }
        ]
      },
      type: `number`
    },

    lastEventTime: {
      sql: `${Events.lastEventTime}`,
      type: `time`,
      subQuery: true
    },

    lastFrontEventTime: {
      sql: `cast(from_iso8601_timestamp(${CUBE}.last_front_event_timestamp) as timestamp)`,
      type: `time`
    },

    ageInHours: {
      sql: `date_diff('minute', ${postedTime}, current_timestamp) / 60.0`,
      type: `number`
    },

    currentRank: {
      sql: `CASE WHEN ${lastFrontEventTime} + interval '5' minute > now() THEN ${CUBE}.rank END`,
      type: `number`
    },

    currentRankScore: {
      sql: `CASE 
        WHEN ${lastFrontEventTime} + interval '5' minute > now() THEN ${CUBE}.rank_score 
        WHEN ${lastFrontEventTime} is null THEN pow(${currentScore}, 0.8) / pow(date_diff('second', ${postedTime}, now())/3600.0 + 2, 1.8) 
        END`,
      type: `number`
    },


    currentScore: {
      sql: `CAST(NULLIF(${CUBE}.score, '') as integer)`,
      type: `number`
    },

    currentComments: {
      sql: `comments_count`,
      type: `number`
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
