cube(`Sessions`, {
  sqlAlias: `ss`,
  refreshKey: {
    every: `5 minutes`
  },

  sql: `
   WITH aggregates AS (
     SELECT
       e.session_id,

       MAX(e.derived_tstamp) AS session_end,
       count(e.event_id) events_count
     FROM ${Events.sql()} e
     GROUP BY 1
   )

   SELECT
    e.*,
    e.derived_tstamp as session_start,

    a.session_end as session_end,
    a.events_count as events_count
   FROM ${Events.sql()} AS e

   INNER JOIN aggregates AS a
     ON e.session_id = a.session_id

   WHERE e.event_in_session_index = 1
  `,

  joins: {
    Users: {
      relationship: `belongsTo`,
      sql: `${CUBE}.domain_userid = ${Users.id}`
    }
  },

  measures: {
    count: {
      type: `count`,
      title: `Sessions`
    },

    usersCount: {
      type: `countDistinct`,
      sql: `domain_userid`,
      title: "Users"
    },

    newUsersCount: {
      type: `countDistinct`,
      sql: `domain_userid`,
      filters: [
        { sql: `${type} = 'New'` }
      ],
      title: "New Users"
    },

    sessionsPerUser: {
      sql: `1.000 * ${count} / NULLIF(${usersCount}, 0)`,
      type: `number`,
      title: `Number of Sessions per User`
    },

    // Engagement
    bouncedCount: {
      sql: `${id}`,
      type: `count`,
      filters:[{
        sql: `${isBounced} = 'True'`
      }],
      title: `Bounces`
    },

    bounceRate: {
      sql: `100.00 * ${bouncedCount} / NULLIF(${count}, 0)`,
      type: `number`,
      format: `percent`,
      title: `Bounce Rate`
    },

    averageDurationSeconds: {
      type: `number`,
      sql: `${totalDuration} / NULLIF(${count}, 0)`
    },

    totalDuration: {
      sql: `${durationSeconds}`,
      type: `sum`
    }
  },

  dimensions: {
    id: {
      sql: `session_id`,
      type: `string`,
      primaryKey: true
    },

    sessionStart: {
      sql: `session_start`,
      type: `time`
    },

    sessionEnd: {
      sql: `session_end`,
      type: `time`
    },

    durationSeconds: {
      sql: `date_diff('second', ${CUBE.sessionStart}, ${sessionEnd})`,
      type: `number`
    },

    sessionIndex: {
      sql: `session_index`,
      type: `number`
    },

    type: {
      type: `string`,
      case: {
        when: [{ sql: `${CUBE.sessionIndex} = 1`, label: `New`}],
        else: { label: `Returning` }
      },
      title: `User Type`
    },

    // Audience
    // Demographics
    language: {
      sql: `br_lang`,
      type: `string`
    },

    country: {
      sql: `geo_country`,
      type: `string`
    },

    city: {
      sql: `geo_city`,
      type: `string`
    },

    // System
    browser: {
      sql: `br_name`,
      type: `string`
    },

    // Engagement
    numberEvents: {
      sql: `events_count`,
      type: `number`
    },

    isBounced: {
     type: `string`,
      case: {
        when: [ { sql: `${numberEvents} = 1`, label: `True` }],
        else: { label: `False` }
      }
    },

    // Acquisition
    landingPage: {
      type: `string`,
      sql: `page_url_path`,
      title: `Landing Page`
    },

    referrerMedium: {
      type: `string`,
      case: {
        when: [
          { sql: `${CUBE}.referrer_medium != ''`, label: { sql: `${CUBE}.referrer_medium` } }
        ],
        else: { label: '(none)' }
      },
      title: `Medium`
    },

    referrerSource: {
      type: `string`,
      case: {
        when: [
          { sql: `${CUBE}.referrer_source != ''`, label: { sql: `${CUBE}.referrer_source` } }
        ],
        else: { label: '(none)' }
      },
      title: `Source`
    },

    sourceMedium: {
      type: `string`,
      sql: `concat(${CUBE.referrerSource}, " / ", ${CUBE.referrerMedium})`,
      title: `Source / Medium`
    }
  },

  segments: {
    bouncedSessions: {
      sql: `${isBounced} = 'True'`,
    },
    directTraffic: {
      sql: `${referrerMedium} = '(none)'`
    },
    searchTraffic: {
      sql: `${referrerMedium} = 'search'`
    },
    newUsers: {
      sql: `${type} = 'New'`
    }
  },

  preAggregations: {
    original: {
      type: `originalSql`,
      refreshKey: {
        every: `5 minutes`
      },
      external: true,
      scheduledRefresh: true
    },
    additive: {
      type: `rollup`,
      measureReferences: [totalDuration, bouncedCount, count],
      segmentReferences: [bouncedSessions, directTraffic, searchTraffic, newUsers],
      dimensionReferences: [landingPage],
      timeDimensionReference: sessionStart,
      granularity: `hour`,
      refreshKey: {
        every: `5 minutes`
      },
      external: true,
      scheduledRefresh: true
    }
  }
});

cube(`SessionUsers`, {
  extends: Sessions,
  sqlAlias: `su`,

  sql: `select distinct
  date_trunc('hour', session_start) as session_start,
  session_id,
  domain_userid,
  session_index,
  br_lang,
  page_url_path,
  geo_country,
  geo_city,
  referrer_source,
  referrer_medium,
  events_count
  from ${Sessions.sql()}`,

  preAggregations: {
    main: {
      type: `originalSql`,
      refreshKey: {
        every: `5 minutes`
      },
      external: true,
      scheduledRefresh: true,
      indexes: {
        sessionId: {
          columns: [`session_id`]
        }
      }
    }
  }
});
