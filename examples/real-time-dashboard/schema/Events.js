cube(`Events`, {
  sql: `SELECT * FROM stats.events`,

  refreshKey: {
    sql: `SELECT UNIX_TIMESTAMP()`
  },

  measures: {
    count: {
      type: `count`
    },

    online: {
      type: `countDistinct`,
      sql : `${anonymousId}`,
      filters: [
        { sql: `${timestamp} > date_sub(now(), interval 3 minute)` }
      ]
    },

    pageView: {
      type: `count`,
      filters: [
        { sql: `${eventType} = 'pageView'` }
      ]
    },

    buttonClick: {
      type: `count`,
      filters: [
        { sql: `${eventType} = 'buttonClicked'` }
      ]
    }
  },

  dimensions: {
    minutesAgo: {
      sql: `TIMESTAMPDIFF(MINUTE, timestamp, NOW())`,
      type: `number`
    },

    quaterMinutes: {
      sql: 'CONCAT(CAST(MINUTE(timestamp) as CHAR), ":", CAST((FLOOR(SECOND(timestamp)/15) + 1)*15 as CHAR))',
      type: `string`
    },

    minutesAgoHumanized: {
      type: `string`,
      case: {
        when: [
          { sql: `${minutesAgo} < 1`, label: `less than a minute ago` },
          { sql: `${minutesAgo} = 1`, label: `one minute ago` },
          { sql: `${minutesAgo} <= 10`, label: { sql: `${minutesAgo}` } },
        ],
        else: { label: "more then 10 minutes ago" }
      }
    },

    anonymousId: {
      sql: `anonymousId`,
      type: `string`
    },

    eventType: {
      sql: `eventType`,
      type: `string`
    },

    timestamp: {
      sql: `timestamp`,
      type: `time`
    }
  }
});

const derivedTables =
  `
      with generator as (
        select 0 as d union all
        select 1 union all select 2 union all select 3 union all
        select 4 union all select 5 union all select 6 union all
        select 7 union all select 8 union all select 9
    ),
    seq as (
      SELECT ( hii.d * 100 + hi.d * 10 + lo.d ) AS num
        FROM generator lo
          , generator hi,
          generator hii
          order by num
          limit 250
    ),
    series as (
    SELECT
      DATE_SUB(now(), INTERVAL seq.num SECOND) AS timestamp
    from seq
    ),
    unioned as (
    select
      1 as events,
      events.timestamp
    from stats.events
    union all
    select
      0,
      series.timestamp
    from series
    )
  `

cube(`EventsBucketed`, {
  sql:
  `
    ${derivedTables}
    select * from unioned
    WHERE ${FILTER_PARAMS.EventsBucketed.time.filter('timestamp')}
  `,

  refreshKey: {
    //sql: `select (FLOOR(SECOND(max(a.timestamp))/15))*15 from ${CUBE.sql()} as a `
    sql: `select (FLOOR(UNIX_TIMESTAMP(now())/15))*15`
  },

  measures: {
    events: {
      type: `sum`,
      sql: `events`
    }
  },

  dimensions: {
    time: {
      sql: `timestamp`,
      type: `time`
    },

    quarter: {
      sql:`
        STR_TO_DATE(CONCAT(
          DATE_FORMAT(timestamp, '%H:%i'), ":",
          LPAD(CAST((FLOOR(SECOND(timestamp)/15))*15 as CHAR), 2, 0)
        ), "%H:%i:%s")`,
      type: `time`
    }
  }
})
