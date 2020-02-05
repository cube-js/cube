cube(`Events`, {
  sql: `
    select
      event_id,
      platform,
      event,

      -- user
      domain_userid,

      -- time
      derived_tstamp,

      -- session
      domain_sessionid AS session_id,
      domain_sessionidx AS session_index,

      br_lang,
      br_name,

      ROW_NUMBER() OVER (PARTITION BY domain_userid ORDER BY derived_tstamp) AS event_index,
      ROW_NUMBER() OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) AS event_in_session_index


    FROM cubejs_snowplow_events.cubejs_snowplow_events
    -- WHERE a.useragent NOT SIMILAR TO '%(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PingdomBot|PhantomJS|YandexBot|Twitterbot|a_archiver|facebookexternalhit|Bingbot|BingPreview|Googlebot|Baiduspider|360(Spider|User-agent)|semalt)%'
  `,

  joins: {
    Sessions: {
      relationship: `belongsTo`,
      sql: `${CUBE}.session_id = ${Sessions.id}`
    }
  },

  measures: {
    count: {
      type: `count`
    },

    usersCount: {
      type: `countDistinct`,
      sql: `domain_userid`,
      title: `Users`
    }
  },

  dimensions: {
    id: {
      sql: `event_id`,
      type: `string`,
      primaryKey: true
    },

    time: {
      sql: `derived_tstamp`,
      type: `time`
    }
  }
});
