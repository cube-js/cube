cube(`Events`, {
  refreshKey: {
    every: `1 day`
  },

  sql: `
    select
      event_id,
      platform,
      event,

      -- user
      domain_userid,

      -- time
      derived_tstamp,
      LEAD(derived_tstamp) OVER(PARTITION BY domain_sessionid ORDER BY derived_tstamp) AS next_event_time,

      -- session
      domain_sessionid AS session_id,
      domain_sessionidx AS session_index,

      -- page
      page_urlpath AS page_url_path,
      page_title as page_title,

      -- browser info
      br_lang,
      br_name,

      -- Location
      geo_country,
      geo_region,
      geo_city,
      geo_zipcode,
      geo_latitude,
      geo_longitude,
      geo_region_name,

      -- referrer
      refr_urlhost || refr_urlpath AS referrer_url,
      refr_urlscheme AS referrer_url_scheme,
      refr_urlhost AS referrer_url_host,
      refr_urlport AS referrer_url_port,
      refr_urlpath AS referrer_url_path,
      refr_urlquery AS referrer_url_query,
      refr_urlfragment AS referrer_url_fragment,
      CASE
        WHEN refr_medium IS NULL THEN 'direct'
        WHEN refr_medium = 'unknown' THEN 'other'
        ELSE refr_medium
      END AS referrer_medium,
      refr_source AS referrer_source,
      refr_term AS referrer_term,

      -- marketing
      mkt_medium AS marketing_medium,
      mkt_source AS marketing_source,
      mkt_term AS marketing_term,
      mkt_content AS marketing_content,
      mkt_campaign AS marketing_campaign,
      mkt_clickid AS marketing_click_id,
      mkt_network AS marketing_network,


      ROW_NUMBER() OVER (PARTITION BY domain_userid ORDER BY derived_tstamp) AS event_index,
      ROW_NUMBER() OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) AS event_in_session_index,
      MAX(derived_tstamp) OVER (PARTITION BY domain_sessionid) as exit_time


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
      type: `countDistinctApprox`,
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
