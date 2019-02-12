const regexp = (key) => `&${key}=(.*?)&`;
const parameters = {
  event: regexp('e'),
  true_tstamp: regexp('ttm'),
  user_fingerprint: regexp('fp'),
  se_category: regexp('se_ca'),
  se_action: regexp('se_ac')
}

cube(`Events`, {
  sql:
  `SELECT
    from_iso8601_timestamp(to_iso8601(date) || 'T' || "time") as time,
    ${Object.keys(parameters).map((key) => ( `regexp_extract(querystring, '${parameters[key]}', 1) as ${key}` )).join(", ")}
  FROM
  cloudfront_logs`,

  measures: {
    count: {
      type: `count`,
    },

    uniqCount: {
      sql: `user_fingerprint`,
      type: `countDistinct`
    }
  },

  dimensions: {
    event: {
      type: `string`,
      case: {
        when: [
          { sql: `${CUBE}.event = 'pv'`, label: `Page View` },
          { sql: `${CUBE}.event = 'se' AND ${CUBE}.se_category = 'Navigation' AND ${CUBE}.se_action = 'Menu%2520Opened'`, label: `Navigation: Menu Opened` },
          { sql: `${CUBE}.event = 'se' AND ${CUBE}.se_category = 'Navigation' AND ${CUBE}.se_action = 'Menu%2520Closed'`, label: `Navigation: Menu Closed` }
        ],
        else: {
          label: `Unknown event`
        }
      }
    }
  }
});
