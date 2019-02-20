import StructuredEvent from './StructuredEvent';

const regexp = (key) => `&${key}=([^&]+)`;
const parameters = {
  event: regexp('e'),
  event_id: regexp('eid'),
  true_tstamp: regexp('ttm'),
  user_fingerprint: regexp('fp'),
  se_category: regexp('se_ca'),
  se_action: regexp('se_ac'),
  page_referrer: regexp('refr'),
  page_title: regexp('page')
}

const customEvents = [
  ['Navigation', 'Menu Opened'],
  ['Navigation', 'Menu Closed'],
  ['Reports', 'Event Selected'],
  ['Reports', 'Property Selected'],
  ['Reports', 'Date Range Changed'],
  ['Reports', 'Visualization Changed']
].map(event =>
  new StructuredEvent(...event)
);

cube(`Events`, {
  sql:
  `SELECT
    from_iso8601_timestamp(to_iso8601(date) || 'T' || "time") as time,
    ${Object.keys(parameters).map((key) => ( `regexp_extract(querystring, '${parameters[key]}', 1) as ${key}` )).join(", ")}
  FROM cloudfront_logs
  WHERE length(querystring) > 1
  `,

  joins: {
    Users: {
      relationship: `belongsTo`,
      sql: `${CUBE}.user_fingerprint = ${Users.id}`
    }
  },

  measures: Object.assign(customEvents.reduce((accum, e) => {
    accum[e.systemName] = {
      title: `${e.humanName} - Total`,
      type: `count`,
      filters: [
        { sql: `${CUBE.event} = '${e.humanName}'` }
      ]
    }
    accum[`${e.systemName}Uniq`] = {
      title: `${e.humanName} - Unique`,
      type: `countDistinct`,
      sql: `user_fingerprint`,
      filters: [
        { sql: `${CUBE.event} = '${e.humanName}'` }
      ]
    }
    return accum
  }, {}), {
    anyEvent: {
      type: `count`,
      title: `Any Event - Total`
    },

    anyEventUniq: {
      sql: `user_fingerprint`,
      type: `countDistinct`,
      title: `Any Event - Unique`
    },

    pageView: {
      type: `count`,
      title: `Page View`,
      filters: [
        { sql: `${CUBE.event} = 'Page View'` }
      ]
    },

    pageViewUniq: {
      type: `countDistinct`,
      title: `Page View - Unique`,
      sql: `user_fingerprint`,
      filters: [
        { sql: `${CUBE.event} = 'Page View'` }
      ]
    },

    // Used for subquery in Users cube
    maxTime: {
      type: `max`,
      sql: `time`
    }
  }),

  dimensions: {
    event: {
      type: `string`,
      case: {
        when: customEvents.map(e => (
          { sql: `${CUBE}.event = 'se'
                  AND ${CUBE}.se_category = '${e.categoryEscaped}'
                  AND ${CUBE}.se_action = '${e.actionEscaped}'`,
            label: e.humanName }
        )).concat([
          { sql: `${CUBE}.event = 'pv'`, label: `Page View` },
        ]),
        else: {
          label: `Unknown event`
        }
      }
    },

    referrer: {
      sql: `page_referrer`,
      type: `string`
    },

    pageTitle: {
      sql: `replace(page_title, '')`,
      type: `string`
    },

    time: {
      sql: `time`,
      type: `time`
    },

    id: {
      sql: `event_id`,
      type: `string`,
      primaryKey: true
    }
  }
});
