cube(`Sessions`, {
  sql: `
   WITH aggregates AS (
     SELECT
       e.session_id,

       MAX(e.derived_tstamp) AS session_end
     FROM ${Events.sql()} e
     GROUP BY 1
   )

   SELECT
     e.session_id,
     e.session_index,
     e.derived_tstamp as session_start,
     a.session_end as session_end,

     e.br_lang,
     e.br_name,

     e.domain_userid
   FROM ${Events.sql()} AS e

   INNER JOIN aggregates AS a
     ON e.session_id = a.session_id

   WHERE e.event_in_session_index = 1
  `,

  measures: {
    count: {
      type: `count`
    },

    usersCount: {
      type: `countDistinct`,
      sql: `domain_userid`
    },

    newUsersCount: {
      type: `countDistinct`,
      sql: `domain_userid`,
      filters: [
        { sql: `${type} = 'New'` }
      ]
    },

    // Engagement
    bouncedCount: {
      sql: `${id}`,
      type: `count`,
      filters:[{
        sql: `${isBounced} = 'True'`
      }]
    },

    bounceRate: {
      sql: `100.00 * ${bouncedCount} / NULLIF(${count}, 0)`,
      type: `number`,
      format: `percent`
    },

    averageDurationSeconds: {
      type: `avg`,
      sql: `${durationSeconds}`
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

    // TODO: can be done via subquery
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
      }
    },


    // Audience
    language: {
      sql: `br_lang`,
      type: `string`
    },

    browser: {
      sql: `br_name`,
      type: `string`
    },

    // Engagement
    numberEvents: {
      sql: `${Events.count}`,
      type: `number`,
      subQuery: true
    },

    isBounced: {
     type: `string`,
      case: {
        when: [ { sql: `${numberEvents} = 1`, label: `True` }],
        else: { label: `False` }
      }
    }
  }
});
