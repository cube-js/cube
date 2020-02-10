cube(`PageViews`, {
  extends: Events,
  sql: `
    SELECT
    *
    FROM ${Events.sql()} events
    WHERE events.platform = 'web' AND events.event = 'page_view'
  `,

  measures: {
    pageviews: {
      type: `count`,
      description: `Pageviews is the total number of pages viewed. Repeated views of a single page are counted.`
    },

    exits: {
      type: `count`,
      filters: [
        { sql: `${CUBE.exitPage} = 'Yes'` }
      ]
    },

    exitPercent: {
      type: `number`,
      sql: `100.0 * ${CUBE.exits} / ${CUBE.pageviews}`,
      format: `percent`
    },

    uniqPageviews: {
      type: `countDistinct`,
      sql: `session_id`
    },

    averageTimeOnPageSeconds: {
      type: `avg`,
      sql: `${timeOnPageSeconds}`
    }
  },

  dimensions: {
    // FIXME
    timeOnPageSeconds: {
      type: `number`,
      sql: `date_diff('second', derived_tstamp, next_event_time)`
    },

    exitPage: {
      type: `string`,
      case: {
        when: [
          { sql: `${CUBE}.exit_time = ${CUBE}.derived_tstamp`, label: `Yes` }
        ],
        else: { label: `No` }
      }
    },

    pageUrlPath: {
      sql: `page_url_path`,
      type: `string`
    }
  }
});
