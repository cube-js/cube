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
      type: `number`,
      sql: `${totalTimeOnPageSeconds} / NULLIF(${count}, 0)`
    },

    totalTimeOnPageSeconds: {
      sql: `${timeOnPageSeconds}`,
      type: `sum`
    }
  },

  dimensions: {
    // FIXME
    timeOnPageSeconds: {
      type: `number`,
      sql: `date_diff('second', ${CUBE}.derived_tstamp, ${CUBE}.next_event_time)`
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
    },

    pageTitle: {
      type: `string`,
      case: {
        when: [
          { sql: `${CUBE}.page_title != ''`, label: { sql: `${CUBE}.page_title` } }
        ],
        else: { label: '(not set)' }
      }
    }
  },

  preAggregations: {
    additive: {
      type: `rollup`,
      measureReferences: [pageviews, exits, count, totalTimeOnPageSeconds],
      timeDimensionReference: time,
      dimensionReferences: [pageUrlPath, pageTitle],
      segmentReferences: [
        Sessions.bouncedSessions,
        Sessions.directTraffic,
        Sessions.searchTraffic,
        Sessions.newUsers
      ],
      granularity: `hour`,
      refreshKey: {
        every: `5 minutes`
      },
      external: true,
      scheduledRefresh: true
    }
  }
});

cube(`PageUsers`, {
  extends: PageViews,

  sql: `select distinct
  date_trunc('hour', derived_tstamp) as derived_tstamp,
  session_id
  from ${Events.sql()}`,

  preAggregations: {
    main: {
      type: `originalSql`,
      refreshKey: {
        every: `5 minutes`
      },
      partitionGranularity: `month`,
      timeDimensionReference: time,
      external: true,
      scheduledRefresh: true
    }
  }
});
