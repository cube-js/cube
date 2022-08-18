import { env } from '../env'

cube(`GdeltEvents`, {
  sql: `
      SELECT time, code
      FROM (
        SELECT 
          PARSE_TIMESTAMP("%Y%m%d", CAST(SQLDATE AS STRING)) AS time,
          EventCode AS code
        FROM \`gdelt-bq.gdeltv2.events\`
      )
      WHERE time BETWEEN TIMESTAMP("2019-01-01") AND TIMESTAMP("2020-01-01")
  `,

  refreshKey: {
    every: '1 minute'
  },

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    code: {
      sql: `code`,
      type: `string`
    },

    time: {
      sql: `time`,
      type: `time`
    },
  },

  preAggregations: {
      main: {
        type: `rollup`,
        external: true,
        scheduledRefresh: true,
        refreshKey: { every: '1 hour' },
        measureReferences: [ count ],
        dimensionReferences: [ code ],
        timeDimensionReference: time,
        granularity: 'day',
        partitionGranularity: 'month',
        unionWithSourceData: !!env.CUBEJS_TEST_USE_LAMBDA,
      },
    },
});