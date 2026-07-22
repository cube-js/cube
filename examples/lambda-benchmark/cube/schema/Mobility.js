import { env } from '../env'

cube(`Mobility`, {
  sql: `
      SELECT time, country
      FROM (
        SELECT 
          TIMESTAMP(date) AS time,
          country_region AS country
        FROM \`bigquery-public-data.covid19_google_mobility.mobility_report\`
      )
      WHERE time BETWEEN TIMESTAMP("2021-01-01") AND TIMESTAMP("2022-01-01")
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
    country: {
      sql: `country`,
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
        dimensionReferences: [ country ],
        timeDimensionReference: time,
        granularity: 'day',
        partitionGranularity: 'month',
        unionWithSourceData: !!env.CUBEJS_TEST_USE_LAMBDA,
      },
    },
});