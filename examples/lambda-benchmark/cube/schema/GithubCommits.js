import { env } from '../env'

cube(`GithubCommits`, {
  sql: `
      SELECT 
        author.time_sec AS time_sec,
        -- 10x fewer rows
        SUBSTR(repo, 0, 2) AS repo
      FROM \`bigquery-public-data.github_repos.commits\` AS commits
      CROSS JOIN UNNEST(commits.repo_name) AS repo
      WHERE TIMESTAMP_SECONDS(author.time_sec) BETWEEN TIMESTAMP("2019-01-01") AND TIMESTAMP("2020-01-01")
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
    repo: {
      sql: `repo`,
      type: `string`
    },

    date: {
      sql: `TIMESTAMP_SECONDS(time_sec)`,
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
        dimensionReferences: [ repo ],
        timeDimensionReference: date,
        granularity: 'day',
        // granularity: 'week',
        partitionGranularity: 'month',
        unionWithSourceData: env.CUBEJS_TEST_USE_LAMBDA,
      },
    },
});