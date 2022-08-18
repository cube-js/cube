import { env } from '../env'

cube(`GithubCommits`, {
  sql: `
      SELECT time, repo
      FROM (
        SELECT 
          TIMESTAMP_SECONDS(author.time_sec) AS time,
          -- SUBSTR(repo, 0, 10): 12 x 400k
          -- SUBSTR(repo, 0, 2): 12 x 40k
          -- SUBSTR(repo, 0, 1): 12 x 2k
          SUBSTR(repo, 0, 2) AS repo
        FROM \`bigquery-public-data.github_repos.commits\` AS commits
        CROSS JOIN UNNEST(commits.repo_name) AS repo
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
    repo: {
      sql: `repo`,
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
        dimensionReferences: [ repo ],
        timeDimensionReference: time,
        granularity: 'day',
        partitionGranularity: 'month',
        unionWithSourceData: !!env.CUBEJS_TEST_USE_LAMBDA,
      },
    },
});