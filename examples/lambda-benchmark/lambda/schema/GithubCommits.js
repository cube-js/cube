cube(`GithubCommits`, {
  sql: `
      SELECT *
      FROM \`bigquery-public-data.github_repos.commits\` AS commits
      CROSS JOIN UNNEST(commits.repo_name) AS repo
      WHERE TIMESTAMP_SECONDS(author.time_sec) < CURRENT_TIMESTAMP()
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
      sql: `TIMESTAMP_SECONDS(author.time_sec)`,
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
      partitionGranularity: 'month',
      unionWithSourceData: true,
      buildRangeEnd: {
        sql: `SELECT DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)`,
      },
    },
  },
});