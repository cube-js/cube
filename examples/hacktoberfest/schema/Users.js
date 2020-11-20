cube(`Users`, {
  sql: `
  SELECT
    user.login AS login,
    user.html_url AS url,
    user.avatar_url AS avatar_url,
    user.type AS type
  FROM cubedev_examples_hacktoberfest.api_accepted
  GROUP BY 1, 2, 3, 4`,

  refreshKey: {
    every: `1 hour`,
  },

  preAggregations: {
    main: {
      type: `originalSql`,
    },
    rollup: {
      type: `rollup`,
      useOriginalSqlPreAggregations: true,
      measureReferences: [
        count,
      ],
      dimensionReferences: [
        pullRequestCount,
      ],
    },
  },

  joins: {
    PullRequests: {
      relationship: `hasMany`,
      sql: `${Users}.login = ${PullRequests}.user_login`,
    },
  },

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    login: {
      sql: `login`,
      type: `string`,
      primaryKey: true,
      shown: true,
    },

    url: {
      sql: `url`,
      type: `string`,
    },

    avatarUrl: {
      sql: `avatar_url`,
      type: `string`,
    },

    type: {
      sql: `type`,
      type: `string`,
    },

    pullRequestCount: {
      sql: `${PullRequests.count}`,
      type: `number`,
      subQuery: true,
    },
  },
});
