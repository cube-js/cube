cube(`Repos`, {
  sql: `
  SELECT
    base.repo.full_name AS full_name,
    base.repo.html_url AS url,
    CAST(FROM_ISO8601_TIMESTAMP(base.repo.created_at) AS TIMESTAMP) AS created_at,
    COALESCE(base.repo.language, 'Unknown') AS language,
    COALESCE(base.repo.license.spdx_id, 'Unknown') AS license,
    COALESCE(base.repo.stargazers_count, 0) AS star_count,
    COALESCE(base.repo.watchers_count, 0) AS watcher_count,
    COALESCE(base.repo.forks, 0) AS fork_count,
    COALESCE(base.repo.size, 0) AS size,
    IF(base.repo.fork, '1', '0') AS is_fork,
    base.repo.default_branch AS default_branch
  FROM cubedev_examples_hacktoberfest.api_accepted
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11`,

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
        maxStarCount,
      ],
      dimensionReferences: [
        fullName,
        url,
        isFork,
        language,
      ],
    },
  },

  joins: {
    PullRequests: {
      relationship: `hasMany`,
      sql: `${Repos}.full_name = ${PullRequests}.repo_name`,
    },
  },

  measures: {
    count: {
      type: `count`,
    },

    avgStarCount: {
      sql: `${starCount}`,
      type: `avg`,
    },

    maxStarCount: {
      sql: `${starCount}`,
      type: `max`,
    },

    medianStarCount: {
      sql: `APPROX_PERCENTILE(${starCount}, 0.5)`,
      type: `number`,
    },

    avgWatcherCount: {
      sql: `${watcherCount}`,
      type: `avg`,
    },

    maxWatcherCount: {
      sql: `${watcherCount}`,
      type: `max`,
    },

    medianWatcherCount: {
      sql: `APPROX_PERCENTILE(${watcherCount}, 0.5)`,
      type: `number`,
    },

    avgForkCount: {
      sql: `${forkCount}`,
      type: `avg`,
    },

    maxForkCount: {
      sql: `${forkCount}`,
      type: `max`,
    },

    medianForkCount: {
      sql: `APPROX_PERCENTILE(${forkCount}, 0.5)`,
      type: `number`,
    },
  },

  dimensions: {
    fullName: {
      sql: `full_name`,
      type: `string`,
      primaryKey: true,
      shown: true,
    },

    url: {
      sql: `url`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    language: {
      sql: `language`,
      type: `string`,
    },

    license: {
      sql: `license`,
      type: `string`,
    },

    starCount: {
      sql: `star_count`,
      type: `number`,
    },

    watcherCount: {
      sql: `watcher_count`,
      type: `number`,
    },

    forkCount: {
      sql: `fork_count`,
      type: `number`,
    },

    size: {
      sql: `size`,
      type: `number`,
    },

    defaultBranch: {
      sql: `default_branch`,
      type: `string`,
    },

    isFork: {
      sql: `is_fork`,
      type: `string`,
    },

    pullRequestCount: {
      sql: `${PullRequests.count}`,
      type: `number`,
      subQuery: true,
    },
  },
});
