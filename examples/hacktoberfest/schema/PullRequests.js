cube(`PullRequests`, {
  // Beware! Not unique! (?)
  sql: `
  SELECT
    id,
    base.repo.full_name AS repo_name,
    html_url AS url,
    CAST(FROM_ISO8601_TIMESTAMP(created_at) AS TIMESTAMP) AS created_at,
    CAST(FROM_ISO8601_TIMESTAMP(updated_at) AS TIMESTAMP) AS updated_at,
    CAST(FROM_ISO8601_TIMESTAMP(closed_at) AS TIMESTAMP) AS closed_at,
    CAST(FROM_ISO8601_TIMESTAMP(merged_at) AS TIMESTAMP) AS merged_at,
    user.login AS user_login,
    COALESCE(state, 'Unknown') AS state,
    IF(merged, '1', '0') AS is_merged,
    CARDINALITY(labels) AS label_count,
    COALESCE(CAST(commits AS INTEGER), 0) AS commit_count,
    COALESCE(CAST(additions AS INTEGER), 0) AS addition_count,
    COALESCE(CAST(deletions AS INTEGER), 0) AS deletion_count,
    COALESCE(CAST(changed_files AS INTEGER), 0) AS changed_file_count,
    CASE WHEN user.login = base.repo.owner.login THEN '1' ELSE '0' END AS is_to_own_repo
  FROM cubedev_examples_hacktoberfest.api_accepted`,

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
        isMerged,
        isToOwnRepo,
      ],
      timeDimensionReference: createdAt,
      granularity: `day`,
    },
  },

  joins: {

  },

  measures: {
    count: {
      type: `count`,
    },

    avgCommitCount: {
      sql: `${commitCount}`,
      type: `avg`,
    },

    maxCommitCount: {
      sql: `${commitCount}`,
      type: `max`,
    },

    medianCommitCount: {
      sql: `APPROX_PERCENTILE(${commitCount}, 0.5)`,
      type: `number`,
    },

    avgAdditionCount: {
      sql: `${additionCount}`,
      type: `avg`,
    },

    maxAdditionCount: {
      sql: `${additionCount}`,
      type: `max`,
    },

    medianAdditionCount: {
      sql: `APPROX_PERCENTILE(${additionCount}, 0.5)`,
      type: `number`,
    },

    avgDeletionCount: {
      sql: `${deletionCount}`,
      type: `avg`,
    },

    maxDeletionCount: {
      sql: `${deletionCount}`,
      type: `max`,
    },

    medianDeletionCount: {
      sql: `APPROX_PERCENTILE(${deletionCount}, 0.5)`,
      type: `number`,
    },

    avgChangedFileCount: {
      sql: `${changedFileCount}`,
      type: `avg`,
    },

    maxChangedFileCount: {
      sql: `${changedFileCount}`,
      type: `max`,
    },

    medianChangedFileCount: {
      sql: `APPROX_PERCENTILE(${changedFileCount}, 0.5)`,
      type: `number`,
    },
  },

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    url: {
      sql: `url`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },

    updatedAt: {
      sql: `updated_at`,
      type: `time`,
    },

    closedAt: {
      sql: `closed_at`,
      type: `time`,
    },

    mergedAt: {
      sql: `merged_at`,
      type: `time`,
    },

    state: {
      sql: `state`,
      type: `string`,
    },

    isMerged: {
      sql: `is_merged`,
      type: `string`,
    },

    labelCount: {
      sql: `label_count`,
      type: `number`,
    },

    commitCount: {
      sql: `commit_count`,
      type: `number`,
    },

    additionCount: {
      sql: `addition_count`,
      type: `number`,
    },

    deletionCount: {
      sql: `deletion_count`,
      type: `number`,
    },

    changedFileCount: {
      sql: `changed_file_count`,
      type: `number`,
    },

    isToOwnRepo: {
      sql: `is_to_own_repo`,
      type: `string`,
    },

    isHacktoberfestAccepted: {
      // sql: `is_hacktoberfest_accepted`,
      sql: `FALSE`, // TODO: Fix
      type: `boolean`,
    },
  },
});
