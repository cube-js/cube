cube(`Words`, {
    sql: `
      SELECT
        commit,
        word,
        TIMESTAMP_SECONDS(committer.time_sec) AS timestamp
      FROM
        \`bigquery-public-data.github_repos.commits\`,
        UNNEST(SPLIT(LOWER(REGEXP_REPLACE(subject, r'\\W', ' ')), ' ')) AS word
      WHERE
        word != '' AND
        TIMESTAMP_SECONDS(committer.time_sec) > '2010-01-01T00:00:00Z' AND
        TIMESTAMP_SECONDS(committer.time_sec) < CURRENT_TIMESTAMP()
    `,
  
    measures: {
      count: {
        type: `countDistinctApprox`,
        sql: `commit`
      },
    },
  
    dimensions: {
      word: {
        sql: `word`,
        type: `string`
      },
  
      timestamp: {
        sql: `timestamp`,
        type: `time`
      }
    },
  
    preAggregations: {
      main: {
        measures: [ count ],
        dimensions: [ word ],
        timeDimension: timestamp,
        granularity: `month`,
        partitionGranularity: `month`,
        refreshKey: {
          every: `1 day`,
          incremental: true
        }
      }
    }
  });
  