import dbt from '@cubejs-backend/dbt-schema-extension'
import { dbtJobId, dbtApiKey } from '../config'

asyncModule(async () => {
  await dbt.loadMetricCubesFromDbtCloud(dbtJobId, dbtApiKey)
})

cube('GithubCommitStatsCommitsCached', {
  extends: GithubCommitStatsCommits,
  
  preAggregations: {
    main: {
      measures: [ commitsCount ],
      dimensions: [ authorDomain, authorName ],
      timeDimension: timestamp,
      granularity: 'day',
      partitionGranularity: 'year'
    }
  },
})