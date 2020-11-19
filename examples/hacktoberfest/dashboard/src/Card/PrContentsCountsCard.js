import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function PrContentsCountsCard() {
  const { resultSet } = useCubeQuery({
    measures: [
      'PullRequests.avgCommitCount',
      'PullRequests.medianCommitCount',
      'PullRequests.avgChangedFileCount',
      'PullRequests.medianChangedFileCount',
    ],
  })

  const [ counts, setCounts ] = useState({
    avgCommitCount: 0,
    medianCommitCount: 0,
    avgChangedFileCount: 0,
    medianChangedFileCount: 0,
  })

  useEffect(() => {
    if (resultSet) {
      setCounts({
        avgCommitCount: (1 * resultSet.tablePivot()[0]['PullRequests.avgCommitCount']).toFixed(0),
        medianCommitCount: (1 * resultSet.tablePivot()[0]['PullRequests.medianCommitCount']).toFixed(0),
        avgChangedFileCount: (1 * resultSet.tablePivot()[0]['PullRequests.avgChangedFileCount']).toFixed(0),
        medianChangedFileCount: (1 * resultSet.tablePivot()[0]['PullRequests.medianChangedFileCount']).toFixed(0),
      })
    }
  }, [ resultSet ])

  return (
    <Card
      metric={counts.avgCommitCount}
      fact={'commits'}
      description={`on average were included in a pull request but the median was ${counts.medianCommitCount}`}
      auxiliary={`${counts.avgChangedFileCount} files were changed on average but the median was also ${counts.medianChangedFileCount}`}
    />
  )
}