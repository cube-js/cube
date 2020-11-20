import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function ForkedRepoCountCard() {
  // All

  const { resultSet } = useCubeQuery({
    measures: [ 'Repos.count' ],
  })

  const [ count, setCount ] = useState(0)

  useEffect(() => {
    if (resultSet) {
      setCount(resultSet.tablePivot()[0]['Repos.count'])
    }
  }, [ resultSet ])

  // Forked

  const { resultSet: forkReposCountSet } = useCubeQuery({
    measures: [ 'Repos.count' ],
    filters: [ {
      dimension: 'Repos.isFork',
      operator: 'equals',
      values: [ '1' ],
    } ],
  })

  const [ forkReposCount, setForkReposCount ] = useState(0)

  useEffect(() => {
    if (forkReposCountSet) {
      setForkReposCount(forkReposCountSet.tablePivot()[0]['Repos.count'])
    }
  }, [ forkReposCountSet ])

  const ratio = count > 0
    ? (100 * forkReposCount / count).toFixed(0)
    : 0

  return (
    <Card
      metric={forkReposCount}
      thousandPrecision={1}
      fact='repositories'
      description='that accepted pull requests were forks rather than original'
      auxiliary={`${ratio} % of all repositories`}
    />
  )
}