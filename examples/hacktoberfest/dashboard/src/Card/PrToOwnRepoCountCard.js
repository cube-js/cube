import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function PrToOwnRepoCountCard() {
  // All

  const { resultSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
  })

  const [ count, setCount ] = useState(0)

  useEffect(() => {
    if (resultSet) {
      setCount(resultSet.tablePivot()[0]['PullRequests.count'])
    }
  }, [ resultSet ])

  // To own repositories

  const { resultSet: prsToOwnRepoCountSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
    filters: [ {
      dimension: 'PullRequests.isToOwnRepo',
      operator: 'equals',
      values: [ '1' ],
    } ],
  })

  const [ prsToOwnRepoCount, setPrsToOwnRepoCount ] = useState(0)

  useEffect(() => {
    if (prsToOwnRepoCountSet) {
      setPrsToOwnRepoCount(prsToOwnRepoCountSet.tablePivot()[0]['PullRequests.count'])
    }
  }, [ prsToOwnRepoCountSet ])

  // Ratio

  const ownRatio = count > 0 ?
    (100 * prsToOwnRepoCount / count).toFixed(0)
    : 0

  return (
    <Card
      metric={prsToOwnRepoCount}
      thousandPrecision={1}
      fact='pull requests'
      description='were submitted and accepted by target repository owners 🤷‍♀️'
      auxiliary={`${ownRatio} % of all pull requests`}
    />
  )
}