import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function UnmergedPrCountCard() {
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

  // Unmerged

  const { resultSet: unmergedPrsCountSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
    filters: [ {
      dimension: 'PullRequests.isMerged',
      operator: 'equals',
      values: [ '0' ],
    } ],
  })

  const [ unmergedPrsCount, setUnmergedPrsCount ] = useState(0)

  useEffect(() => {
    if (unmergedPrsCountSet) {
      setUnmergedPrsCount(unmergedPrsCountSet.tablePivot()[0]['PullRequests.count'])
    }
  }, [ unmergedPrsCountSet ])

  // Ratio

  const ratio = count > 0 ?
    (100 * unmergedPrsCount / count).toFixed(0)
    : 0

  return (
    <Card
      metric={unmergedPrsCount}
      thousandPrecision={1}
      fact='pull requests'
      description='were accepted but remain unmerged'
      auxiliary={`${ratio} % of all pull requests`}
    />
  )
}