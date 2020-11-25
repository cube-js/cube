import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function PrPerSuccUserRatioCard() {
  // All

  const { resultSet: prCountSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
  })

  const { resultSet: userCountSet } = useCubeQuery({
    measures: [ 'Users.count' ],
  })

  const [ ratio, setRatio ] = useState(0)

  useEffect(() => {
    if (prCountSet && userCountSet) {
      const prCount = prCountSet.tablePivot()[0]['PullRequests.count']
      const userCount = userCountSet.tablePivot()[0]['Users.count']
      setRatio(prCount / userCount)
    }
  }, [ prCountSet, userCountSet ])

  // Successful

  const { resultSet: succPrCountSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
  })

  const { resultSet: succUserCountSet } = useCubeQuery({
    measures: [ 'Users.count' ],
    filters: [ {
      dimension: 'Users.pullRequestCount',
      operator: 'gte',
      values: [ '4' ],
    } ],
  })

  const [ succRatio, setSuccRatio ] = useState(0)

  useEffect(() => {
    if (succPrCountSet && succUserCountSet) {
      const prCount = succPrCountSet.tablePivot()[0]['PullRequests.count']
      const userCount = succUserCountSet.tablePivot()[0]['Users.count']
      setSuccRatio(prCount / userCount)
    }
  }, [ succPrCountSet, succUserCountSet ])

  // Ratio

  const prRatio = ratio > 0
    ? (succRatio / ratio).toFixed(1)
    : 0

  return (
    <Card
      metric={succRatio}
      precision={1}
      fact='pull requests'
      description='were accepted from every award-winning participant'
      auxiliary={`${prRatio}x more than average`}
    />
  )
}