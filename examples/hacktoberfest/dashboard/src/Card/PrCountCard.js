import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

const prevYearCount = 483127

export default function PrCountCard() {
  const { resultSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
  })

  const [ count, setCount ] = useState(0)

  useEffect(() => {
    if (resultSet) {
      setCount(resultSet.tablePivot()[0]['PullRequests.count'])
    }
  }, [ resultSet ])

  const prevYearRatio = count > 0
    ? (prevYearCount / count).toFixed(1)
    : 0

  return (
    <Card
      metric={count}
      thousandPrecision={1}
      fact='pull requests'
      description='were submitted and accepted during Hacktoberfest 2020'
      auxiliary={`${prevYearRatio}x less than in 2019`}
    />
  )
}