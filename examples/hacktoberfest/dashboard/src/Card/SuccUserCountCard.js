import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function SuccUserCountCard() {
  // All

  const { resultSet } = useCubeQuery({
    measures: [ 'Users.count' ],
  })

  const [ count, setCount ] = useState(0)

  useEffect(() => {
    if (resultSet) {
      setCount(resultSet.tablePivot()[0]['Users.count'])
    }
  }, [ resultSet ])

  // Successful

  const { resultSet: succCountSet } = useCubeQuery({
    measures: [ 'Users.count' ],
    filters: [ {
      dimension: 'Users.pullRequestCount',
      operator: 'gte',
      values: [ '4' ],
    } ],
  })

  const [ succCount, setSuccCount ] = useState(0)

  useEffect(() => {
    if (succCountSet) {
      setSuccCount(succCountSet.tablePivot()[0]['Users.count'])
    }
  }, [ succCountSet ])

  // Ratio

  const succRatio = count > 0 ?
    (100 * succCount / count).toFixed(0)
    : 0

  return (
    <Card
      metric={succCount}
      thousandPrecision={1}
      fact='participants'
      description='have submitted 4+ pull requests accepted by maintainers'
      auxiliary={`${succRatio} % of all participants`}
    />
  )
}