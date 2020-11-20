import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

const prevYearCount = 154767

export default function RepoCountCard() {
  const { resultSet } = useCubeQuery({
    measures: [ 'Repos.count' ],
  })

  const [ count, setCount ] = useState(0)

  useEffect(() => {
    if (resultSet) {
      setCount(resultSet.tablePivot()[0]['Repos.count'])
    }
  }, [ resultSet ])

  const prevYearRatio = count > 0
    ? (prevYearCount / count).toFixed(1)
    : 0

  return (
    <Card
      metric={count}
      thousandPrecision={1}
      fact='repositories'
      description='accepted pull requests from participants of Hacktoberfest 2020'
      auxiliary={`${prevYearRatio}x less than in 2019`}
    />
  )
}