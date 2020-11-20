import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

const ghUserCount = 40000000

export default function UserCountCard() {
  const { resultSet } = useCubeQuery({
    measures: [ 'Users.count' ],
  })

  const [ count, setCount ] = useState(0)

  useEffect(() => {
    if (resultSet) {
      setCount(resultSet.tablePivot()[0]['Users.count'])
    }
  }, [ resultSet ])

  const ghUserRatio = (100 * count / ghUserCount).toFixed(2)

  return (
    <Card
      metric={count}
      thousandPrecision={1}
      fact='participants'
      description='took part in Hacktoberfest 2020'
      auxiliary={`A whole ${ghUserRatio} % of the GitHub developer community participated`}
    />
  )
}