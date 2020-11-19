import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function PrPerUserRatioCard() {
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

  return (
    <Card
      metric={ratio}
      precision={1}
      fact='pull requests'
      description='were accepted from every participant'
      auxiliary='Minimum of 4 accepted pull requests was required to get awards'
    />
  )
}