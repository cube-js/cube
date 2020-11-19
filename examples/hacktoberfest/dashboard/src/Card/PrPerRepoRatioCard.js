import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

export default function PrPerRepoRatioCard() {
  const { resultSet: prCountSet } = useCubeQuery({
    measures: [ 'PullRequests.count' ],
  })

  const { resultSet: repoCountSet } = useCubeQuery({
    measures: [ 'Repos.count' ],
  })

  const [ ratio, setRatio ] = useState(0)

  useEffect(() => {
    if (prCountSet && repoCountSet) {
      const prCount = prCountSet.tablePivot()[0]['PullRequests.count']
      const repoCount = repoCountSet.tablePivot()[0]['Repos.count']
      setRatio(prCount / repoCount)
    }
  }, [ prCountSet, repoCountSet ])

  return (
    <Card
      metric={ratio}
      precision={1}
      fact='pull requests'
      description='were accepted by each repository, on average'
    />
  )
}