import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'

const prevYearName = 'JavaScript'

export default function LanguageCard() {
  const { resultSet } = useCubeQuery({
    measures: [ 'Repos.count' ],
    dimensions: [ 'Repos.language' ],
    filters: [ {
      dimension: 'Repos.language',
      operator: 'notEquals',
      values: [ 'Unknown' ],
    } ],
    order: {
      'Repos.count': 'desc',
    },
    limit: 1,
  })

  const [ name, setName ] = useState('?')

  useEffect(() => {
    if (resultSet) {
      setName(resultSet.tablePivot()[0]['Repos.language'])
    }
  }, [ resultSet ])

  return (
    <Card
      fact={name}
      description='is the most popular language in repositories of Hacktoberfest 2020'
      auxiliary={`${prevYearName} in 2019`}
    />
  )
}