import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import Card from './index'
import { formatNumber, roundNumberWithThousandPrecision } from '../util'

export default function RepoStarCountCard() {
  // Average

  const { resultSet: avgCountSet } = useCubeQuery({
    measures: [ 'Repos.avgStarCount' ],
  })

  const [ avgCount, setAvgCount ] = useState(0)

  useEffect(() => {
    if (avgCountSet) {
      setAvgCount(avgCountSet.tablePivot()[0]['Repos.avgStarCount'])
    }
  }, [ avgCountSet ])

  // Median

  const { resultSet: medianCountSet } = useCubeQuery({
    measures: [ 'Repos.medianStarCount' ],
  })

  const [ medianCount, setMedianCount ] = useState(0)

  useEffect(() => {
    if (medianCountSet) {
      setMedianCount(medianCountSet.tablePivot()[0]['Repos.medianStarCount'])
    }
  }, [ medianCountSet ])

  // Max

  const { resultSet: maxCountSet } = useCubeQuery({
    measures: [ 'Repos.maxStarCount' ],
    dimensions: [
      'Repos.fullName',
      'Repos.url',
    ],
    order: {
      'Repos.maxStarCount': 'desc',
    },
    limit: 1,
  })

  const [ maxCount, setMaxCount ] = useState(0)
  const [ maxRepoName, setMaxRepoName ] = useState('?')
  const [ maxRepoUrl, setMaxRepoUrl ] = useState('#')

  useEffect(() => {
    if (maxCountSet) {
      setMaxCount(maxCountSet.tablePivot()[0]['Repos.maxStarCount'])
      setMaxRepoName(maxCountSet.tablePivot()[0]['Repos.fullName'])
      setMaxRepoUrl(maxCountSet.tablePivot()[0]['Repos.url'])
    }
  }, [ maxCountSet ])

  const auxiliary = (
    <>
      <a href={maxRepoUrl} target='_blank' rel='noreferrer'>{maxRepoName}</a>{' '}
      stands out with {formatNumber(roundNumberWithThousandPrecision(maxCount))} stars
      but the median is {medianCount}, obviously
    </>
  )

  return (
    <Card
      metric={avgCount}
      fact='stars'
      description='a repository has, on average'
      auxiliary={auxiliary}
    />
  )
}