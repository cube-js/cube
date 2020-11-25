import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import moment from 'moment'
import Chart from './index'
import { formatNumber, getMedian, withFilters } from '../util'

export default function PrsChart({ filters }) {
  const queries = withFilters(filters, {
    measures: [ 'PullRequests.count' ],
    timeDimensions: [ {
      dimension: 'PullRequests.createdAt',
      granularity: 'day',
      dateRange: [ '2020-09-28', '2020-11-03' ],
    } ],
  })

  const { resultSet } = useCubeQuery(queries)

  const [ keys, setKeys ] = useState([])
  const [ data, setData ] = useState([])
  const [ stats, setStats ] = useState({
    mostCreatedDay: 'October ?',
    mostCreatedCount: 0,
    medianCount: 0,
    allCount: 0,
  })

  useEffect(() => {
    if (resultSet) {
      const sets = resultSet.decompose()
      const keys = sets
        .map(set => set.loadResponse.pivotQuery.filters.length > 0 ? set.loadResponse.pivotQuery.filters[0].values[0] : '')

      setKeys(keys)

      const pivots = sets.map(set => set.chartPivot())
      const data = pivots[0].map(row => ({ x: row['x'] }))
      data.forEach((row, i) => {
        pivots.forEach((pivot, j) => {
          data[i][keys[j]] = pivot[i]['PullRequests.count']
        })

        return data
      })

      setData(data)

      const mostCreated = data.reduce((selected, day) => day[keys[0]] > selected[keys[0]] ? day : selected)
      const medianCount = getMedian(data.map(day => day[keys[0]]))
      const allCount = getMedian(data.map(day => day[keys[0]]))
      setStats({
        mostCreatedDay: moment(mostCreated['x']).format('MMMM D'),
        mostCreatedCount: mostCreated[keys[0]],
        medianCount,
        allCount,
      })
    }
  }, [ filters, resultSet ])

  const ratio = stats.medianCount > 0
    ? (stats.mostCreatedCount / stats.medianCount).toFixed(1)
    : 0

  return (
    <Chart
      data={data}
      metricKeys={keys}
      metricTickCount={4}
      fact={stats.mostCreatedDay}
      description={`was the most active day with ${formatNumber(stats.mostCreatedCount)} pull requests created`}
      auxiliary={`${ratio}x compared to a usual day`}
    />
  )
}