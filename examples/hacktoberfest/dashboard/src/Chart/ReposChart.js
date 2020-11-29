import { useCubeQuery } from '@cubejs-client/react'
import React, { useState, useEffect } from 'react'
import moment from 'moment'
import Chart from './index'
import { getMedian, withFilters } from '../util'

export default function PrsChart({ filters }) {
  const queries = withFilters(filters, {
    measures: [ 'Repos.count' ],
    timeDimensions: [ {
      dimension: 'Repos.createdAt',
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
          data[i][keys[j]] = pivot[i]['Repos.count']
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

  return (
    <Chart
      data={data}
      metricKeys={keys}
      metricTickCount={4}
      fact={stats.mostCreatedDay}
      description={`was the day when many participating repositories were created`}
      auxiliary={`The reasons for that are shrouded in mystery`}
    />
  )
}