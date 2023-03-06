import ReactDOM from 'react-dom/client'
import { useState, useEffect } from 'react'
import cubejs from '@cubejs-client/core'
import { CubeProvider, useCubeQuery } from '@cubejs-client/react'
import RegionSelector from './components/RegionSelector'
import CalendarChart from './components/CalendarChart'
import DayRegionIndicator from './components/DayRegionIndicator'
import ArticleList from './components/ArticleList'
import LoadingIndicator from './components/LoadingIndicator'

const cubejsApi = cubejs(process.env.CUBE_TOKEN, { apiUrl: process.env.CUBE_API_URL })

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(<CubeProvider cubejsApi={cubejsApi}><App /></CubeProvider>)

const defaultRegion = 'EN'
const regionCount = 20
const articleCount = 10

function App() {
  // Regions

  const [ regions, setRegions ] = useState([ defaultRegion ])
  const [ selectedRegion, setSelectedRegion ] = useState(undefined)

  function toggleSelectedRegion(region) {
    // Reset selected day
    setSelectedDay(undefined)

    // Unselect if selected, select otherwise
    setSelectedRegion(region === selectedRegion ? undefined : region)
  }

  let regionsQuery = {
    dimensions: [ 'Outliers.region' ],
    measures: [ 'Outliers.count' ],
    order: { 'Outliers.count': 'desc' },
    segments: [ 'Outliers.wikipedia' ],
    limit: regionCount
  }
  const { resultSet: regionsResultSet } = useCubeQuery(regionsQuery)

  useEffect(() => {
    if (regionsResultSet) {
      setRegions(regionsResultSet.tablePivot().map(x => x['Outliers.region']))
    }
  }, [ regionsResultSet ])


  // Outliers

  const [ outliers, setOutliers ] = useState([])

  let outliersQuery = {
    timeDimensions: [{
      dimension: 'Outliers.logDate',
      granularity: 'day'
    }],
    measures: [ 'Outliers.dailyTotal' ],
    segments: [ 'Outliers.wikipedia' ],
  }
  if (selectedRegion) {
    outliersQuery.filters = [{
      member: 'Outliers.region',
      operator: 'equals',
      values: [ selectedRegion ]
    }]
  }
  const { resultSet: outliersResultSet } = useCubeQuery(outliersQuery)

  useEffect(() => {
    if (outliersResultSet) {
      setOutliers(outliersResultSet.tablePivot()
        .map(x => ({
          day: x['Outliers.logDate.day'].split('T')[0],
          value: parseInt(x['Outliers.dailyTotal'])
        }))
        .filter(x => x.value !== 0)
      )
    }
  }, [ outliersResultSet ])
  

  // Articles

  const [ articles, setArticles ] = useState([])
  const [ selectedDay, setSelectedDay ] = useState(undefined)

  function toggleSelectedDay(day) {
    setSelectedDay(day)
  }

  let articlesQuery = {
    ...outliersQuery,
    timeDimensions: [{
      dimension: 'Outliers.logDate'
    }],
    dimensions: [ 'Outliers.url', 'Outliers.title', 'Outliers.region' ],
    order: { 'Outliers.dailyTotal': 'desc' },
    limit: articleCount
  }
  if (selectedDay) {
    articlesQuery.timeDimensions[0].dateRange = selectedDay
  }
  const { resultSet: articlesResultSet, isLoading } = useCubeQuery(articlesQuery)

  useEffect(() => {
    if (isLoading) {
      setArticles([])
    }

    if (articlesResultSet) {
      setArticles(articlesResultSet.tablePivot().map(x => ({
        url: x['Outliers.url'],
        title: x['Outliers.title'],
        region: x['Outliers.region'],
        value: parseInt(x['Outliers.dailyTotal'])
      })))
    }
  }, [ articlesResultSet, isLoading ])

  return <>
    <RegionSelector
      regions={regions}
      selectedRegion={selectedRegion}
      toggleRegion={toggleSelectedRegion}
    />
    <CalendarChart
      data={outliers}
      toggleDay={toggleSelectedDay}
    />
    <DayRegionIndicator
      day={selectedDay}
      region={selectedRegion}
      count={articleCount}
    />
    {isLoading && (
      <LoadingIndicator />
    )}
    <ArticleList
      articles={articles}
    />
  </>
}