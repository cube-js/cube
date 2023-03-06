import React from 'react'
import { useState, useEffect } from 'react'
import * as classes from './index.module.css'
import * as ReactDOM from 'react-dom/client'
import cubejs from '@cubejs-client/core'

import {
  DisplayBarChart,
  years,
  months,
  apiUrl,
  defaultYearId,
  defaultMonthId,
  token,
  jsonQuery,
  randomIntFromInterval,
} from './utils/utils'

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(
    <App />
  )

function App() {
  const [ timerClickhouse, setTimerClickhouse ] = useState({})
  const [ timerMysql, setTimerMysql ] = useState({})

  const [ yearId, setYearId ] = useState(defaultYearId)
  const year = years.find(x => x.id === yearId)

  const [ monthId, setMonthId ] = useState(defaultMonthId)
  const month = months.find(x => x.id === monthId)

  const shuffleAndRun = () => {
    setYearId(randomIntFromInterval(1, 34));
    setMonthId(randomIntFromInterval(1, 12));
  }

  const cubejsApi = cubejs(
    token,
    { apiUrl },
  )

  const [ clickhouseOntimeBarData, setClickhouseOntimeBarData ] = useState({})
  useEffect(() => {
    setTimerClickhouse({})
    const start = Date.now()

    setClickhouseOntimeBarData({})
    cubejsApi
      .load(jsonQuery({ year, month, dataSource: 'clickhouse' }))
      .then(setClickhouseOntimeBarData)
      .then(() => {
        const end = Date.now()
        const responseTime = end - start
        setTimerClickhouse({ responseTime })
      })
  }, [
    year.year,
    month.id,
  ])

  const [ mysqlOntimeBarData, setMysqlOntimeBarData ] = useState({})
  useEffect(() => {
    setTimerMysql({})
    const start = Date.now()

    setMysqlOntimeBarData({})
    cubejsApi
      .load(jsonQuery({ year, month, dataSource: 'mysql' }))
      .then(setMysqlOntimeBarData)
      .then(() => {
        const end = Date.now()
        const responseTime = end - start
        setTimerMysql({ responseTime })
      })
  }, [
    year.year,
    month.id,
  ])

  return <>
    <div style={{ display: 'flex', justifyContent: 'center' }}>
      <select
        className={classes.select}
        value={yearId}
        onChange={e => setYearId(parseInt(e.target.value))}
      >
        <option value="" disabled>Select year...</option>
        {years.map(year => (
          <option key={year.id} value={year.id}>
            {year.year}
          </option>
        ))}
      </select>
      <select
        className={classes.select}
        value={monthId}
        onChange={e => setMonthId(parseInt(e.target.value))}
      >
        <option value="" disabled>Select month...</option>
        {months.map(month => (
          <option key={month.id} value={month.id}>
            {month.month}
          </option>
        ))}
      </select>
      <div className={`${classes.buttonwrp}`}>
        <button className={`Button Button--size-s Button--pink`} onClick={shuffleAndRun}>
          Shuffle and Run!
        </button>
      </div>
    </div>

    <div style={{ display: 'flex', justifyContent: 'center' }}>
      <table style={{ width: '80%' }}>
        <tbody>
          <tr>
            <td style={{ width: '50%' }}>
              <div style={{ height: '375px', margin: '20px 0' }}>
                <h3 style={{display: 'flex', justifyContent: 'center'}}>ClickHouse { (timerClickhouse.responseTime) ? `${timerClickhouse.responseTime / 1000} seconds` : '...' }</h3>
                <DisplayBarChart
                  chartData={clickhouseOntimeBarData}
                />
              </div>
            </td>
            <td style={{ width: '50%' }}>
              <div style={{ height: '375px', margin: '20px 0' }}>
                <h3 style={{display: 'flex', justifyContent: 'center'}}>MySQL { (timerMysql.responseTime) ? `${timerMysql.responseTime / 1000} seconds` : '...' }</h3>
                <DisplayBarChart
                  chartData={mysqlOntimeBarData}
                />
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </>
}
