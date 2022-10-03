import React from 'react'
import { useState, useEffect } from 'react'
import * as classes from './index.module.css'
import * as ReactDOM from 'react-dom/client'
import cubejs from '@cubejs-client/core'

import {
  apiUrl,
  defaultDataSourceId,
  dataSources,
  DisplayBarChart,
  jsonQuery,
} from './utils/utils'

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(
    <App />
  )

function App() {
  const [ timer, setTimer ] = useState({})

  const [ dataSourceId, setDataSourceId ] = useState(defaultDataSourceId)
  const dataSource = dataSources.find(x => x.id === dataSourceId)

  const cubejsApi = cubejs(
    dataSource.token,
    { apiUrl },
  )

  const [ ontimeBarData, setOntimeBarData ] = useState({})
  useEffect(() => {
    setTimer({})
    const start = Date.now()

    setOntimeBarData({})
    cubejsApi
      .load(jsonQuery())
      .then(setOntimeBarData)
      .then(() => {
        const end = Date.now()
        const responseTime = end - start
        setTimer({ responseTime })
      })
  }, [
    dataSource.token,
    apiUrl,
  ])

  return <>
    <div style={{ display: 'flex', justifyContent: 'center' }}>
      <div style={{ display: 'flex', justifyContent: 'center' }}>
        <label style={{ display: 'flex', justifyContent: 'center', alignItems: 'center' }}>Authorization token</label>
        <textarea 
          readOnly
          value={dataSource.token}
          className={classes.select}
        ></textarea>
      </div>
      <select
        className={classes.select}
        value={dataSourceId}
        onChange={e => setDataSourceId(parseInt(e.target.value))}
      >
        <option value="" disabled>Select data source...</option>
        <option key={1} value={1}>
          ClickHouse
        </option>
        <option key={2} value={2}>
          MySQL
        </option>
      </select>
    </div>

    <div style={{ display: 'flex', justifyContent: 'center' }}>
      <table style={{ width: '80%' }}>
        <tbody>
          <tr>
            <td style={{ width: '100%' }}>
              <div style={{ height: '375px', margin: '20px 0' }}>
                <h3 style={{display: 'flex', justifyContent: 'center'}}>{ (timer.responseTime / 1000) || '...' } seconds</h3>
                <DisplayBarChart
                  chartData={ontimeBarData}
                />
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </>
}
