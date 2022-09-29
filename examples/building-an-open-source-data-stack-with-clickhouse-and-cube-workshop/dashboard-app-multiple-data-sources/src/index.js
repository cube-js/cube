import React from 'react';
import { useState, useEffect } from 'react'
import * as classes from './index.module.css'
import * as ReactDOM from 'react-dom/client';
import cubejs from '@cubejs-client/core';

import {
  DisplayBarChart,
  years,
  defaultApiUrl,
  defaultYearId,
  token,
  jsonQuery,
} from './utils/utils';

ReactDOM
  .createRoot(document.getElementById('app'))
  .render(
    <App />
  )

function App() {
  const [ timer, setTimer ] = useState({});
  const apiUrl = defaultApiUrl;

  const [ yearId, setYearId ] = useState(defaultYearId);
  const year = years.find(x => x.id === yearId);

  const cubejsApi = cubejs(
    token,
    { apiUrl },
  );

  useEffect(() => {
    setTimer({});
    const start = Date.now();
    cubejsApi
      .meta()
      .then(() => {
        const end = Date.now();
        const responseTime = end - start;
        setTimer({ responseTime });
      })
  }, [
    year.year,
  ]);

  const [ clickhouseOntimeBarData, setClickhouseOntimeBarData ] = useState({});
  useEffect(() => {
    cubejsApi
      .load(jsonQuery({ year: year.year, dataSource: 'clickhouse' }))
      .then(setClickhouseOntimeBarData)
  }, [
    year.year,
  ]);

  const [ mysqlOntimeBarData, setMysqlOntimeBarData ] = useState({});
  useEffect(() => {
    cubejsApi
      .load(jsonQuery({ year: year.year, dataSource: 'clickhouse' })) // edit to use mysql once we get endpoint
      .then(setMysqlOntimeBarData)
  }, [
    year.year,
  ]);

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
    </div>

    <div style={{ display: 'flex', justifyContent: 'center' }}>
      <table style={{ width: '80%' }}>
        <tbody>
          <tr>
            <td style={{ width: '50%' }}>
              <div style={{ height: '375px', margin: '20px 0' }}>
                <h3 style={{display: 'flex', justifyContent: 'center'}}>ClickHouse { (timer.responseTime / 1000) || '...' } seconds</h3>
                <DisplayBarChart
                  chartData={clickhouseOntimeBarData}
                />
              </div>
            </td>
            <td style={{ width: '50%' }}>
              <div style={{ height: '375px', margin: '20px 0' }}>
                <h3 style={{display: 'flex', justifyContent: 'center'}}>MySQL { (timer.responseTime / 1000) || '...' } seconds</h3>
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
