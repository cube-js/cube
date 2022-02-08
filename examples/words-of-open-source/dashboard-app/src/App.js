import { useState, useEffect } from 'react'
import { AreaChart, Area, XAxis, ResponsiveContainer } from 'recharts';
import { fetchData } from './api'
import * as classes from './App.module.css'

export function App() {
  const [ word, setWord ] = useState("FOSDEM")
  const [ data, setData ] = useState([])

  useEffect(() => {
    fetchData(word).then(data => setData(data))
  }, [ word ])

  return (
    <>
      <div className={classes.root}>
        <h1>Words of Open Source</h1>
        
        <p>Ratio of the number of commits containing a certain word to the total number of commits.</p>
        <p>Based on the public dataset of <a href="https://console.cloud.google.com/marketplace/product/github/github-repos">GitHub Activity Data</a> and powered by <a href="https://cube.dev">Cube</a></p>

        <div className={classes.inputWrapper}>
          <input
            defaultValue={word}
            onKeyDown={e => e.key === 'Enter' && e.target.value && setWord(e.target.value)}
            className={classes.input}
          />
          <div className={classes.enterSymbol}>↵</div>
        </div>

        <div className={classes.chartWrapper}>
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart
              data={data}
              margin={{ top: 3, right: 20, left: 20 }}
            >
              <XAxis
                dataKey="name"
                axisLine={false}
                interval={0}
                tickSize={0}
                tickMargin={15}
              />
              <Area
                type="basis"
                dataKey="data"
                stroke="#c93324"
                fill="#eddedd"
                strokeWidth={3}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>
    </>
  )
}