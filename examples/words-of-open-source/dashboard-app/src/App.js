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