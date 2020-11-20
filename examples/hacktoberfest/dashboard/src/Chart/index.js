import React from 'react'
import { LineChart, Line, ResponsiveContainer, XAxis, YAxis } from 'recharts'
import styles from '../Card/styles.module.css'
import { colors } from '../styles'
import moment from 'moment'
import { formatNumber, roundNumberWithThousandPrecision } from '../util'

export default function Chart(props) {
  const {
    data,
    metricKey,
    metricKeys = [ metricKey ],
    metricFormatter = formatNumber,
    metricTickCount = 1,
    timeKey = 'x',
    timeFormat = 'MMM D',
    metric,
    precision = 0,
    thousandPrecision,
    fact,
    description,
    auxiliary,
  } = props

  let number = parseFloat(metric).toFixed(precision)

  if (thousandPrecision) {
    number = roundNumberWithThousandPrecision(number, thousandPrecision)
  }

  return (
    <div className={styles.root}>
      <div className={styles.fact}>
        {metric !== undefined && (
          <span className={styles.metric}>{formatNumber(number)}{' '}</span>
        )}
        {fact}
      </div>
      <div className={styles.description}>{description}</div>
      <ResponsiveContainer width='100%' height={200}>
        <LineChart
          data={data}
          margin={{ top: 30, right: 5, left: 0, bottom: 0 }}
        >
          <XAxis
            dataKey={timeKey}
            style={{ fontSize: 14 }}
            tick={{ dy: 5 }}
            tickFormatter={time => moment(time).format(timeFormat)}
          />
          <YAxis
            style={{ fontSize: 14 }}
            tick={{ dx: -5 }}
            tickCount={metricTickCount}
            tickFormatter={metricFormatter}
          />
          {metricKeys.map(key => (
            <Line
              key={key}
              dot={false}
              dataKey={key}
              isAnimationActive={false}
              stroke={colors.languages[key]}
              strokeWidth={2}
              type='monotone'
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
      {auxiliary && (
        <div className={styles.auxiliary}>{auxiliary}</div>
      )}
    </div>
  )
}