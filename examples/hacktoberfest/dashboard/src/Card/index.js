import React from 'react'
import styles from './styles.module.css'
import { formatNumber, roundNumberWithThousandPrecision } from '../util'

export default function Card(props) {
  const {
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
      {auxiliary && (
        <div className={styles.auxiliary}>{auxiliary}</div>
      )}
    </div>
  )
}