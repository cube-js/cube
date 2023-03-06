import * as classes from './Card.module.css'
import { getTimestamp } from '../utils'
import { useState, useEffect } from 'react'

function Card({ title, value, timestamp }) {
  const [ currentTimestamp, setCurrentTimestamp ] = useState(getTimestamp())

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentTimestamp(getTimestamp())
    }, 1000)

    return () => clearInterval(interval)
  }, [])

  return <div className={classes.root}>
    <p>{title}</p>
    <p className={classes.value}>{value}</p>
    <p className={classes.timestamp}>{currentTimestamp - timestamp} secs ago</p>
  </div>
}

export default Card