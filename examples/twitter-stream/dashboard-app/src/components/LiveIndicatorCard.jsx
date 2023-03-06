import * as classes from './LiveIndicatorCard.module.css'
// import { getTimestamp } from '../utils'

function LiveIndicatorCard() {
  // useEffect(() => {
  //   const interval = setInterval(() => {
  //     setCurrentTimestamp(getTimestamp())
  //   }, 1000)

  //   return () => clearInterval(interval)
  // }, [])

  return <div className={classes.root}>
  <p>Streaming...</p>
  <p className={classes.value}>
    <span className={classes.dot}>&#9679;</span>
    <span className={classes.dot}>&#9679;</span>
    <span className={classes.dot}>&#9679;</span>
  </p>
  <p className={classes.timestamp}>Real-time</p>
</div>
}

export default LiveIndicatorCard