import * as classes from './LoadingIndicator.module.css'

function LoadingIndicator() {
  return <div className={classes.root}>
    Loading pre-aggregated data from Cube as a Hasura Remote Graph...
  </div>
}

export default LoadingIndicator