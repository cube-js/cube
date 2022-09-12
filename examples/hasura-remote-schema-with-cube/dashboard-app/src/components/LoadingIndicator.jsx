import * as classes from './LoadingIndicator.module.css'

function LoadingIndicator() {
  return <div style={{display: 'flex', justifyContent: 'center', marginTop: '20px'}}><div className={classes.ldsring}><div></div><div></div><div></div><div></div></div></div>;
}

export default LoadingIndicator