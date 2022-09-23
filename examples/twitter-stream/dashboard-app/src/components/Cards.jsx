import * as classes from './Cards.module.css'

function SendTweetBlock({ children }) {
  return <div className={classes.root}>
    {children}
  </div>
}

export default SendTweetBlock