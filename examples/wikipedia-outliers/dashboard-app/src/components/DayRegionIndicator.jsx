import { getEmoji } from '../emoji'
import * as classes from './DayRegionIndicator.module.css'

function DayRegionIndicator({ day, region, count }) {
  return <div className={classes.root}>
    Top {count}
    {' '}
    {day ? `popular articles on ${new Date(day).toLocaleDateString(undefined, { year: 'numeric', month: 'long', day: 'numeric' })}` : 'all-time popular articles'}
    {' in '}
    {region ? `${getEmoji(region)} Wikipedia` : 'all Wikipedia languages'}
    {':'}
  </div>
}

export default DayRegionIndicator