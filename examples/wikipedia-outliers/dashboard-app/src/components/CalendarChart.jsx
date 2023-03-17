import { ResponsiveCalendar } from '@nivo/calendar'
import * as classes from './CalendarChart.module.css'

function CalendarChart({ data, toggleDay }) {
  const startDate = data.reduce((start, current) => !start || start > current.day ? current.day : start, undefined)
  const endDate = data.reduce((end, current) => !end || end < current.day ? current.day : end, undefined)

  return <div className={classes.root}>
    <ResponsiveCalendar
      data={data}
      from={startDate}
      to={endDate}
      onClick={({ day }) => toggleDay(day)}
      margin={{ top: 20, right: 20, bottom: 20, left: 20 }}
    />
  </div>
}

export default CalendarChart