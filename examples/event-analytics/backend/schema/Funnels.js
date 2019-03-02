import Funnels from 'Funnels';

import { eventsSQl, PAGE_VIEW_EVENT } from './Events.js';

let funnelConfig = {
  userId: {
    sql: () => `user_id`
  },
  time: {
    sql: () => `time`
  },
  steps: []
}

const funnels = [
  {
    title: 'ReportsFunnel',
    steps: {
      view_reports_page: [PAGE_VIEW_EVENT, 'Reports'],
      selected_event: ['Reports', 'Event Selected'],
      saved_report: ['Reports', 'Save Button Clicked']
    }
  }
]

funnels.forEach(({ title, steps }) => {
  Object.keys(steps).forEach((key) => {
    const value = steps[key];
    let where = null
    if (value[0] === PAGE_VIEW_EVENT) {
      where = `event = '${value[0]}' AND page_title = '${value[1]}'`
    } else {
      where = `event = 'se' AND se_category = '${value[0]}' AND se_action = '${value[1]}'`
    }

    funnelConfig.steps.push({
      name: key,
      eventsView: {
        sql: () => `select * from (${eventsSQl}) WHERE ${where}`
      }
    })
  });

  cube(title, {
    extends: Funnels.eventFunnel(funnelConfig)
  });
});
