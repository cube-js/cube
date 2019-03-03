import Funnels from 'Funnels';

import { eventsSQl, PAGE_VIEW_EVENT } from './Events.js';

const funnels = [
  {
    title: 'ReportsFunnel',
    steps: {
      view_reports_page: [PAGE_VIEW_EVENT, 'Reports'],
      selected_event: ['Reports', 'Event Selected'],
      report_saved: ['Reports', 'Save Button Clicked']
    }
  },
  {
    title: 'FunnelsUsageFunnel',
    steps: {
      view_any_page: [PAGE_VIEW_EVENT],
      view_funnels_page: [PAGE_VIEW_EVENT, 'Funnels'],
      funnel_selected: ['Funnels', 'Funnel Selected']
    }
  },
  {
    title: 'FunnelsEditFunnel',
    steps: {
      view_funnels_page: [PAGE_VIEW_EVENT, 'Funnels'],
      funnel_selected: ['Funnels', 'Funnel Selected'],
      edit_button_clicked: ['Funnels', 'Edit Button Clicked']
    }
  }
]

class Funnel {
  constructor({ title, steps }) {
    this.title = title;
    this.steps = steps;
  }

  get transformedSteps() {
    return Object.keys(this.steps).map((key, index) => {
      const value = this.steps[key];
      let where = null
      if (value[0] === PAGE_VIEW_EVENT) {
        if (value.length === 1) {
          where = `event = '${value[0]}'`
        } else {
          where = `event = '${value[0]}' AND page_title = '${value[1]}'`
        }
      } else {
        where = `event = 'se' AND se_category = '${value[0]}' AND se_action = '${value[1]}'`
      }

      return {
        name: key,
        eventsView: {
          sql: () => `select * from (${eventsSQl}) WHERE ${where}`
        },
        timeToConvert: index > 0 ? '30 day' : null
      }
    });
  }

  get config() {
    return {
      userId: {
        sql: () => `user_id`
      },
      time: {
        sql: () => `time`
      },
      steps: this.transformedSteps
    }
  }
}

funnels.forEach((funnel) => {
  const funnelObject = new Funnel(funnel);
  cube(funnelObject.title, {
    extends: Funnels.eventFunnel(funnelObject.config),
    preAggregations: {
      main: {
        type: `originalSql`,
      }
    }
  });
});
