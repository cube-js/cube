/* eslint-disable import/no-anonymous-default-export */
import moment from 'moment';

const dateRange = [
  moment().subtract(14,'d').format('YYYY-MM-DD'),
  moment().format('YYYY-MM-DD'),
];

export default [
  {
    title: "Page Views last 14 days",
    type: "line",
    query: {
      "measures": [
        "Events.pageView"
      ],
      "timeDimensions": [
        {
          "dimension": "Events.time",
          "dateRange": dateRange,
          "granularity": "day"
        }
      ]
    }
  },
  {
    title: "Top 5 Events",
    type: "pie",
    query: {
      "measures": [
        "Events.anyEvent"
      ],
      "dimensions": [
        "Events.event"
      ],
      limit: 5
    }
  },
  {
    title: "Events list",
    type: "table",
    query: {
      "measures": [
        "Events.anyEvent",
        "Events.anyEventUniq"
      ],
      "dimensions": [
        "Events.event"
      ]
    }
  },
  {
    title: "Page Views by Page",
    type: "bar",
    query: {
      "measures": [
        "Events.pageView"
      ],
      dimensions: [
        "Events.pageTitle"
      ],
      "timeDimensions": [
        {
          "dimension": "Events.time",
          "dateRange": dateRange,
          "granularity": "day"
        }
      ]
    }
  }
]
