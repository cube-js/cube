---
order: 6
title: "Adding Interactivity"
---

Currently all our charts are hardcoded to show the data for the last 30 days. Let's add the date range picker to our dashboard to let users change it.
To keep things simple, we'll use the date range picker package we created specifically for this tutorial. Feel free to use any other date range picker component  in your application.

To install this package run the following command in your terminal inside the `dashboard-app` folder.

```bash
$ npn install --save daterange-web-analytics-demo date-fns@^2.14.0
```

Next, update the `<DashboardPage />` in the
`dashboard-app/src/pages/DashboardPage.js` file the following content.

```jsx
import React, { useState } from "react";
import { makeStyles } from "@material-ui/core/styles";
import Grid from "@material-ui/core/Grid";
import OverTimeChart from "../components/OverTimeChart";
import Dropdown from "../components/Dropdown";
import Chart from "../components/Chart";
import { subDays, format } from 'date-fns'
import DateRangePicker from "daterange-web-analytics-demo";

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(3),
  }
}));

const overTimeQueries = {
  "Users": { measures: ["Sessions.usersCount"], },
  "Sessions": { measures: ["Sessions.count"] },
  "Page Views": {
    measures: ["PageViews.count"],
    timeDimensions: [{ dimension: "PageViews.timestamp" }]
  },
};

const queries = {
  users: {
    chartType: 'number',
    query: { measures: ['Sessions.usersCount'] }
  },
  sessions: {
    chartType: 'number',
    query: { measures: ['Sessions.count'] }
  },
  newUsers: {
    chartType: 'number',
    query: { measures: ['Sessions.newUsersCount'] }
  },
  avgEvents: {
    chartType: 'number',
    query: { measures: ['Sessions.avgEvents'] }
  },
  avgSessionDuration: {
    chartType: 'number',
    query: { measures: ['Sessions.averageDurationSeconds'] }
  },
  bounceRate: {
    chartType: 'number',
    query: { measures: ['Sessions.bounceRate'] }
  },
  usersByType: {
    chartType: 'pie',
    query: { measures: ['Sessions.usersCount'], dimensions: ['Sessions.type'] }
  }
}

const DashboardPage = () => {
  const [beginDate, setBeginDate] = useState(subDays(new Date(), 7));
  const [endDate, setEndDate] = useState(new Date());
  const withTime = ({ query, ...vizState }) => {
    const timeDimensionObj = (query.timeDimensions || [])[0] || {};
    const dimension = timeDimensionObj.dimension || 'Sessions.timestamp';
    const granularity = timeDimensionObj.granularity || null;
    return {
      ...vizState,
      query: {
        ...query,
        timeDimensions: [{
          dimension,
          granularity,
          dateRange: [beginDate, endDate].map(d => format(d, "yyyy-MM-dd'T'HH:mm")),
        }]
      }
    }
  };
  const classes = useStyles();
  const [overTimeQuery, setOverTimeQuery] = useState("Users");
  return (
    <Grid container spacing={3}  className={classes.root}>
      <Grid item xs={12}>
        <Grid
          container
          spacing={3}
          justify="flex-end"
        >
          <Grid item xs={3}>
            <DateRangePicker
              value={[beginDate, endDate]}
              placeholder="Select a date range"
              onChange={values => {
                setBeginDate(values.begin);
                setEndDate(values.end);
              }}
            />
          </Grid>
        </Grid>
      </Grid>
      <Grid item xs={12}>
        <OverTimeChart
          title={
            <Dropdown
              value={overTimeQuery}
              options={
                Object.keys(overTimeQueries).reduce((out, measure) => {
                  out[measure] = () => setOverTimeQuery(measure)
                  return out;
                }, {})
              }
            />
          }
          vizState={withTime({
            chartType: 'line',
            query: overTimeQueries[overTimeQuery]
          })}
        />
      </Grid>
      <Grid item xs={6}>
        <Grid container spacing={3}>
          <Grid item xs={6}>
            <Chart title="Users" vizState={withTime(queries.users)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Sessions" vizState={withTime(queries.sessions)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="New Users" vizState={withTime(queries.newUsers)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Avg. Events per Session" vizState={withTime(queries.avgEvents)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Avg. Session Duration" vizState={withTime(queries.avgSessionDuration)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Bounce Rate" vizState={withTime(queries.bounceRate)} />
          </Grid>
        </Grid>
      </Grid>
      <Grid item xs={6}>
        <Chart
          title="Users by Type"
          vizState={withTime(queries.usersByType)}
        />
      </Grid>
    </Grid>
  )
};

export default DashboardPage;
```

In the code above we've introduced the `withTime` function which inserts values from
date range picker into every query.

With these new changes, we can reload our dashboard, change the value in the
date range picker and see charts reload.

As you can see, there is quite a delay to load an updated chart. Every time we
change the values in the date picker we send 6 new SQL queries to be executed in
the Athena. Although, Athena is good at processing large volumes of data, it is
bad at handling a lot of small simultaneous queries. It also can get costly
quite quickly if we continue to execute queries against the raw all the time.

In the next part we'll cover how to optimize performance and cost by using
Cube.js pre-aggregations.
