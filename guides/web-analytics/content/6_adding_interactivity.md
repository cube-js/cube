---
order: 6
title: "Adding Interactivity"
---

Currently all our charts are hardcoded to show the data for the last 30 days. Let's add the date range picker to our dashboard to let users change it.

We'll use the date picker from the `@material-ui/pickers` package. First we need
to install it alongside its dependencies. All we need to do on the frontend is
just to dynamically change the date range in the JSON queries. Cube.js will
generate a new SQL query based on the updated date range and execute it against our Athena database.

Run the following command in your terminal inside the `dashboard-app` folder.

```bash
$ npm install --save @material-ui/pickers date-fns @date-io/date-fns@1.3.13
```

Next, update the `<DashboardPage />` in the
`dashboard-app/src/pages/DashboardPage.js` file the following content.
Make sure to include the `DashboardItems` from the previous part here as well.

```js
import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";
import ChartRenderer from "../components/ChartRenderer";
import Dashboard from "../components/Dashboard";
import DashboardItem from "../components/DashboardItem";
import {
  MuiPickersUtilsProvider,
  KeyboardDatePicker
} from "@material-ui/pickers";
import DateFnsUtils from "@date-io/date-fns";
import { subDays } from 'date-fns'
const DEFAULT_BEGIN_DATE = subDays(new Date(), 30);
const DEFAULT_END_DATE = new Date();

const DashboardItems = [
  // Charts from the previous part
]

const withTimeDimension = (dateRange, { query, ...options }) => ({
  ...options,
  query: {
    timeDimensions: [
      {
        dimension: "Sessions.timestamp",
        granularity: "day",
        dateRange: dateRange
      }
    ],
    ...query
  }
});

const DashboardPage = () => {
  const [beginDate, setBeginDate] = useState(DEFAULT_BEGIN_DATE);
  const [endDate, setEndDate] = useState(DEFAULT_END_DATE);

  const dashboardItem = item => (
    <Grid item xs={12} lg={6} key={item.id}>
      <DashboardItem title={item.name}>
        <ChartRenderer vizState={withTimeDimension([beginDate, endDate], item.vizState)} />
      </DashboardItem>
    </Grid>
  );

  return (
    <MuiPickersUtilsProvider utils={DateFnsUtils}>
      <Dashboard>
        <Grid item xs={12}>
          <KeyboardDatePicker
            value={beginDate}
            onChange={(date) => { setBeginDate(date) }}
          />
          <KeyboardDatePicker
            value={endDate}
            onChange={(date) => { setEndDate(date) }}
          />
        </Grid>
        {DashboardItems.map(dashboardItem)}
      </Dashboard>
    </MuiPickersUtilsProvider>
  )
};

export default DashboardPage;
```

The above snippet stores the `beginDate` and `endDate` dates the component's state, inserts two date picker components into our web page and updates these variables in `onChange` handlers. The `withTimeDimension` function inserts the date range in every chart, effectively making them reload data when values in date range pickers change.

With these new changes, we can reload our dashboard, change the values in the
date pickers and see charts reload.


GIF

As you can see, there is quite a delay to load an updated chart. Every time we
change the values in the date picker we send 6 new SQL queries to be executed in
the Athena. Although, Athena is good at processing large volumes of data, it is
bad at handling a lot of small simultaneous queries. It also can get costly
quite quickly if we continue to execute queries against the raw all the time.

In the next part we'll cover how to optimize performance and cost by using
Cube.js pre-aggregations.
