---
order: 4
title: "Frontend App with React and Material UI"
---

We can quickly create a frontend application with Cube.js, because it can generate it using open-source, ready-to-use templates. We can just pick what technologies we need and it gets everything configured and ready to use. In the Developer Playground, navigate to the Dashboard App and click *Create Your Own*. We will use React, Material UI, and Recharts as our charting library.

![](/images/4-screenshot-1.png)

It will create the `dashboard-app` folder with the frontend application inside the project folder. It could take several minutes to download and install all the dependencies. Once it is done, you can start Dashboard App either from "Dashboard App" tab in the Playground or by running `npm start` inside the `dashboard-app` folder.

To keep things simple we're not going to build the [full demo
application](https://web-analytics-demo.cube.dev/), but
focus on the foundations of working with Cube.js API on the frontend, building the data schema and optimize the queries performance. We're going to build the [Audience Dashboard](https://web-analytics-demo.cube.dev/) and you can check [the source code of the rest of application on Github](https://github.com/cube-js/cube.js/tree/master/examples/web-analytics).

We'll start by building the top over time chart to display page views, users or
sessions with different time granularity options.

![](https://cube.dev/downloads/media/web-analytics-guide-gif-1.gif)

## Page Views Chart

Let's first define the data schema for the page views chart. In our
database page views are events with the type of `page_view` and platform `web`.
The type is stored in column called `event`. Let's create a new file for
`PageViews` cube.

Create the `schema/PageViews.js` with the following content.

```javascript
cube(`PageViews`, {
  extends: Events,
  sql: `
    SELECT
      *
    FROM ${Events.sql()} events
    WHERE events.platform = 'web' AND events.event = 'page_view'
  `
});
```

We've created a new cube and extended it from existing `Events` cube. This way
`PageViews` is going to have all the measures and dimensions from `Events` cube,
but will select events only with platform `web` and event type `page_view`.
You can [learn more about extending cubes here.](https://cube.dev/docs/extending-cubes)

You can test out newly created `PaveViews` in the Cube.js Plyground. Navigate to
the *Build* tab in the Playground, select *Page Views Count" in the measures
dropdown and you should be able to see the chart with your page views.

![](/images/4-screenshot-2.png)

Let's add this chart to our Dashboard App. First, we'll create the
`<OverTimeChart />` component. This component's job is to render the chart as
well as the switch buttons to let users change date's granularity between
hour, day, week, and, month.

Create the the `dashbooard-app/src/component/OverTimeChart.js` with the
following content.

```jsx
import React, { useState } from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";
import ButtonGroup from "@material-ui/core/ButtonGroup";
import Button from "@material-ui/core/Button";
import Grid from "@material-ui/core/Grid";
import ChartRenderer from "./ChartRenderer";

const withGranularity = ({ query, ...vizState }, granularity) => ({
  ...vizState,
  query: {
    ...query,
    timeDimensions: [{
      ...query.timeDimensions[0],
      granularity: granularity
    }]

  }
})

const OverTimeChart = ({ title, vizState, granularityControls }) => {
  const [granularity, setGranularity] = useState("day");
  return (
    <Card>
      <CardContent>
        <Grid container justify="space-between">
          <Grid item>
            <Typography component="p" color="primary" gutterBottom>
              {title}
            </Typography>
          </Grid>
          <Grid item>
            <ButtonGroup
              size="small"
              color="primary"
              aria-label="outlined primary button group"
            >
              {['hour', 'day', 'week', 'month'].map(granOption => (
                <Button
                  variant={granularity === granOption ? 'contained' : ''}
                  key={granOption}
                  onClick={() => setGranularity(granOption)}
                >
                  {granOption.toUpperCase()}
                </Button>
              ))}
            </ButtonGroup>
          </Grid>
        </Grid>
        <ChartRenderer
          height={250}
          vizState={withGranularity(vizState, granularity)}
        />
      </CardContent>
    </Card>
  )
};

export default OverTimeChart;
```

We are almost ready to plot the page views chart, but before doing it, let's customize
our chart rendering a little. This template has created the `<ChartRenderer />`
component which uses [Recharts](http://recharts.org/en-US/) to render the chart.
We're going to change formatting, colors and general appearance of the chart.

To nicely format numbers and dates values we can use [Numeral.js](http://numeraljs.com/) and [Moment.js](https://momentjs.com/) packages respectively. Let's install them, run the following command inside the `dashbooard-app` folder.

```bash
$ npm install --save numeral moment
```

Next, make the following changes in the
`dashbooard-app/src/components/ChartRenderer.js` file.

```diff
  import TableCell from "@material-ui/core/TableCell";
  import TableHead from "@material-ui/core/TableHead";
  import TableRow from "@material-ui/core/TableRow";
+ import moment from "moment";
+ import numeral from "numeral";
+ const dateFormatter = item => moment(item).format("MMM DD");
+ const numberFormatter = item => numeral(item).format("0,0");

  const CartesianChart = ({ resultSet, children, ChartComponent }) => (
    <ResponsiveContainer width="100%" height={350}>
-     <ChartComponent data={resultSet.chartPivot()}>
-       <XAxis dataKey="x" />
-       <YAxis />
-       <CartesianGrid />
+     <ChartComponent
+       margin={{
+             top: 16,
+             right: 16,
+             bottom: 0,
+             left: 0,
+           }}
+        data={resultSet.chartPivot()}
+      >
+       <XAxis dataKey="x" axisLine={false} tickLine={false} tickFormatter={dateFormatter} />
+       <YAxis axisLine={false} tickLine={false} />
+       <CartesianGrid vertical={false} />
        {children}
        <Legend />
-       <Tooltip />
+       <Tooltip labelFormatter={dateFormatter} formatter={numberFormatter} />
      </ChartComponent>
    </ResponsiveContainer>
  );

- const colors = ["#FF6492", "#141446", "#7A77FF"];
+ const colors = ["#4791db", "#e33371", "#e57373"];
```

The code above uses Moment.js and Numeral.js to define formatter for axes and
tooltip, passes some additional properties to Recharts components and changes the
colors of the chart. With this approach you can fully customize your charts'
look and feel to fit your application's design.

Now, we are ready to plot our page views chart. The template generated the
`<DashboardPage />` component which is an entry point of our frontend application. We're
going to render all our dashboard inside this component.

Replace the content of the `dashboard-app/src/pages/DashboardPage.js` with the
following.

```jsx
import React from "react";
import { makeStyles } from "@material-ui/core/styles";
import Grid from "@material-ui/core/Grid";
import OverTimeChart from "../components/OverTimeChart";

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(3),
  }
}));

const DashboardPage = () => {
  const classes = useStyles();
  return (
    <Grid item xs={12} className={classes.root}>
      <OverTimeChart
        vizState={{
          chartType: 'line',
          query: {
            measures: ["Sessions.count"],
            timeDimensions: [{
              dimension: "Sessions.timestamp",
              granularity: "day",
              dateRange: "Last 30 days"
            }]
          }
        }}
      />
    </Grid>
  )
};

export default DashboardPage;
```

The code above is pretty straightforward - we're using our newly created
`<OverTimeChart />` to render the page views chart by passing the [Cube.js JSON
Query](https://cube.dev/docs/query-format) inside the `vizState` prop.

Navigate to the http://localhost:3000 in your browser and you should be able to see
the chart like the one below.

![](/images/4-screenshot-3.png)

## Adding Sessions and Users Charts

Next, let's build sessions chart. A session is defined as a group of interactions one user takes within a given time frame on your app. Usually that time frame defaults to 30 minutes, meaning that whatever a user does on your app (e.g. browses pages, downloads resources, purchases products) before they leave equals one session.

As you probably noticed before we're using the ROW_NUMBER window function in our
Events cube definition to calculate the index of the event in the session.

```sql
ROW_NUMBER() OVER (PARTITION BY domain_sessionid ORDER BY derived_tstamp) AS event_in_session_index
```

We can use this index to aggregate our events into sessions. We rely here on the `domain_sessionid` set by Snowplow tracker, but you can also implement your own sessionization with Cube.js to have more control over how you want to define sessions or in case you have multiple trackers and you can not rely on the client-side sessionization. You can check [this tutorial for sessionization with Cube.js](https://cube.dev/docs/event-analytics).

Let's create `Sessions` cube in `schema/Sessions.js` file.

```javascript
cube(`Sessions`, {
  sql: `
   SELECT
    *
   FROM ${Events.sql()} AS e
   WHERE e.event_in_session_index = 1
  `,

  measures: {
    count: {
      type: `count`
    }
  },

  dimensions: {
    timestamp: {
      type: `time`,
      sql: `derived_tstamp`
    },

    id: {
      sql: `domain_sessionid`,
      type: `string`,
      primaryKey: true
    }
  }
});
```

We'll use `Sessions.count` measure to plot the sessions on our over time chart.
To plot users we need to add one more measure to the `Sessions` cube.

Snowplow tracker assigns user ID by using 1st party cookie. We can find this
user ID in `domain_userid` column. To plot users chart we're going to use the existing `Sessions` cube, but we will count not all the sessions, but only unique by `domain_userid`.

Add the following measure to the `Sessions` cube.

```javascript
usersCount: {
  type: `countDistinct`,
  sql: `domain_userid`,
}
```

Now, let's add the dropdown to our chart to let
users select what they want to plot: page views, sessions, or users.

First, let's create a simple `<DropDown />` component. Create the `dashboard-app/src/components/Dropdown.js` file with the following content.

```jsx
import React from 'react';
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';

export default function Dropdown({ value, options }) {
  const [anchorEl, setAnchorEl] = React.useState(null);
  const open = Boolean(anchorEl);

  const handleClose = (callback) => {
    setAnchorEl(null);
    callback && callback();
  };

  return (
    <div>
      <Button
        color="inherit"
        aria-haspopup="true"
        onClick={({ currentTarget }) => setAnchorEl(currentTarget)}
      >
        { value }
        <ExpandMoreIcon fontSize="small" />
      </Button>
      <Menu
        id="long-menu"
        anchorEl={anchorEl}
        keepMounted
        open={open}
        onClose={() => handleClose() }
      >
        {Object.keys(options).map(option => (
          <MenuItem key={option} onClick={() => handleClose(options[option])}>
            {option}
          </MenuItem>
        ))}
      </Menu>
    </div>
  );
}
```

Now, let's use it on our dashboard page alongside adding new charts for
users to select from.
Make the following changes in the `dashboard-app/src/pages/DashboardPage.js`
file.


```diff
- import React from "react";
+ import React, { useState } from "react";
  import { makeStyles } from "@material-ui/core/styles";
  import Grid from "@material-ui/core/Grid";
  import OverTimeChart from "../components/OverTimeChart";
+ import Dropdown from "../components/Dropdown";

  const useStyles = makeStyles(theme => ({
    root: {
      padding: theme.spacing(3),
    }
  }));

+ const overTimeQueries = {
+   "Users": {
+     measures: ["Sessions.usersCount"],
+     timeDimensions: [{
+       dimension: "Sessions.timestamp",
+       granularity: "day",
+       dateRange: "Last 30 days"
+     }]
+   },
+   "Sessions": {
+     measures: ["Sessions.count"],
+     timeDimensions: [{
+       dimension: "Sessions.timestamp",
+       granularity: "day",
+       dateRange: "Last 30 days"
+     }]
+   },
+   "Page Views": {
+     measures: ["PageViews.count"],
+     timeDimensions: [{
+       dimension: "PageViews.timestamp",
+       granularity: "day",
+       dateRange: "Last 30 days"
+     }]
+   },
+ };

  const DashboardPage = () => {
    const classes = useStyles();
+   const [overTimeQuery, setOverTimeQuery] = useState("Users");
    return (
      <Grid item xs={12} className={classes.root}>
        <OverTimeChart
+         title={
+           <Dropdown
+             value={overTimeQuery}
+             options={
+               Object.keys(overTimeQueries).reduce((out, measure) => {
+                 out[measure] = () => setOverTimeQuery(measure)
+                 return out;
+               }, {})
+             }
+           />
+         }
          vizState={{
            chartType: 'line',
-           query: {
-             measures: ["Sessions.count"],
-             timeDimensions: [{
-               dimension: "Sessions.timestamp",
-               granularity: "day",
-               dateRange: "Last 30 days"
-             }]
-           }
+           query: overTimeQueries[overTimeQuery]
          }}
        />
      </Grid>
    )
  };

  export default DashboardPage;
```

Navigate to http://localhost:3000 and you should be able to switch between charts and change the granularity like on the animated image below.

![](https://cube.dev/downloads/media/web-analytics-guide-gif-1.gif)

In the next part we'll add more new charts to this dashboard! 📊🎉

