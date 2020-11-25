---
order: 5
title: "Building a Dashboard"
---

In the previous part we've created our basic data schema and built first few charts. In
this part we'll add more measures and dimensions to our data schema and build new charts on the dashboard.

We are going to add several KPI charts and one pie chart to our dashboard, like
on the schreenshot below.

Let's first create `<Chart />` component, which we're going to use to render
the KPI and Pie charts.

Create the `dashboard-app/src/components/Chart.js` file with the following
content.

```jsx
import React from "react";
import Card from "@material-ui/core/Card";
import CardContent from "@material-ui/core/CardContent";
import Typography from "@material-ui/core/Typography";

import ChartRenderer from "./ChartRenderer";

const Chart = ({ title, vizState }) => (
  <Card>
    <CardContent>
      <Typography component="p" color="primary" gutterBottom>
        {title}
      </Typography>
      <ChartRenderer vizState={vizState} />
    </CardContent>
  </Card>
);

export default Chart;
```

Let's use this `<Chart />` component to render couple KPI charts for measures we already
have in the data schema: Users and Sessions.

Make the following changes to the `dashboard-app/src/components/DashboardPage.js` file.

```diff
  import { makeStyles } from "@material-ui/core/styles";
  import Grid from "@material-ui/core/Grid";
  import OverTimeChart from "../components/OverTimeChart";
  import Dropdown from "../components/Dropdown";
+ import Chart from "../components/Chart";

  const useStyles = makeStyles(theme => ({
    root: {
    },
  };

+ const queries = {
+   users: {
+     chartType: 'number',
+     query: {
+       measures: ['Sessions.usersCount'],
+       timeDimensions: [{
+         dimension: 'Sessions.timestamp',
+         dateRange: "Last 30 days"
+       }]
+     }
+   },
+   sessions: {
+     chartType: 'number',
+     query: {
+       measures: ['Sessions.count'],
+       timeDimensions: [{
+         dimension: 'Sessions.timestamp',
+         dateRange: "Last 30 days"
+       }]
+     }
+   },
+ }

  const DashboardPage = () => {
    const classes = useStyles();
    const [overTimeQuery, setOverTimeQuery] = useState("Users");
    return (
-     <Grid item xs={12} className={classes.root}>
-       <OverTimeChart
-         title={
-           <Dropdown
-             value={overTimeQuery}
-             options={
-               Object.keys(overTimeQueries).reduce((out, measure) => {
-                 out[measure] = () => setOverTimeQuery(measure)
-                 return out;
-               }, {})
-             }
-           />
-         }
-         vizState={{
-           chartType: 'line',
-           query: overTimeQueries[overTimeQuery]
-         }}
-       />
+     <Grid container spacing={3}  className={classes.root}>
+       <Grid item xs={12}>
+         <OverTimeChart
+           title={
+             <Dropdown
+               value={overTimeQuery}
+               options={
+                 Object.keys(overTimeQueries).reduce((out, measure) => {
+                   out[measure] = () => setOverTimeQuery(measure)
+                   return out;
+                 }, {})
+               }
+             />
+           }
+           vizState={{
+             chartType: 'line',
+             query: overTimeQueries[overTimeQuery]
+           }}
+         />
+       </Grid>
+       <Grid item xs={6}>
+         <Grid container spacing={3}>
+           <Grid item xs={6}>
+             <Chart title="Users" vizState={queries.users} />
+           </Grid>
+           <Grid item xs={6}>
+             <Chart title="Sessions" vizState={queries.sessions} />
+           </Grid>
+         </Grid>
+       </Grid>
      </Grid>
    )
  };

  export default DashboardPage;
```

Refresh the dashboard after making the above changes and you should see something
like on the screenshot below.

![](/images/5-screenshot-1.png)

To add more charts on the dashboard, we first need to define new measures and
dimensions in our data schema.

## New Measures and Dimensions in Data Schema

In the previous part we've already built the foundation for our data schema and
covered some topics like sessionization. Now, we're going to add new measures on
top of the cubes we've created earlier.

Feel free to use Cube.js Playground to test new measures and dimensions as we
adding them. We'll update our dashboard with all newly created metrics in the
end of this part.

### Returning vs News Users

Let's add a way to figure out whether users are new or returning. To
distinguish **New** users from **Returning** we're going to use session's index
set by Snowplow tracker - `domain_sessionidx`.

First, create a new `type` dimension in the `Sessions` cube. We're using
[case property](https://cube.dev/docs/dimensions#parameters-case) to make this dimension return either `New` or `Returning` based on the
value of `domain_sessionidx`.

```js
type: {
  type: `string`,
  case: {
    when: [{ sql: `${CUBE}.domain_sessionidx = 1`, label: `New`}],
    else: { label: `Returning` }
  },
  title: `User Type`
}
```

Next, let's create a new measure to count only for "New Users". We're going to define `newUsersCount` measure by using [filters
property](https://cube.dev/docs/measures#parameters-filters) to select
only **new** sessions.

Add the following measure to the `Sessions` cube.


```js
newUsersCount: {
  type: `countDistinct`,
  sql: `domain_userid`,
  filters: [
    { sql: `${type} = 'New'` }
  ],
  title: "New Users"
}
```

We'll use `type` dimension to build "New vs Returning" pie chart. And `newUsersCount` measure for "New Users" KPI chart. Feel free to test these measure and dimension in the Playground meanwhile.

### Average Number of Events per Session

To calculate the average we need to have the number of events per session first. We can achieve that by creating a [subQuery dimension](https://cube.dev/docs/subquery#top). Subquery dimensions are used to reference measures from other cubes inside a dimension.

To make subQuery work we need to define a relationship between `Events` and `Sessions` cubes. Since, every event belongs to some session, we're going to define `belongsTo` join. You can [learn more about joins in Cube.js here](https://cube.dev/docs/joins#top).

Add the following block to the `Events` cube.

```js
joins: {
  Sessions: {
    relationship: `belongsTo`,
    sql: `${CUBE}.domain_sessionid = ${Sessions.id}`
  }
}
```

We'll calculate count of events, which we already have as a measure in the `Events` cube, as a dimension in the Sessions cube.

Once, we have this dimension we can easily calculate its average as a measure.

```js
// Add the following dimension to the Sessions cube
eventsCount: {
  type: `number`,
  sql: `${Events.count}`,
  subQuery: true
}

// Add the following measure to the Sessions cube
avgEvents: {
  type: `number`,
  sql: `round(avg(${eventsCount}))`
}
```
### Average Session Durarion

To calculate the average session duration we need first calculate the
duration of sessions as a dimension and then take the average of this dimension as a
measure.

To get the duration of the session we need to know when it
starts and when it ends. We already have the start time, which is our `time`
dimension. To get the `sessionEnd` we need to find the timestamp of the last
event in the session. we'll take the same approach here with the subQuery dimension as we did for number of events per session.

First, create the following measure in the `Events` cube.

```js
maxTimestamp: {
  type: `max`,
  sql: `derived_tstamp`
}
```

Next, create the subQuery dimension to find the last max timestamp for the
session. Add the following dimension to the `Sessions` cube.

```js
sessionEnd: {
  type: `time`,
  sql: `${Events.maxTimestamp}`,
  subQuery: true
}
```

Now, we have everything to calculate the duration of the session. Add the
`durationSeconds` dimension to the `Sessions` cube.

```js
durationSeconds: {
  sql: `date_diff('second', ${timestamp}, ${sessionEnd})`,
  type: `number`
}
```

The last step is to define the `averageDurationSeconds` measure in the
`Sessions` cube.

```js
averageDurationSeconds: {
  type: `avg`,
  sql: `${durationSeconds}`,
  meta: {
    format: 'time'
  }
}
```

In the above definition we're also using measure's [meta
property](https://cube.dev/docs/measures#parameters-meta). Cube.js has [several built-in measure
formats](https://cube.dev/docs/types-and-formats#measures-formats) like
`currency` or `percent`, but it doesn't have `time` format. In this case we can use
`meta` property to pass this information to the frontend to format it properly.

### Bounce Rate
The last metric for today is the **Bounce Rate**.

A bounced session is usually defined as a session with only one event. Since we’ve already defined the number of events per session, we can easily add a dimension `isBounced` to identify bounced sessions to the `Sessions` cube. Using this dimension, we can add two measures to the `Sessions` cube as well - a count of bounced sessions and a bounce rate.

```js
// Add the following dimension to the Sessions cube
isBounced: {
 type: `string`,
  case: {
    when: [ { sql: `${eventsCount} = 1`, label: `True` }],
    else: { label: `False` }
  }
}

// Add the following measures to the Sessuins cube
bouncedCount: {
  type: `count`,
  filters:[{
    sql: `${isBounced} = 'True'`
  }]
},

bounceRate: {
  sql: `100.00 * ${bouncedCount} / NULLIF(${count}, 0)`,
  type: `number`,
  format: `percent`
}
```


## Adding New Charts to the Dahsboard

Now, we can use these new measures and dimensions to add more charts to our
dashboard. But before doing it, let's make some changes on how we render the KPI
chart. We want to format the value differently depending on the format of the
measure - whether it is number,
percent or time.

Make the following changes to the `dashboard-app/src/components/ChartRenderer.js` file.

```diff
- number: ({ resultSet }) => (
-   <Typography
-     variant="h4"
-     style={{
-       textAlign: "center"
-     }}
-   >
-     {resultSet.seriesNames().map(s => resultSet.totalRow()[s.key])}
-   </Typography>
- ),
+ number: ({ resultSet }) => {
+   const measureKey = resultSet.seriesNames()[0].key;
+   const annotations = resultSet.tableColumns().find(tableColumn => tableColumn.key === measureKey)
+   const format = annotations.format || (annotations.meta && annotations.meta.format);
+   const value = resultSet.totalRow()[measureKey];
+   let formattedValue;
+   const percentFormatter = item => numeral(item/100.0).format('0.00%');
+   const timeNumberFormatter = item => numeral(item).format('00:00:00');
+   if (format === 'percent') {
+     formattedValue = percentFormatter(value);
+   } else if (format === 'time') {
+     formattedValue = timeNumberFormatter(value);
+   } else {
+     formattedValue = numberFormatter(value);
+   }
+   return (<Typography variant="h4" > {formattedValue} </Typography>)
+ },
```

Finally, we can make a simple change to the `<DashboardPage />` component. All
we need to do is to update the list of queries and chart items on the dashboard
with new metrics: New Users, Average Events per Sessions, Average Sessions
Duration, Bounce Rate and the breakdown of Users by Type.

Make the following changes to the `dashboard-app/src/pages/DashboardPage.js` file.

```diff
 const queries = {

 // ...

- }
+ },
+ newUsers: {
+   chartType: 'number',
+   query: {
+     measures: ['Sessions.newUsersCount'],
+     timeDimensions: [{
+       dimension: 'Sessions.timestamp',
+       dateRange: "Last 30 days"
+     }]
+   }
+ },
+ avgEvents: {
+   chartType: 'number',
+   query: {
+     measures: ['Sessions.avgEvents'],
+     timeDimensions: [{
+       dimension: 'Sessions.timestamp',
+       dateRange: "Last 30 days"
+     }]
+   }
+ },
+ avgSessionDuration: {
+   chartType: 'number',
+   query: {
+     measures: ['Sessions.averageDurationSeconds'],
+     timeDimensions: [{
+       dimension: 'Sessions.timestamp',
+       dateRange: "Last 30 days"
+     }]
+   }
+ },
+ bounceRate: {
+   chartType: 'number',
+   query: {
+     measures: ['Sessions.bounceRate'],
+     timeDimensions: [{
+       dimension: 'Sessions.timestamp',
+       dateRange: "Last 30 days"
+     }]
+   }
+ },
+ usersByType: {
+   chartType: 'pie',
+   query: {
+     measures: ['Sessions.usersCount'],
+     dimensions: ['Sessions.type'],
+     timeDimensions: [{
+       dimension: 'Sessions.timestamp',
+       dateRange: "Last 30 days"
+     }]
+   }
+ }

  // ...

  const DashboardPage = () => {

  // ...

          <Grid item xs={6}>
            <Chart title="Sessions" vizState={queries.sessions} />
          </Grid>
+         <Grid item xs={6}>
+           <Chart title="New Users" vizState={queries.newUsers} />
+         </Grid>
+         <Grid item xs={6}>
+           <Chart title="Avg. Events per Session" vizState={queries.avgEvents} />
+         </Grid>
+         <Grid item xs={6}>
+           <Chart title="Avg. Session Duration" vizState={queries.avgSessionDuration} />
+         </Grid>
+         <Grid item xs={6}>
+           <Chart title="Bounce Rate" vizState={queries.bounceRate} />
+         </Grid>
        </Grid>
      </Grid>
+     <Grid item xs={6}>
+       <Chart
+         title="Users by Type"
+         vizState={queries.usersByType}
+       />
+     </Grid>
    </Grid>
```

That's it for this chapter. We have added 7 more new charts to our dashboard.
If you navigate to the [http://localhost:3000](http://localhost:3000) you should see the dashboard with all these charts like on the screenshot below.

![](/images/5-screenshot-2.png)

In the next part, we'll add some filters to our dashboard to make it more
interactive and let users slice and filter the data.
