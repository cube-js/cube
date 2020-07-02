---
order: 5
title: "Building a Dashboard"
---

In the previous part we've built our basic data schema and first few charts. In
this part we'll build a dashboard with the users chart and several new metrics.

First, we need to slightly update the `DashboardPage` component to extract
setting `timeDimensions` property into a separate function. It's going to be a common property for all our queries. We also are going to make it dynamic based on the user's input in the next part of this tutorial.

Replace the content of the `dashboard-app/src/pages/DashboardPage.js` with the
following.

```jsx
import React from "react";
import Grid from "@material-ui/core/Grid";
import ChartRenderer from "../components/ChartRenderer";
import Dashboard from "../components/Dashboard";
import DashboardItem from "../components/DashboardItem";
const DashboardItems = [
  {
    id: 0,
    name: "Users",
    vizState: {
      query: {
        measures: ["Sessions.usersCount"]
      },
      chartType: "line"
    }
  },
];

const withTimeDimension = ({ query, ...options }) => ({
  ...options,
  query: {
    timeDimensions: [
      {
        dimension: "Sessions.timestamp",
        granularity: "day",
        dateRange: "Last 30 days"
      }
    ],
    ...query
  }
});

const DashboardPage = () => {
  const dashboardItem = item => (
    <Grid item xs={12} lg={6} key={item.id}>
      <DashboardItem title={item.name}>
        <ChartRenderer vizState={withTimeDimension(item.vizState)} />
      </DashboardItem>
    </Grid>
  );

  return <Dashboard>{DashboardItems.map(dashboardItem)}</Dashboard>
};

export default DashboardPage;
```

## New Users

We already have created the Users chart in the previous part. Let's add few more. First, let's add the chart to display the number of new users. To do that, we need to
a create a new measure in our data schema to count only new users. To
distinguish **New** users from **Returning** we're going to use session's index
set by Snowplow tracker - `domain_sessionidx`.

Let's first create a new `type` dimension in the `Sessions` cube. We're using
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

Next, let's define `newUsersCount` measure by using [filters
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

Finally, on the frontend in the `dashboard-app/src/pages/DashboardPage.js` file add the following query to the `DashboardItems` array.

```js
{
  id: 1,
  name: "New Users",
  vizState: {
    query: {
      measures: ["Sessions.newUsersCount"]
    },
    chartType: "bar"
  }
}
```

## Average Number of Events per Session

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

Same as the previous one we'll add average number of events per sessions as a
KPI chart. Add the following query to the `DashboardItems` array.

```js
{
  id: 2,
  name: "Avg. Events per Session",
  vizState: {
    query: {
      measures: ["Sessions.avgEvents"]
    },
    chartType: "line"
  }
}
```

## Users by Type

Now, let's plot New vs Returning users over time with a bar chart! We already have the `type` dimension which shows exactly this, so all we need here is to add the following query to
our `DashboardItems` array in the frontend app.

```js
{
  id: 3,
  name: "Users by Type",
  vizState: {
    query: {
      measures: ["Sessions.usersCount"],
      dimensions: ["Sessions.type"],
    },
    chartType: "bar"
  }
}
```

## Sessions by Referrer Medium

This metric would help us to figure which channel or medium bring the
most traffic to our website. Snowplow sets the `refr_medium`, so we just
need to clean up the values a little bit in the data schema.

```js
referrerMedium: {
  type: `string`,
  case: {
    when: [
      { sql: `${CUBE}.refr_medium IS NULL`, label: 'direct' },
      { sql: `${CUBE}.refr_medium = 'unknown'`, label: 'other' },
      { sql: `${CUBE}.refr_medium != ''`, label: { sql: `${CUBE}.refr_medium` } }
    ],
    else: { label: '(none)' }
  }
}
```

Add let's add it to the `DashboardItems` on the frontend as an area chart.

```js
{
  id: 3,
  size: 6,
  name: "Sessions by Medium",
  vizState: {
    query: {
      measures: ["Sessions.count"],
      dimensions: ["Sessions.referrerMedium"]
    },
    chartType: "area"
  }
}
```

## Bounce Rate
The last metric we're going to add to our dashboard is the **Bounce Rate**.

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

Now, let's add to the `DashboardItems` array as a line chart.

```js
{
  id: 4,
  name: "Bounce Rate",
  vizState: {
    query: {
      measures: ["Sessions.bounceRate"]
    },
    chartType: "line"
  }
}
```

That's it for this chapter. We have added 5 more new charts to our dashboard.
If you navigate to the [http://localhost:3000](http://localhost:3000) you should see the dashboard with all these charts like on the screenshot below.

![](/images/5-screenshot-1.png)

In the next part, we'll add some filters to our dashboard to make it more
interactive and let users slice and filter the data.
