---
order: 4
title: "Frontend App with React and Material UI"
---

We can quickly generate a frontend application with Cube.js Templates. Templates are open-source, ready-to-use frontend analytics apps. We can just pick what technologies we need and it gets everything configured and ready to use. In the Cube.js playground navigate to the Dashboard App and click *Create Your Own*. We will use React and Material UI and let's pick Recharts as our charting library.

SCREENSHOT

It will create the `dashboard-app` folder with the frontend application inside the project folder. It could take several minutes to download and install all the dependencies. Once it is done, you can start Dashboard App either from "Dashboard App" tab in the Playground or by running `npm run start` inside the `dashboard-app` folder.

To keep things simple we're not going to build the [full demo
application](https://web-analytics-demo.cube.dev/), but
focus on the foundations of working with Cube.js API on the frontend, building the data schema and optimize the queries performance. We're going to build the [Audience Dashboard](https://web-analytics-demo.cube.dev/) and you can check [the source code of the rest of application on Github](https://github.com/cube-js/cube.js/tree/master/examples/web-analytics).


Let's

## Page Views Chart

Let's first build the chart to show daily pageviews for our website. In our
database pageviews are events with the type of `page_view` and platform `web`.
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


Now, we can add page views chart to our dashboard.  Frontend application is already
configured to render data from Cube.js backend with Recharts. So we only to
specify what Cube.js query we want to render and with what visualization type.
To do that we need to edit `dashboard-app/src/pages/DashboardPage.js`.

```diff
- const DashboardItems = [];
+ const DashboardItems = [
+   {
+     id: 0,
+     name: "Page Views",
+     vizState: {
+       query: {
+         measures: ["PageViews.count"],
+         timeDimensions: [
+           {
+             dimension: "PageViews.timestamp",
+             granularity: "day",
+             dateRange: "Last 30 days"
+           }
+         ],
+         order: {
+           "PageViews.timestamp": "asc"
+         },
+         filters: []
+       },
+       chartType: "line"
+     }
+   }
+ ];
```

The query above in the `vizState` property is the Cube.js JSON query, you can
[learn more about it and its format here.](https://cube.dev/docs/query-format)
With this query our dashboard app should display the pageviews chart.

![](/images/4-screenshot-1.png)

## Sessions Chart

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

We can change our query in the dashboard app to display sessions instead of the page views.

```diff
 const DashboardItems = [
   {
     id: 0,
-    name: "Page Views",
+    name: "Sessions",
     vizState: {
       query: {
-        measures: ["PageViews.count"],
+        measures: ["Sessions.count"],
         timeDimensions: [
           {
-            dimension: "PageViews.timestamp",
+            dimension: "Sessions.timestamp",
             granularity: "day",
             dateRange: "Last 30 days"
           }
         ],
         order: {
-          "PageViews.timestamp": "asc"
+          "Sessions.timestamp": "asc"
         },
         filters: []
       },
     },
     chartType: "line"
   }
 ];
```

SCREENSHOT

## Users Chart

Snowplow tracker assigns user ID by using 1st party cookie. We can find this
user ID in `domain_userid` column. To plot users chart we're going to use the existing `Sessions` cube, but we will count not all the sessions, but only unique by `domain_userid`.

Add the following measure to the `Sessions` cube.

```javascript
usersCount: {
  type: `countDistinct`,
  sql: `domain_userid`,
}
```

Update the query in the `dashboard-app/src/pages/DashboardPage.js`

```diff
-    name: "Sessions",
+    name: "Users",
     vizState: {
       query: {
-        measures: ["Sessions.count"],
+        measures: ["Sessions.usersCount"],
         timeDimensions: [
           {
             dimension: "Sessions.timestamp",
```

That will give us the following Users chart.

SCREENSHOT

In the next part we'll use Users Chart and some new metrics to build our web analytics dashboard!📊🎉

