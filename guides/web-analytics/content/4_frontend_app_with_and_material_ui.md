---
order: 4
title: "Frontend App with React and Material UI"
---

We can quickly generate a frontend application with Cube.js Templates. Templates are open-source, ready-to-use frontend analytics apps. We can just pick what technologies we need and it gets everything configured and ready to use. In the Cube.js playground navigate to the Dashboard App and click *Create Your Own*. We will use React and Material UI and let's pick Recharts as our charting library.

SCREENSHOT

It will take several minutes to setup Dashboard App and install all the dependencies. It will create the `dashboard-app` folder inside the project folder. That is where all the frontend code goes. You can start Dashboard App either from "Dashboard App" tab in the Playground or by running `npm run start` inside the `dashboard-app` folder.

To keep things simple we're not going to build the [full demo
application](https://web-analytics-demo.cube.dev/), but
focus on the foundations of working with Cube.js API on the frontend, building the data schema and optimize the queries performance.
As we progress throughout our tutorial we'll partially rebuild the [Audience dashboard page](https://web-analytics-demo.cube.dev/#/) from the demo application.

Let's first build the chart to show daily pageviews for our website. In our
database pageviews are events with the type of `page_view` and platform `web`.
The type is stored in column called `event`. Let's create a new file for
`PageViews` cube. Create the `schema/PageViews.js` with the following content.

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

Pageviews Chart

Sessions Chart - talk about sessionization

Users Chart

Several single values + schema examples
