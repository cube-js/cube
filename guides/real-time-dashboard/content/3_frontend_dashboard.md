---
order: 4
title: "Frontend Dashboard"
---

In the previous part we've successfully configured a database, [BigQuery](cube-js-backend-with-big-query) or [MongoDB](cube-js-backend-with-mongo-db), and created a Cube.js schema for the real-time dashboard. Now it is time to build a dashboard itself!

Cube.js Playground can generate a boilerplate frontend app. It is
a convenient way to start developing a dashboard or analytics application. You can
select your favorite frontend framework and charting library and Playground will
generate a new application and wire all things together to work with the Cube.js API.

We'll use React and Chart.js in our tutorial. To generate a new application,
navigate to "Dashboard App,” select "React Antd Static" with "Chart.js", turn on the "Web Socket Transport (Real-time)" switch, and click on the “Create dashboard app” button.


![](/images/4-screenshot-1.png)

It could take a while to generate an app and install all the dependencies. Once it is done, you will have a `dashboard-app` folder inside your Cube.js project folder. To start a dashboard app, either go to the “Dashboard App” tab in the playground and hit the “Start” button, or run the following command inside the `dashboard-app` folder:

```bash
$ npm start
```

Make sure the Cube.js backend process is up and running since our dashboard uses its API. The frontend application is running on http://localhost:3000.

To add a chart on the dashboard, you can either edit the `dashboard-app/src/pages/DashboardPage.js` file or use Cube.js Playground. To add a chart via Playground, navigate to the "Build" tab, build a chart you want, and click the "Add to Dashboard" button.

### Configure Cube.js for Real-Time Data Fetch

We need to do a few things for real-time support in Cube.js. First,
WebSockets transport should be enabled by setting the `CUBEJS_WEB_SOCKETS` environment variable to `true`.

Check that the following line is present in the `.env` file:

```bash
CUBEJS_WEB_SOCKETS=true
```

Next, we need to update the `cube.js` file to pass a few additional options to the
Cube.js server.

Update the content of the `cube.js` file the following.

```javascript
module.exports = {
  processSubscriptionsInterval: 1,
  orchestratorOptions: {
    queryCacheOptions: {
      refreshKeyRenewalThreshold: 1,
    }
  },
};
```

We have passed two configuration options to Cube.js. The first,
`processSubscriptionsInterval`, controls the polling interval. The default value
is 5 seconds; we are setting it to 1 second to make it slightly more real-time.

The second, `refreshKeyRenewalThreshold`, controls how often the `refreshKey` is
executed. The default value of this option is 120, which is 2 minutes. In the previous part, we've changed `refreshKey` to reset a cache every second, so it doesn't make sense for us to wait an additional 120 seconds to
invalidate the `refreshKey` result itself, that’s why we are changing it to 1 second
as well.

These are all the updates we need to make on the backend part. Now, let's update the
code of our dashboard app. First, let's check that the `@cubejs-client/ws-transport`
package is installed in the `package.json` file in the `dashboard-app` folder. It provides a WebSocket transport to work with the Cube.js real-time API:

```json
{
  "name": "dashboard-app",
  "version": "0.1.0",
  "private": true,
  "dependencies": {

    // ...

    "@cubejs-client/ws-transport": "^0.23.11",

    // ...
```

Now, we need to update how we request a query itself in the `src/components/ChartRenderer.js`. Make the following changes:

```diff
  const ChartRenderer = ({
-   vizState
+   vizState, cubejsApi
  }) => {
    const {
      query,
      chartType
    } = vizState;
    const component = TypeToMemoChartComponent[chartType];
-   const renderProps = useCubeQuery(query);
+   const renderProps = useCubeQuery(query, { subscribe: true, cubejsApi });
    return component && renderChart(component)(renderProps);
  };
```

That's it! Now you can add more charts to your dashboard, perform changes in the
database, and see how charts are updating in real time.

The video below shows the dashboard with the total count of events, number of users online, and the table with
the last events. You can see the charts update in real time as I insert new
data in the database.

`video: /videos/real-time-mongo-example.mov`

You can also check [this online live
demo](https://real-time-dashboard-demo.cube.dev) with various charts displaying
real-time data.

In the next part, we'll talk about how to deploy our application, both the
backend and the frontend.
