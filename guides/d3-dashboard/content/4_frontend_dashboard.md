---
order: 4
title: "Building a Frontend Dashboard"
---

Now we are ready to build our frontend application. We’re going to use Cube.js Templates, which is a scaffolding engine for quickly creating frontend applications configured to work with the Cube.js API. It provides a selection of different frontend frameworks, UI kits, and charting libraries to mix together. We’ll pick React, Material UI, and D3.js. Let’s navigate to the Dashboard App tab and create a new dashboard application.

![](/images/4-screenshot-1.png)

It could take several minutes to generate an app and install all the dependencies. Once it is done, you will have a `dashboard-app` folder inside your Cube.js project folder. To start a frontend application, either go to the “Dashboard App” tab in the playground and hit the “Start” button, or run the following command inside the dashboard-app folder:

```bash
$ npm start
```

Make sure the Cube.js API is up and running since our frontend application uses it. The frontend application is running on [http://localhost:3000](http://localhost:3000). If you open it in your browser, you should be able to see an empty dashboard.

![](/images/4-screenshot-2.png)

To add a chart to the dashboard, we can either build it in the playground and click the “add to dashboard” button or edit the `src/pages/DashboardPage.js` file in the `dashboard-app` folder. Let’s go with the latter option. Among other things, this file declares the `DashboardItems` variable, which is an array of queries for charts.

Edit `dashboard-app/src/pages/DashboardPage.js` to add charts to the dashboard.


```diff
-const DashboardItems = [];
+const DashboardItems = [
+  {
+    id: 0,
+    name: "Orders last 14 days",
+    vizState: {
+      query: {
+        measures: ["Orders.count"],
+        timeDimensions: [
+          {
+            dimension: "Orders.createdAt",
+            granularity: "day",
+            dateRange: "last 14 days"
+          }
+        ],
+        filters: []
+      },
+      chartType: "line"
+    }
+  },
+  {
+    id: 1,
+    name: "Orders Status by Customers City",
+    vizState: {
+      query: {
+        measures: ["Orders.count"],
+        dimensions: ["Users.city", "Orders.status"],
+        timeDimensions: [
+          {
+            dimension: "Orders.createdAt",
+            dateRange: "last year"
+          }
+        ]
+      },
+      chartType: "bar",
+      pivotConfig: {
+        x: ["Users.city"],
+        y: ["Orders.status", "measures"]
+      }
+    }
+  },
+  {
+    id: 3,
+    name: "Orders by Product Categories Over Time",
+    vizState: {
+      query: {
+        measures: ["Orders.count"],
+        timeDimensions: [
+          {
+            dimension: "Orders.createdAt",
+            granularity: "month",
+            dateRange: "last year"
+          }
+        ],
+        dimensions: ["ProductCategories.name"]
+      },
+      chartType: "area"
+    }
+  },
+  {
+    id: 3,
+    name: "Orders by Price Range",
+    vizState: {
+      query: {
+        measures: ["Orders.count"],
+        filters: [
+          {
+            "member": "Orders.price",
+            "operator": "set"
+          }
+        ],
+        dimensions: ["Orders.priceRange"]
+      },
+      chartType: "pie"
+    }
+  }
+];
```

As you can see above, we’ve just added an array of Cube.js query objects.

If you refresh the dashboard, you should be able to see your charts!

![](/images/4-screenshot-3.png)

You can notice that one of our queries has the `pivotConfig` defined as the following.

```javascript
  pivotConfig: {
    x: ["Users.city"],
    y: ["Orders.status", "measures"]
  }
```

As I mentioned in the previous chapter the default value for the `pivotConfig` usually works fine, but in some cases like this one, we need to adjust it to get the desired result. We want to plot a bar chart here with the cities on the X-Axis and the number of orders on the Y-Axis grouped by the orders' statuses. That is exactly what we are passing here in the `pivotConfig`: `Users.city` to the X-Axis and measures with `Orders.status` to the Y-axis to get the grouped result.

To customize the rendering of the charts, you can edit the `dashboard-app/src/pages/ChartRenderer.js` file. It should look familiar to what we saw in the previous chapter.

In the next, final, chapter I'll show you how to add a filter to the dashboard
and make it more interactive and dynamic.
