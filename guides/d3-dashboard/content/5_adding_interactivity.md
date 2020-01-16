---
order: 5
title: "Adding Interactivity"
---

In this chapter we'll add a filter to our dashboard to make it more interactive.
The filter will allow users to look at specific sets of orders based on their
status: processing, completed or shipped.

Cube.js makes it easy to add such
dynamic features, because we don't need to add anything to our data schema. We
already have a dimension `Orders.status` and we can just filter by this
dimension by adding filters properties to our JSON query.

Say we have a following query, which is used to plot an area chart with number
of orders over time grouped by the product category.

```javascript
{
  measures: ["Orders.count"],
  timeDimensions: [
    {
      dimension: "Orders.createdAt",
      granularity: "month",
      dateRange: "last year"
    }
  ],
  dimensions: ["ProductCategories.name"]
}
```

To load only completed orders with this query we need to add a filters property
to it.

```javascript
{
  measures: ["Orders.count"],
  timeDimensions: [
    {
      dimension: "Orders.createdAt",
      granularity: "month",
      dateRange: "last year"
    }
  ],
  filters: [
    {
      member: "Orders.status",
      operator: "equals",
      values: ["completed"]
    }
  ],
  dimensions: ["ProductCategories.name"]
}
```

You can learn about all the filters operators in the [query format
docs](https://cube.dev/docs/query-format#filters-operators).

So all we need to do to make the filter work is to conditionally add this
filters property to all our dashboard queries. To do this let's introduce the
`dashboardItemsWithFilter` method in `dashboard-app/src/pages/DashboardPage.js`.
In this method we check if the filter value s any other rather than "all" we inject the filters
property with the corresponding filter value to all the queries.

```javascript
const dashboardItemsWithFilter = (dashboardItems, statusFilter) => {
  if (statusFilter === "all") {
    return dashboardItems;
  }

  const statusFilterObj = {
    member: "Orders.status",
    operator: "equals",
    values: [statusFilter]
  };

  return dashboardItems.map(({ vizState, ...dashboardItem }) => (
    {
      ...dashboardItem,
      vizState: {
        ...vizState,
        query: {
          ...vizState.query,
          filters: (vizState.query.filters || []).concat(statusFilterObj),
        },
      }
    }
  ))
};
```

Now, we need to render the user input for the filter. We can use `<ButtonGroup />` component from Material UI kit for this and render button per the possible state of the order plus the "All" button. We'll use React `useState` hook to store and update the filter value.


First make sure to import `useState` and required components from Material UI.

```diff
-import React from "react";
+import React, { useState } from "react";
+import Button from '@material-ui/core/Button';
+import ButtonGroup from '@material-ui/core/ButtonGroup';
```

Next, we render the buttons group and changing the value of the `statusFilter`
on button's click. Note, that we use newly created `dashboardItemsWithFilter` method to
iterate over dashboard items for rendering.

```diff
-  return DashboardItems.length ? (
-    <Dashboard>{DashboardItems.map(dashboardItem)}</Dashboard>
-  ) : (
+  const [statusFilter, setStatusFilter] = useState("all");
+  return DashboardItems.length ? ([
+    <ButtonGroup style={{ padding: "24px 24px 0 24px" }} color="primary">
+      {["all", "processing", "completed", "shipped"].map(value => (
+        <Button
+          variant={value === statusFilter ? "contained" : ""}
+          onClick={() => setStatusFilter(value)}>
+          {value.toUpperCase()}
+        </Button>
+      ))}
+    </ButtonGroup>,
+    <Dashboard>
+      {dashboardItemsWithFilter(DashboardItems, statusFilter).map(dashboardItem)}
+    </Dashboard>
+  ]) : (
```

That is all we need to create a simple filter and make our D3 dashboard dynamic
and interactive.

![](/images/5-screenshot-1.png)

Congratulations on completing this guide! 🎉

You can check [the online demo of this dashboard here](http://d3-dashboard-demo.cube.dev/) and [the complete source code of the example app is available on Github](https://github.com/cube-js/cube.js/tree/master/examples/d3-dashboard).

I’d love to hear from you about your experience following this guide. Please send any comments or feedback you might have in this [Slack Community](http://slack.cube.dev/). Thank you and I hope you found this guide helpful!
