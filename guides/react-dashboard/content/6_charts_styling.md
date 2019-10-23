---
title: "Charts Styling"
order: 6
---

When we created a dashboard app in the first chapter, we selected Recharts as our visualization library. Recharts provides a set of charting components, which you can mix together to build different kinds of charts. It is also quite powerful when it comes to customization.

Every component in the Recharts library accepts multiple properties that control its look and feel. You can learn more about the Recharts components API [here](http://recharts.org/en-US/api). We are going to use these properties and a little CSS to customize charts according to our design.

The first step is to provide correct and nice formatting for numbers and dates. We’re going to accomplish that with the help of two libraries: `moment`—for date formatting, and `numeral`—for number formatting. Let’s install them.

```bash
$ yarn add numeral moment
```

Next, we’re adding some CSS to customize the SVG elements of the charts. Create a `dashboard-app/src/components/recharts-theme.less` file with the following content.

```less
.recharts-cartesian-grid-horizontal {
  line {
    stroke-dasharray: 2, 2;
    stroke: #D0D0DA;
  }
}

.recharts-cartesian-axis-tick-value {
  tspan {
    fill: #A1A1B5;
    letter-spacing: 0.03em;
    //font-weight: bold;
    font-size: 14px;
  }
}
```

Finally, let’s import our CSS, define formatters, and pass customization properties to the charts’ components. Make the following changes in the `src/components/ChartRenderer.js` file.

```diff
+import "./recharts-theme.less";
+import moment from "moment";
+import numeral from "numeral";
+
+const numberFormatter = item => numeral(item).format("0,0");
+const dateFormatter = item => moment(item).format("MMM YY");
+const colors = ["#7DB3FF", "#49457B", "#FF7C78"];
+const xAxisFormatter = (item) => {
+  if (moment(item).isValid()) {
+    return dateFormatter(item)
+  } else {
+    return item;
+  }
+}
+
+const CartesianChart = ({ resultSet, children, ChartComponent }) => (
+  <ResponsiveContainer width="100%" height={350}>
+    <ChartComponent margin={{ left: -10 }} data={resultSet.chartPivot()}>
+      <XAxis axisLine={false} tickLine={false} tickFormatter={xAxisFormatter} dataKey="x" minTickGap={20} />
+      <YAxis axisLine={false} tickLine={false} tickFormatter={numberFormatter} />
+      <CartesianGrid vertical={false} />
+      { children }
+      <Legend />
+      <Tooltip labelFormatter={dateFormatter} formatter={numberFormatter} />
+    </ChartComponent>
+  </ResponsiveContainer>
+)
-const CartesianChart = ({ resultSet, children, ChartComponent }) => (
-  <ResponsiveContainer width="100%" height={400}>
-    <ChartComponent data={resultSet.chartPivot()}>
-      <XAxis dataKey="x" />
-      <YAxis />
-      <CartesianGrid />
-      {children}
-      <Legend />
-      <Tooltip />
-    </ChartComponent>
-  </ResponsiveContainer>
-);
-
-const colors = ["#FF6492", "#141446", "#7A77FF"];
```

That is all on charts customization. Depending on the library and how much you want to customize your charts’ look and feel, you can end with less or more changes, but for our design, we are good with the above changes. Head out to http://localhost:3000 to check out your new charts’ styles.

Finally, we're done with customization and are ready to deploy our dashboard.
That is what we are going to cover in our next part.
