---
order: 5
title: "First Chart"
---

We can generate a query in Cube.js playground. For example, go to [http://localhost:4000/](http://localhost:4000/), select the Build tab, and choose Orders Count measure, then choose Orders Status dimension, date range this weak, and chart type - Bar. Then we can copy this query to use in our component.

![generate a query in Cube.js playground](/images/generating_query.gif)

Let's create a `<BarChart/>` component. In this component, we use ChartRenderer component.

We are going to the `dateRange` in the component's state to make it dynamic in the next part.

Create the `src/components/BarChart.js` file with the following content.

```jsx
import React from "react";
import clsx from "clsx";
import PropTypes from "prop-types";
import { makeStyles } from '@material-ui/styles';
import ChartRenderer from './ChartRenderer'
import {
  Card,
  CardContent,
  Divider,
} from "@material-ui/core";

const useStyles = makeStyles(() => ({
  root: {},
  chartContainer: {
    position: "relative",
    padding: "19px 0"
  }
}));

const BarChart = props => {
  const { className, ...rest } = props;
  const classes = useStyles();

  const [dateRange, setDateRange] = React.useState('This week');

  const query = {
    "measures": [
      "Orders.count"
    ],
    "timeDimensions": [
      {
        "dimension": "Orders.createdAt",
        "granularity": "day",
        "dateRange": `${dateRange}`
      }
    ],
    "dimensions": [
      "Orders.status"
    ],
    "filters": [
      {
        "dimension": "Orders.status",
        "operator": "notEquals",
        "values": [
          "completed"
        ]
      }
    ]
  };
  return (
    <Card {...rest} className={clsx(classes.root, className)}>
      <CardContent>
        <div className={classes.chartContainer}>
          <ChartRenderer vizState={{ query, chartType: 'bar' }}/>
        </div>
      </CardContent>
    </Card>
  )
};

BarChart.propTypes = {
  className: PropTypes.string
};

export default BarChart;
```

Now let's add custom options for `<ChartRenderer/>` component. 

Create the `helpers` folder inside the `dashboard-app/src`. Inside the `helpers` folder create `BarOptions.js` file with the following content.

```jsx
import palette from '../theme/palette';
export const BarOptions = {
  responsive: true,
  legend: { display: false },
  cornerRadius: 50,
  tooltips: {
    enabled: true,
    mode: 'index',
    intersect: false,
    borderWidth: 1,
    borderColor: palette.divider,
    backgroundColor: palette.white,
    titleFontColor: palette.text.primary,
    bodyFontColor: palette.text.secondary,
    footerFontColor: palette.text.secondary,
  },
  layout: { padding: 0 },
  scales: {
    xAxes: [
      {
        barThickness: 12,
        maxBarThickness: 10,
        barPercentage: 0.5,
        categoryPercentage: 0.5,
        ticks: {
          fontColor: palette.text.secondary,
        },
        gridLines: {
          display: false,
          drawBorder: false,
        },
      },
    ],
    yAxes: [
      {
        ticks: {
          fontColor: palette.text.secondary,
          beginAtZero: true,
          min: 0,
        },
        gridLines: {
          borderDash: [2],
          borderDashOffset: [2],
          color: palette.divider,
          drawBorder: false,
          zeroLineBorderDash: [2],
          zeroLineBorderDashOffset: [2],
          zeroLineColor: palette.divider,
        },
      },
    ],
  },
};
```

Now let's add this options for our chart in `<ChartRenderer/>` component.

`components/ChartRenderer.js`

```diff
// ...

import TableHead from '@material-ui/core/TableHead';
import TableRow from '@material-ui/core/TableRow';
+ import palette from '../theme/palette'
+ import moment from 'moment';
+ import { BarOptions } from '../helpers/BarOptions.js';
- const COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];
+ const COLORS_SERIES = [palette.secondary.main, palette.primary.light, palette.secondary.light];
// ...
	bar:
 ({ resultSet }) => {
    const data = {
-      labels: resultSet.categories().map((c) => c.category),
+      labels: resultSet.categories().map((c) => moment(c.category).format('DD/MM/YYYY')),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: COLORS_SERIES[index],
        fill: false,
      })),
    };
-    return <Bar data={data} options={BarOptions} />;
+    return <Bar data={data} options={BarOptions} />;
  },
//...
```

Now we can edit `<DashboardPage/>`

```jsx
import React from 'react';
import {Grid} from '@material-ui/core';
import {makeStyles} from '@material-ui/styles';

import BarChart from '../components/BarChart.js'

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
}));

const Dashboard = () => {
  const classes = useStyles();
  return (
    <div className={classes.root}>
      <Grid
        container
        spacing={4}
      >
        <Grid
          item
          lg={8}
          md={12}
          xl={9}
          xs={12}
        >
          <BarChart/>
        </Grid>
      </Grid>
    </div>
  );
};

export default Dashboard;

```

That's all we need to display our first chart! 🎉

![display our first chart](/images/first_chart.png)

In the next part we'll make this chart interactive and let users to change the date range.
