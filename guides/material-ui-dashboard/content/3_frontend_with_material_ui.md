---
order: 3
title: "Frontend with Material UI"
---

Creating a complex dashboard from scratch usually takes time and effort.

**The Cube.js Playground can generate a template for any chosen frontend framework and charting library for you.** To create a template for our dashboard, navigate to the "Dashboard App" and use these options:

- Framework: React
- Main Template: React Material UI Static
- Charting Library: Chart.js

![](/images/image-0.gif)

Congratulations! Now we have the `dashboard-app` folder in our project. This folder contains all the frontend code of our analytical dashboard.

**Now it's time to add the Material UI framework.** To have a nice-looking dashboard, we're going to use a custom Material UI theme. You can learn about creating your custom Material UI themes from the [documentation](https://material-ui.com/customization/theming/). For now, let's download a pre-configured theme from GitHub:

```jsx
curl -LJO https://github.com/cube-js/cube.js/tree/master/examples/material-ui-dashboard/dashboard-app/src/theme/theme.zip
```

Then, let's install the Roboto font which works best with Material UI:

```bash
npm install typeface-roboto
```

Now we can include the theme and the font to our frontend code. Let's use the `ThemeProvider` from Material UI and make the following changes in the `App.js` file:

```diff
// ...
- import { makeStyles } from "@material-ui/core/styles";
+ import { makeStyles, ThemeProvider } from "@material-ui/core/styles";
+ import theme from './theme';
+ import 'typeface-roboto'
+ import palette from "./theme/palette";
// ...

const useStyles = makeStyles((theme) => ({
  root: {
    flexGrow: 1,
+    margin: '-8px',
+    backgroundColor: palette.primary.light,
  },
}));

const AppLayout = ({children}) => {
  const classes = useStyles();
  return (
+   <ThemeProvider theme={theme}>
      <div className={classes.root}>
        <Header/>
        <div>{children}</div>
      </div>
+   </ThemeProvider>
  );
};
// ...
```

**The only thing left to connect the frontend and the backend is a Cube.js query.** We can generate a query in the Cube.js Playground. Go to [http://localhost:4000/](http://localhost:4000/), navigate to the "Build" tab, and choose the following query parameters:

- Measure: Orders Count
- Dimension: Orders Status
- Data range: This week
- Chart type: Bar

![](/images/image-201.gif)

We can copy the Cube.js query for the shown chart and use it in our dashboard application.

To do so, let's create a generic `<BarChart />` component which, in turn, will use a `ChartRenderer` component. Create the `src/components/BarChart.js` file with the following contents:

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
  const { className, query, ...rest } = props;
  const classes = useStyles();

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

We'll need some custom options for the `<ChartRenderer />` component. These options will make the bar chart look nice.

Create the `helpers` folder inside the `dashboard-app/src`. Inside the `helpers` folder, create the `BarOptions.js` file with the following contents:

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

Let's edit the `src/components/ChartRenderer.js` file to pass the options to the `<Bar />` component:

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
    return <Bar data={data} options={BarOptions} />;
  },
//...
```

Now the final step! Let's add the bar chart to the dashboard. Edit the `src/pages/DashboardPage.js` and use the following contents:

```jsx
import React from 'react';
import { Grid } from '@material-ui/core';
import { makeStyles } from '@material-ui/styles';

import BarChart from '../components/BarChart.js'

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
}));

const barChartQuery = {
  measures: ['Orders.count'],
  timeDimensions: [
    {
      dimension: 'Orders.createdAt',
      granularity: 'day',
      dateRange: 'This week',
    },
  ],
  dimensions: ['Orders.status'],
  filters: [
      {
        dimension: 'Orders.status',
        operator: 'notEquals',
        values: ['completed'],
      },
    ],
};

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
          <BarChart query={barChartQuery}/>
        </Grid>
      </Grid>
    </div>
  );
};

export default Dashboard;

```

That's all we need to display our first chart! 🎉

![](/images/image-51.png)

In the next part, we'll make this chart interactive by letting users change the date range from "This week" to other predefined values.