---
order: 4
title: "Interactive Dashboard with Multiple Charts"
---

In the previous part, we've created an analytical backend and a basic dashboard with the first chart. Now we're going to expand the dashboard so it provides the at-a-glance view of key performance indicators of our e-commerce company.

## Custom Date Range

As the first step, we'll let users change the date range of the existing chart.

We'll use a separate `<BarChartHeader />` component to control the date range. Let's create the `src/components/BarChartHeader.js` file with the following contents:

```jsx
import React from 'react';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import { CardHeader, Button } from '@material-ui/core';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';

const useStyles = makeStyles(() => ({
  headerButton: {
    letterSpacing: '0.4px',
  },
}));

const BarChartHeader = (props) => {
  const { setDateRange, dateRange, dates } = props;
  const defaultDates = ['This week', 'This month', 'Last 7 days', 'Last month'];
  const classes = useStyles();

  const [anchorEl, setAnchorEl] = React.useState(null);
  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };
  const handleClose = (date) => {
    setDateRange(date);
    setAnchorEl(null);
  };
  return (
    <CardHeader
      action={
        <div>
          <Button
            className={classes.headerButton}
            size="small"
            variant="text"
            aria-controls="simple-menu"
            aria-haspopup="true"
            onClick={handleClick}
          >
            {dateRange} <ArrowDropDownIcon />
          </Button>
          <Menu
            id="simple-menu"
            anchorEl={anchorEl}
            keepMounted
            open={Boolean(anchorEl)}
            onClose={() => handleClose(dateRange)}
          >
            {dates ?
              dates.map((date) => (
                <MenuItem key={date} onClick={() => handleClose(date)}>{date}</MenuItem>
              ))
             : defaultDates.map((date) => (
                <MenuItem key={date} onClick={() => handleClose(date)}>{date}</MenuItem>
              ))}
          </Menu>
        </div>
      }
      title="Latest Sales"
    />
  );
};

BarChartHeader.propTypes = {
  className: PropTypes.string,
};

export default BarChartHeader;
```

Now let's add this `<BarChartHeader />` component to our existing chart. Make the following changes in the `src/components/BarChart.js` file:

```diff
// ...
import ChartRenderer from './ChartRenderer'
+ import BarChartHeader from "./BarChartHeader";
// ...
const BarChart = (props) => {
-  const { className, query, ...rest } = props;
+  const { className, query, dates, ...rest } = props;
  const classes = useStyles();

+  const [dateRange, setDateRange] = React.useState(dates ? dates[0] : 'This week');
+  let queryWithDate = {...query,
+    timeDimensions: [
+      {
+        dimension: query.timeDimensions[0].dimension,
+        granularity: query.timeDimensions[0].granularity,
+        dateRange: `${dateRange}`
+      }
+    ],
+  };

  return (
    <Card {...rest} className={clsx(classes.root, className)}>
+      <BarChartHeader dates={dates} dateRange={dateRange} setDateRange={setDateRange} />
+      <Divider />
      <CardContent>
        <div className={classes.chartContainer}>
          <ChartRenderer vizState={{ query: queryWithDate, chartType: 'bar' }}/>
        </div>
      </CardContent>
    </Card>
  )
};
// ...
```

Well done! 🎉 Here's what our dashboard application looks like:

![](/images/image-48.png)

## KPI Chart

The KPI chart can be used to display business indicators that provide information about the current performance of our e-commerce company. The chart will consist of a grid of tiles, where each tile will display a single numeric KPI value for a certain category.

First, let's use the `react-countup` package to add the count-up animation to the values on the KPI chart. Run the following command in the `dashboard-app` folder:

```jsx
npm install --save react-countup
```

New we're ready to add new `<KPIChart/>` component. Add the `src/components/KPIChart.js` component with the following contents:

```jsx
import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import { Card, CardContent, Grid, Typography, LinearProgress } from '@material-ui/core';
import { useCubeQuery } from '@cubejs-client/react';
import CountUp from 'react-countup';
import CircularProgress from '@material-ui/core/CircularProgress';

const useStyles = makeStyles((theme) => ({
  root: {
    height: '100%',
  },
  content: {
    alignItems: 'center',
    display: 'flex',
  },
  title: {
    fontWeight: 500,
  },
  progress: {
    marginTop: theme.spacing(3),
    height: '8px',
    borderRadius: '10px',
  },
  difference: {
    marginTop: theme.spacing(2),
    display: 'flex',
    alignItems: 'center',
  },
  differenceIcon: {
    color: theme.palette.error.dark,
  },
  differenceValue: {
    marginRight: theme.spacing(1),
  },
  green: {
    color: theme.palette.success.dark,
  },
  red: {
    color: theme.palette.error.dark,
  },
}));

const KPIChart = (props) => {
  const classes = useStyles();
  const { className, title, progress, query, difference, duration, ...rest } = props;
  const { resultSet, error, isLoading } = useCubeQuery(query);
  const differenceQuery = {...query,
    "timeDimensions": [
      {
        "dimension": `${difference || query.measures[0].split('.')[0]}.createdAt`,
        "granularity": null,
        "dateRange": "This year"
      }
    ]};
  const differenceValue = useCubeQuery(differenceQuery);

  if (isLoading || differenceValue.isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress color="secondary" />
      </div>
    );
  }
  if (error || differenceValue.error) {
    return <pre>{(error || differenceValue.error).toString()}</pre>;
  }
  if (!resultSet || !differenceValue.resultSet) {
    return null
  }
  if (resultSet && differenceValue.resultSet) {
    let postfix = null;
    let prefix = null;
    const measureKey = resultSet.seriesNames()[0].key;
    const annotations = resultSet.tableColumns().find(tableColumn => tableColumn.key === measureKey);
    const format = annotations.format || (annotations.meta && annotations.meta.format);
    if (format === 'percent') {
      postfix = '%'
    } else if (format === 'currency') {
      prefix = '$'
    }

    let value = null;
    let fullValue = resultSet.seriesNames().map((s) => resultSet.totalRow()[s.key])[0];
    if (difference) {
      value = differenceValue.resultSet.totalRow()[differenceQuery.measures[0]] / fullValue * 100;
    }
    return (
      <Card {...rest} className={clsx(classes.root, className)}>
        <CardContent>
          <Grid container justify="space-between">
            <Grid item>
              <Typography className={classes.title} color="textSecondary" gutterBottom variant="body2">
                {title}
              </Typography>
              <Typography variant="h3">
                {prefix}
                <CountUp
                  end={fullValue}
                  duration={duration}
                  separator=","
                  decimals={0}
                />
                {postfix}
              </Typography>
            </Grid>
          </Grid>
          {progress ? (
            <LinearProgress
              className={classes.progress}
              value={fullValue}
              variant="determinate"
            />
          ) : null}
          {difference ? (
            <div className={classes.difference}>
              <Typography className={classes.differenceValue} variant="body2">
                {value > 1 ? (
                  <span className={classes.green}>{value.toFixed(1)}%</span>
                ) : (
                  <span className={classes.red}>{value.toFixed(1)}%</span>
                )}
              </Typography>
              <Typography className={classes.caption} variant="caption">
                Since this year
              </Typography>
            </div>
          ) : null}
        </CardContent>
      </Card>
    );
  }
};

KPIChart.propTypes = {
  className: PropTypes.string,
  title: PropTypes.string,
};

export default KPIChart;
```

**Let's learn how to create custom measures in the data schema and display their values.** In the e-commerce business, it's crucial to know the share of completed orders. To enable our users to monitor this metric, we'll want to display it on the KPI chart. So, we will modify the data schema by [adding a custom measure](https://cube.dev/docs/measures/) (`percentOfCompletedOrders`) which will calculate the share based on another measure (`completedCount`).

Let's customize the "Orders" schema. Open the `schema/Orders.js` file in the root folder of the Cube.js project and make the following changes: 

- add the `completedCount` measure
- add the `percentOfCompletedOrders` measure

```diff
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  //.. 

  measures: {
    count: {
      type: `count`,
      drillMembers: [id, createdAt]
    },
    number: {
      sql: `number`,
      type: `sum`
    },
+    completedCount: {
+      sql: `id`,
+      type: `count`,
+      filters: [
+        { sql: `${CUBE}.status = 'completed'` }
+      ]
+    },
+    percentOfCompletedOrders: {
+      sql: `${completedCount}*100.0/${count}`,
+      type: `number`,
+      format: `percent`
+    }
  },

  // ...
```

Now we're ready to add the KPI chart displaying a number of KPIs to the dashboard. Make the following changes to the `src/pages/DashboardPage.js` file:

```diff
// ...
+ import KPIChart from '../components/KPIChart';
import BarChart from '../components/BarChart.js'
// ...
+ const cards = [
+  {
+    title: 'ORDERS',
+    query: { measures: ['Orders.count'] },
+    difference: 'Orders',
+    duration: 1.25,
+  },
+  {
+    title: 'TOTAL USERS',
+    query: { measures: ['Users.count'] },
+    difference: 'Users',
+    duration: 1.5,
+  },
+  {
+    title: 'COMPLETED ORDERS',
+    query: { measures: ['Orders.percentOfCompletedOrders'] },
+    progress: true,
+    duration: 1.75,
+  },
+  {
+    title: 'TOTAL PROFIT',
+    query: { measures: ['LineItems.price'] },
+    duration: 2.25,
+  },
+ ];

const Dashboard = () => {
  const classes = useStyles();
  return (
    <div className={classes.root}>
      <Grid
        container
        spacing={4}
      >
+        {cards.map((item, index) => {
+         return (
+           <Grid
+             key={item.title + index}
+             item
+             lg={3}
+             sm={6}
+             xl={3}
+             xs={12}
+           >
+             <KPIChart {...item}/>
+           </Grid>
+         )
+       })}
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
```

Great! 🎉 Now our dashboard has a row of nice and informative KPI metrics:

![](/images/image-18.png)

## Doughnut Chart

Now, using the KPI chart, our users are able to monitor the share of completed orders. However, there are two more kinds of orders: "processed" orders (ones that were acknowledged but not yet shipped) and "shipped" orders (essentially, ones that were taken for delivery but not yet completed).

To enable our users to monitor all these kinds of orders, we'll want to add one final chart to our dashboard. It's best to use the Doughnut chart for that, because it's quite useful to visualize the distribution of a certain metric between several states (e.g., all kinds of orders).

First, just like in the previous part, we're going to put the chart options to a separate file. Let's create the `src/helpers/DoughnutOptions.js` file with the following contents:

```jsx
import palette from "../theme/palette";
export const DoughnutOptions = {
  legend: {
    display: false
  },
  responsive: true,
  maintainAspectRatio: false,
  cutoutPercentage: 80,
  layout: { padding: 0 },
  tooltips: {
    enabled: true,
    mode: "index",
    intersect: false,
    borderWidth: 1,
    borderColor: palette.divider,
    backgroundColor: palette.white,
    titleFontColor: palette.text.primary,
    bodyFontColor: palette.text.secondary,
    footerFontColor: palette.text.secondary
  }
};
```

Then, let's create the `src/components/DoughnutChart.js` for the new chart with the following contents:

```jsx
import React from 'react';
import { Doughnut } from 'react-chartjs-2';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles, useTheme } from '@material-ui/styles';
import { Card, CardHeader, CardContent, Divider, Typography } from '@material-ui/core';
import { useCubeQuery } from '@cubejs-client/react';
import CircularProgress from '@material-ui/core/CircularProgress';
import { DoughnutOptions } from '../helpers/DoughnutOptions.js';

const useStyles = makeStyles((theme) => ({
  root: {
    height: '100%',
  },
  chartContainer: {
    marginTop: theme.spacing(3),
    position: 'relative',
    height: '300px',
  },
  stats: {
    marginTop: theme.spacing(2),
    display: 'flex',
    justifyContent: 'center',
  },
  status: {
    textAlign: 'center',
    padding: theme.spacing(1),
  },
  title: {
    color: theme.palette.text.secondary,
    paddingBottom: theme.spacing(1),
  },
  statusIcon: {
    color: theme.palette.icon,
  },
}));

const DoughnutChart = (props) => {
  const { className, query, ...rest } = props;

  const classes = useStyles();
  const theme = useTheme();

  const { resultSet, error, isLoading } = useCubeQuery(query);
  if (isLoading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <CircularProgress color="secondary" />
      </div>
    );
  }
  if (error) {
    return <pre>{error.toString()}</pre>;
  }
  if (!resultSet) {
    return null
  }
  if (resultSet) {
    const COLORS_SERIES = [
      theme.palette.secondary.light,
      theme.palette.secondary.lighten,
      theme.palette.secondary.main,
    ];
    const data = {
      labels: resultSet.categories().map((c) => c.category),
      datasets: resultSet.series().map((s) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: COLORS_SERIES,
        hoverBackgroundColor: COLORS_SERIES,
      })),
    };
    const reducer = (accumulator, currentValue) => accumulator + currentValue;
    return (
      <Card {...rest} className={clsx(classes.root, className)}>
        <CardHeader title="Orders status" />
        <Divider />
        <CardContent>
          <div className={classes.chartContainer}>
            <Doughnut data={data} options={DoughnutOptions} />
          </div>
          <div className={classes.stats}>
            {resultSet.series()[0].series.map((status) => (
              <div className={classes.status} key={status.category}>
                <Typography variant="body1" className={classes.title}>
                  {status.category}
                </Typography>
                <Typography variant="h2">{((status.value/resultSet.series()[0].series.map(el => el.value).reduce(reducer)) * 100).toFixed(0)}%</Typography>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }
};

DoughnutChart.propTypes = {
  className: PropTypes.string,
};

export default DoughnutChart;
```

The last step is to add the new chart to the dashboard. Let's modify the `src/pages/DashboardPage.js` file:

```diff
// ...
import DataCard from '../components/DataCard';
import BarChart from '../components/BarChart.js'
+ import DoughnutChart from '../components/DoughnutChart.js'

// ...
+ const doughnutChartQuery = {
+  measures: ['Orders.count'],
+  timeDimensions: [
+    {
+      dimension: 'Orders.createdAt',
+    },
+  ],
+  filters: [],
+  dimensions: ['Orders.status'],
+ };
//...

return (
    <div className={classes.root}>
      <Grid
        container
        spacing={4}
      >
        // ..
+        <Grid
+          item
+          lg={4}
+          md={6}
+          xl={3}
+          xs={12}
+        >
+          <DoughnutChart query={doughnutChartQuery}/>
+        </Grid>
      </Grid>
    </div>
  );
```

Awesome! 🎉 Now the first page of our dashboard is complete:

![](/images/image-53.png)

If you like the layout of our dashboard, check out the [Devias Kit Admin Dashboard](https://github.com/devias-io/react-material-dashboard), an open source React Dashboard made with Material UI's components. 