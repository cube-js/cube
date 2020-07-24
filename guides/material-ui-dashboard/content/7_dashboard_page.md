---
order: 7
title: "Dashboard Page"
---

A dashboard is a type of graphical user interface that often provides at-a-glance views of key performance indicators relevant to a particular objective or business process. In other usages, "dashboard" is another name for "progress report" or "report."

### KPI Charts

The KPI chart is used to, at a quick glance, give information about the current performance of a company or organization. Factors, which are crucial for monitoring how the company performs, are measured and then presented in form of KPIs, Key Performance Indicators. The type of information that is shown varies. Examples of KPIs are information about net revenue, sales growth, and customer satisfaction. The KPI chart consists of a grid of tiles, where each tile displays various KPI values for a certain category.

Let's add other components. To do count up components we need to install the react-countup dependency.

```jsx
// npm
npm install react-countup
// yarn
yarn add react-countup
```

Then let's add new component `<KPIChart/>`

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

Default schema it suits us. But for one KPI chart, we need to create custom measure `percentOfCompletedOrders`. This measure will count percent based on another measure `completedCount`.

Let's do it and customize Orders schema. Open `schema/Orders.js` file. In this file, we need to add: 

- [measure](https://cube.dev/docs/measures) completedCount
- [measure](https://cube.dev/docs/measures/) percentOfCompletedOrders

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
});
```

We create all simple card components and prepare schema. Let's add this cards to our page.

`<DashboardPage/>`

```diff
// ...
+ import KPIChart from '../components/KPIChart';
import BarChart from '../components/BarChart.js'
// ...
+ const cards = [
+   {
+     title: 'ORDERS',
+     query: { measures: ['Orders.count'] },
+     difference: 'Orders',
+     duration: 1.25,
+   },
+   {
+     title: 'TOTAL USERS',
+     query: { measures: ['Users.count'] },
+     difference: 'Users',
+     duration: 1.5,
+   },
+   {
+     title: 'COMPLETED ORDERS',
+     query: { measures: ['Orders.percentOfCompletedOrders'] },
+     progress: true,
+     duration: 1.75,
+   },
+   {
+     title: 'TOTAL PROFIT',
+     query: { measures: ['LineItems.price'] },
+     duration: 2.25,
+  } ,
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

![Example gif](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/045224cb-9d96-4c80-a416-bf03f7fd4953/Screenshot_2020-07-01_at_19.08.18.png)

### Doughnut Chart

Final chart and when to use Doughnut

Next chart is `<DoughnutChart/>`

We need to create options in `helpers/DoughnutOptions.js` path. A dashboard is a type of graphical user interface that often provides at-a-glance views of key performance indicators relevant to a particular objective or business process. A dashboard is a type of graphical user interface which often provides at-a-glance views of key performance indicators relevant to a particular objective or business process

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

And `<DoughnutChart/>`

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

const query = {
  measures: ['Orders.count'],
  timeDimensions: [
    {
      dimension: 'Orders.createdAt',
    },
  ],
  filters: [],
  dimensions: ['Orders.status'],
};

const DoughnutChart = (props) => {
  const { className, cubejsApi, ...rest } = props;

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

Now we can do our `<DashboardPage/>`

```diff
// ...
import DataCard from '../components/DataCard';
import BarChart from '../components/BarChart.js'
+ import DoughnutChart from '../components/DoughnutChart.js'
// ...

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
+          <DoughnutChart/>
+        </Grid>
      </Grid>
    </div>
  );
```

Well done! Next step is ещ edit layout and finish step will Data table page
