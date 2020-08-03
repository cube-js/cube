import React from 'react';
import { Grid } from '@material-ui/core';
import { makeStyles } from '@material-ui/styles';

import KPIChart from '../components/KPIChart';
import BarChart from '../components/BarChart.js';
import DoughnutChart from '../components/DoughnutChart.js';

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(4),
  },
}));

const cards = [
  {
    title: 'ORDERS',
    query: { measures: ['Orders.count'] },
    difference: 'Orders',
    duration: 1.25,
  },
  {
    title: 'TOTAL USERS',
    query: { measures: ['Users.count'] },
    difference: 'Users',
    duration: 1.5,
  },
  {
    title: 'COMPLETED ORDERS',
    query: { measures: ['Orders.percentOfCompletedOrders'] },
    progress: true,
    duration: 1.75,
  },
  {
    title: 'TOTAL PROFIT',
    query: { measures: ['LineItems.price'] },
    duration: 2.25,
  },
];
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
const doughnutChartQuery = {
  measures: ['Orders.count'],
  timeDimensions: [
    {
      dimension: 'Orders.createdAt',
    },
  ],
  filters: [],
  dimensions: ['Orders.status'],
};

const Dashboard = () => {
  const classes = useStyles();
  return (
    <div className={classes.root}>
      <Grid container spacing={4}>
        {cards.map((item, index) => {
          return (
            <Grid key={item.title + index} item lg={3} sm={6} xl={3} xs={12}>
              <KPIChart {...item} />
            </Grid>
          );
        })}
        <Grid item lg={8} md={12} xl={9} xs={12}>
          <BarChart query={barChartQuery} />
        </Grid>
        <Grid item lg={4} md={6} xl={3} xs={12}>
          <DoughnutChart query={doughnutChartQuery} />
        </Grid>
      </Grid>
    </div>
  );
};

export default Dashboard;
