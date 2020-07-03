import React from 'react';
import {Grid} from '@material-ui/core';
import {makeStyles} from '@material-ui/styles';

import KPIChart from '../components/KPIChart';
import BarChart from '../components/BarChart.js'
import DoughnutChart from '../components/DoughnutChart.js'

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
}));

const cards = [
  {title: 'ORDERS', query: {measures: ["Orders.count"]}, difference: 'Since last month', value: -12, duration: 1.25},
  {title: 'TOTAL USERS', query: {measures: ["Users.count"]}, difference: 'Since last month', value: 16, duration: 1.5},
  {title: 'COMPLETED ORDERS', query: { measures: ["Orders.percentOfCompletedOrders"] }, progress: true, postfix: '%', duration: 1.75},
  {title: 'TOTAL PROFIT', query: {"measures": ["LineItems.price"], "timeDimensions": [], "filters": []}, prefix: '$', duration: 2.25},
];

const Dashboard = () => {
  const classes = useStyles();
  return (
    <div className={classes.root}>
      <Grid
        container
        spacing={4}
      >
        {cards.map((item, index) => {
          return (
            <Grid
              key={item.title + index}
              item
              lg={3}
              sm={6}
              xl={3}
              xs={12}
            >
              <KPIChart {...item}/>
            </Grid>
          )
        })}
        <Grid
          item
          lg={8}
          md={12}
          xl={9}
          xs={12}
        >
          <BarChart/>
        </Grid>
        <Grid
          item
          lg={4}
          md={6}
          xl={3}
          xs={12}
        >
          <DoughnutChart/>
        </Grid>
      </Grid>
    </div>
  );
};

export default Dashboard;
