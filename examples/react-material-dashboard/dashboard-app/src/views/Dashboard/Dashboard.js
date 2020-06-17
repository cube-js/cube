import React from 'react';
import { makeStyles } from '@material-ui/styles';
import { Grid } from '@material-ui/core';

import {
  Budget,
  TotalUsers,
  TasksProgress,
  TotalProfit,
  LatestSales,
  OrdersStatus,
  LatestProducts,
  LatestOrders
} from './components';
import cubejs from "@cubejs-client/core";

const useStyles = makeStyles(theme => ({
  root: {
    padding: theme.spacing(4)
  },
}));

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});

const Dashboard = () => {
  const classes = useStyles();

  return (
    <div className={classes.root}>
      <Grid
        container
        spacing={4}
      >
        <Grid
          className={classes.gridItem}
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
        >
          <Budget cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
        >
          <TotalUsers cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
        >
          <TasksProgress cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={3}
          sm={6}
          xl={3}
          xs={12}
        >
          <TotalProfit cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={8}
          md={12}
          xl={9}
          xs={12}
        >
          <LatestSales cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={4}
          md={6}
          xl={3}
          xs={12}
        >
          <OrdersStatus cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={4}
          md={6}
          xl={3}
          xs={12}
        >
          <LatestProducts cubejsApi={cubejsApi}/>
        </Grid>
        <Grid
          className={classes.gridItem}
          item
          lg={8}
          md={12}
          xl={9}
          xs={12}
        >
          <LatestOrders cubejsApi={cubejsApi}/>
        </Grid>
      </Grid>
    </div>
  );
};

export default Dashboard;
