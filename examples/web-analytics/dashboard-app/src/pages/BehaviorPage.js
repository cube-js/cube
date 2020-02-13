import React from "react";
import Grid from "@material-ui/core/Grid";
import ListSubheader from '@material-ui/core/ListSubheader';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import OverTimeChart from "../components/OverTimeChart";
import Chart from "../components/Chart";

const queries = {
  pageviewsOverTime: {
    chartType: 'line',
    legend: false,
    query: {
      measures: ['PageViews.count'],
      timeDimensions: [{
        dimension: 'PageViews.time',
        granularity: 'day'
      }]
    }
  },

  pageviews: {
    chartType: 'number',
    query: {
      measures: ['PageViews.pageviews'],
      timeDimensions: [{
        dimension: 'PageViews.time'
      }]
    }
  },

  uniqPageviews: {
    chartType: 'number',
    query: {
      measures: ['PageUsers.uniqPageviews'],
      timeDimensions: [{
        dimension: 'PageUsers.time'
      }]
    }
  },

  averageTimeOnPageSeconds: {
    chartType: 'number',
    query: {
      measures: ['PageViews.averageTimeOnPageSeconds'],
      timeDimensions: [{
        dimension: 'PageViews.time'
      }]
    }
  },

  bounceRate: {
    chartType: 'number',
    query: {
      measures: ['Sessions.bounceRate']
    }
  },

  exitPercent: {
    chartType: 'number',
    query: {
      measures: ['PageViews.exitPercent'],
      timeDimensions: [{
        dimension: 'PageViews.time'
      }]
    }
  },

  pageviewsTable: {
    chartType: 'table',
    query: {
      measures: ['PageViews.pageviews'],
      dimensions: ['PageViews.pageUrlPath'],
      timeDimensions: [{
        dimension: 'PageViews.time'
      }]
    },
    order: { 'PageViews.pageviews': 'desc' }
  }
}

const BehaviorPage = ({ withTime }) => (
  <Grid item xs={12}>
    <Grid container spacing={3}>
      <Grid item xs={12}>
        <OverTimeChart
          title="Pageviews"
          vizState={withTime(queries.pageviewsOverTime)}
        />
      </Grid>
      <Grid item xs={2}>
        <Chart title="Pageviews" vizState={withTime(queries.pageviews)} />
      </Grid>
      <Grid item xs={2}>
        <Chart title="Unique Pageviews" vizState={withTime(queries.uniqPageviews)} />
      </Grid>
      <Grid item xs={2}>
        <Chart title="Avg. Time on Page" vizState={withTime(queries.averageTimeOnPageSeconds)} />
      </Grid>
      <Grid item xs={2}>
        <Chart title="Bounce Rate" vizState={withTime(queries.bounceRate)} />
      </Grid>
      <Grid item xs={2}>
        <Chart title="% Exit" vizState={withTime(queries.exitPercent)} />
      </Grid>
      <Grid item xs={3}>
        <List
      component="nav"
      aria-labelledby="nested-list-subheader"
      subheader={
        <ListSubheader component="div" id="nested-list-subheader">
          Site Content
        </ListSubheader>
      }
    >
      <ListItem selected button>
        <ListItemText primary="Page" />
      </ListItem>
      <ListItem button>
        <ListItemText primary="Page Title" />
      </ListItem>
    </List>
      </Grid>
      <Grid item xs={9}>
        <Chart vizState={withTime(queries.pageviewsTable)} />
      </Grid>
    </Grid>
  </Grid>
);

export default BehaviorPage;
