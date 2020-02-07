import React from "react";
import Grid from "@material-ui/core/Grid";
import OverTimeChart from "../components/OverTimeChart";

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
  }
}

const BehaviorPage = ({ withTime }) => (
  <Grid item xs={12}>
    <OverTimeChart
      title="Pageviews"
      vizState={withTime(queries.pageviewsOverTime)}
    />
  </Grid>
);

export default BehaviorPage;
