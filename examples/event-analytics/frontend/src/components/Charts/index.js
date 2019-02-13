import React from 'react';
import PropTypes from 'prop-types';

import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import CardContent from '@material-ui/core/CardContent';
import CircularProgress from '@material-ui/core/CircularProgress';
import { withStyles } from '@material-ui/core/styles';

import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';

import LineChart from './Line';
import TableChart from './Table';
import PieChart from './Pie';
import BarChart from './Bar';
import { DASHBOARD_CHART_MIN_HEIGHT } from './helpers.js';

const cubejsClient = cubejs(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1NDk5ODg3NjgsImV4cCI6MTU1MDA3NTE2OH0.fGNQw9t_jFwdcRLyA-ITJaqdMG2TsNrN8HxHqmRcwEA',
  { apiUrl: "http://localhost:4000/cubejs-api/v1" }
);

const styles = ({
  chartContainer: {
    minHeight: DASHBOARD_CHART_MIN_HEIGHT
  }
});

const supportedTypes = {
  line: LineChart,
  table: TableChart,
  pie: PieChart,
  bar: BarChart
}

const rendetChart = (type, props) => {
  const Component = supportedTypes[type];
  return <Component {...props} />;
}

const Chart = ({ title, query, type, classes }) => (
  <Card>
    <CardHeader title={title} />
    <CardContent>
      <div className={classes.chartContainer}>
        <QueryRenderer
          cubejsApi={cubejsClient}
          query={query}
          render={({ resultSet }) => {
            if (resultSet) {
              return rendetChart(type, { resultSet });
            }

            return <CircularProgress />
          }}
        />
      </div>
    </CardContent>
  </Card>
);

Chart.propTypes = {
  title: PropTypes.string,
  type: PropTypes.oneOf(Object.keys(supportedTypes)).isRequired
}

Chart.defaultprops = {
  title: null
}

export default withStyles(styles)(Chart);
