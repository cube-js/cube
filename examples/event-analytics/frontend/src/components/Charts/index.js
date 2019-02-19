import React from 'react';
import PropTypes from 'prop-types';

import Card from '@material-ui/core/Card';
import CardHeader from '@material-ui/core/CardHeader';
import CardContent from '@material-ui/core/CardContent';
import CircularProgress from '@material-ui/core/CircularProgress';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';
import ErrorIcon from '@material-ui/icons/Error';
import { withStyles } from '@material-ui/core/styles';

import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';

import LineChart from './Line';
import TableChart from './Table';
import PieChart from './Pie';
import BarChart from './Bar';
import { DASHBOARD_CHART_MIN_HEIGHT } from './helpers.js';

const cubejsClient = cubejs(process.env.REACT_APP_CUBEJS_API_KEY,
  { apiUrl: process.env.REACT_APP_CUBEJS_API_URL }
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

const Loading = () => (
  <Grid container justify="center">
    <CircularProgress />
  </Grid>
)

const Error = ({ error }) => (
  <>
    <Grid container justify="center">
      <ErrorIcon color="error" fontSize="large" />
    </Grid>
    <Grid container justify="center">
      <Typography align='center' color='error' variant='body1'>
        {error.message}
      </Typography>
    </Grid>
  </>
)

const Chart = ({ title, query, type, classes }) => (
  <Card>
    <CardHeader title={title} />
    <CardContent>
      <div className={classes.chartContainer}>
        <QueryRenderer
          cubejsApi={cubejsClient}
          query={query}
          render={({ resultSet, loadingState, error }) => {
            if (resultSet) {
              return rendetChart(type, { resultSet });
            }

            if (error) {
              return <Error error={error} />
            }

            return <Loading loadingState={loadingState} />
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
