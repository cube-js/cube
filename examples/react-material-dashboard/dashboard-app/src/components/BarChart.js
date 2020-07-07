import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { Bar } from 'react-chartjs-2';
import { makeStyles } from '@material-ui/styles';
import { Card, CardContent, Divider } from '@material-ui/core';

import { useCubeQuery } from '@cubejs-client/react';
import palette from '../theme/palette';
import moment from 'moment';
import CircularProgress from '@material-ui/core/CircularProgress';
import { options } from '../helpers/BarOptions.js';
import BarChartHeader from './BarChartHeader';

const useStyles = makeStyles(() => ({
  root: {},
  chartContainer: {
    position: 'relative',
    padding: '19px 0',
  },
}));

const BarChart = (props) => {
  const { className, cubejsApi, id, dates, ...rest } = props;
  const classes = useStyles();

  const [dateRange, setDateRange] = React.useState(dates ? dates[0] : 'This week');

  const query = {
    measures: ['Orders.count'],
    timeDimensions: [
      {
        dimension: 'Orders.createdAt',
        granularity: id ? 'month' : 'day',
        dateRange: `${dateRange}`,
      },
    ],
    dimensions: ['Orders.status'],
    filters: id
      ? [
          {
            dimension: 'Users.id',
            operator: 'equals',
            values: [id],
          },
        ]
      : [
          {
            dimension: 'Orders.status',
            operator: 'notEquals',
            values: ['completed'],
          },
        ],
  };
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
    const COLORS_SERIES = [palette.secondary.main, palette.primary.light];
    const data = {
      labels: resultSet.categories().map((c) => moment(c.category).format('DD/MM/YYYY')),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: COLORS_SERIES[index],
        fill: false,
      })),
    };
    return (
      <Card {...rest} className={clsx(classes.root, className)}>
        <BarChartHeader dates={dates} dateRange={dateRange} setDateRange={setDateRange} />
        <Divider />
        <CardContent>
          <div className={classes.chartContainer}>
            <Bar data={data} options={options} />
          </div>
        </CardContent>
      </Card>
    );
  }
};

BarChart.propTypes = {
  className: PropTypes.string,
};

export default BarChart;
