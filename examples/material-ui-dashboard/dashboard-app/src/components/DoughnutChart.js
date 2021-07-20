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
    return null;
  }
  if (resultSet) {
    const COLORS_SERIES = [
      theme.palette.secondary.light,
      theme.palette.secondary.lighten,
      theme.palette.secondary.main,
    ];
    const data = {
      labels: resultSet.categories().map((c) => c.x),
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
                <Typography variant="h2">
                  {(
                    (status.value /
                      resultSet
                        .series()[0]
                        .series.map((el) => el.value)
                        .reduce(reducer)) *
                    100
                  ).toFixed(0)}
                  %
                </Typography>
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
