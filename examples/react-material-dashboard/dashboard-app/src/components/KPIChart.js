import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import {makeStyles} from '@material-ui/styles';
import {
  Card,
  CardContent,
  Grid,
  Typography,
  LinearProgress
} from '@material-ui/core';
import {useCubeQuery} from "@cubejs-client/react";
import CountUp from 'react-countup';
import CircularProgress from "@material-ui/core/CircularProgress";

const useStyles = makeStyles(theme => ({
  root: {
    height: '100%'
  },
  content: {
    alignItems: 'center',
    display: 'flex'
  },
  title: {
    fontWeight: 500
  },
  progress: {
    marginTop: theme.spacing(3),
    height: "8px",
    borderRadius: "10px"
  },
  difference: {
    marginTop: theme.spacing(2),
    display: 'flex',
    alignItems: 'center'
  },
  differenceIcon: {
    color: theme.palette.error.dark
  },
  differenceValue: {
    marginRight: theme.spacing(1)
  },
  green: {
    color: theme.palette.success.dark,
  },
  red: {
    color: theme.palette.error.dark,
  },
}));

const KPIChart = props => {
  const classes = useStyles();
  const {
    className,
    cubejsApi,
    title,
    progress,
    query,
    prefix,
    postfix,
    difference,
    value,
    duration,
    ...rest
  } = props;
  const { resultSet, error, isLoading } = useCubeQuery(query, { cubejsApi });

  if (isLoading) {
    return <div style={{display: 'flex', alignItems: 'center', justifyContent: 'center'}}><CircularProgress color="secondary" /></div>;
  }
  if (error) {
    return <pre>{error.toString()}</pre>;
  }
  if (resultSet) {
    return (
      <Card
        {...rest}
        className={clsx(classes.root, className)}
      >
        <CardContent>
          <Grid
            container
            justify="space-between"
          >
            <Grid item>
              <Typography
                className={classes.title}
                color="textSecondary"
                gutterBottom
                variant="body2"
              >
                {title}
              </Typography>
              <Typography variant="h3">
                {prefix}
                <CountUp end={resultSet.seriesNames().map(s => resultSet.totalRow()[s.key])[0]}
                         duration={duration}
                         separator=","
                         decimals={0}
                />
                {postfix}
              </Typography>
            </Grid>
          </Grid>
          {progress ? <LinearProgress
            className={classes.progress}
            value={resultSet.seriesNames().map(s => resultSet.totalRow()[s.key])[0]}
            variant="determinate"
          /> : null}
          {difference ? <div className={classes.difference}>
            <Typography
              className={classes.differenceValue}
              variant="body2"
            >
              {value > 0 ? <span className={classes.green}>{value}%</span> :
                <span className={classes.red}>{value}%</span>}
            </Typography>
            <Typography
              className={classes.caption}
              variant="caption"
            >
              {difference}
            </Typography>
          </div> : null}
        </CardContent>
      </Card>
    );
  } else {
    return null
  }
};

KPIChart.propTypes = {
  className: PropTypes.string,
  title: PropTypes.string,
};

export default KPIChart;
