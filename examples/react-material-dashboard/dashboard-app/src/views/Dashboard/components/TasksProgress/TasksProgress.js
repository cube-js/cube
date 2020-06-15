import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import {
  Card,
  CardContent,
  Grid,
  Typography,
  LinearProgress
} from '@material-ui/core';
import { QueryRenderer } from "@cubejs-client/react";
import CountUp from 'react-countup';


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
  icon: {
    height: 32,
    width: 32
  },
  progress: {
    marginTop: theme.spacing(3)
  }
}));

const query = { measures: ["Orders.percentOfCompletedOrders"] };

const TasksProgress = props => {
  const { className, cubejsApi, ...rest } = props;

  const classes = useStyles();

  return (
    <Card
      {...rest}
      className={clsx(classes.root, className)}
    >
      <QueryRenderer
        query={query}
        cubejsApi={cubejsApi}
        render={({ resultSet }) => {
          if (!resultSet) {
            return <div className="loader" />;
          }
          let data = +resultSet.tablePivot()[0]['Orders.percentOfCompletedOrders'];
          return (
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
                    COMPLETED ORDERS
                  </Typography>
                  <Typography variant="h3">
                    <CountUp end={data}
                             duration={2}
                             separator=","
                             decimals={1}
                    />
                   %
                  </Typography>
                </Grid>
              </Grid>
              <LinearProgress
                className={classes.progress}
                value={data}
                variant="determinate"
              />
            </CardContent>
          );
        }}
      />
    </Card>
  );
};

TasksProgress.propTypes = {
  className: PropTypes.string
};

export default TasksProgress;
