import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import {
  Card,
  CardContent,
  Grid,
  Typography,
  Avatar,
  LinearProgress
} from '@material-ui/core';
import InsertChartIcon from '@material-ui/icons/InsertChartOutlined';
import { QueryRenderer } from "@cubejs-client/react";
import cubejs from "@cubejs-client/core";
import CountUp from 'react-countup';

const cubejsApi = cubejs(process.env.REACT_APP_CUBEJS_TOKEN, {
  apiUrl: process.env.REACT_APP_API_URL
});
const query = { measures: ["Orders.percentOfCompletedOrders"] };

const useStyles = makeStyles(theme => ({
  root: {
    height: '100%'
  },
  content: {
    alignItems: 'center',
    display: 'flex'
  },
  title: {
    fontWeight: 700
  },
  avatar: {
    backgroundColor: theme.palette.primary.main,
    color: theme.palette.primary.contrastText,
    height: 56,
    width: 56
  },
  icon: {
    height: 32,
    width: 32
  },
  progress: {
    marginTop: theme.spacing(3)
  }
}));

const TasksProgress = props => {
  const { className, ...rest } = props;

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
                             separator=","/>
                   %
                  </Typography>
                </Grid>
                <Grid item>
                  <Avatar className={classes.avatar}>
                    <InsertChartIcon className={classes.icon} />
                  </Avatar>
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
