import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import { Card, CardContent, Grid, Typography } from '@material-ui/core';
import { QueryRenderer } from "@cubejs-client/react";
import CountUp from 'react-countup';

const useStyles = makeStyles(theme => ({
  root: {
    height: '100%',
  },
  content: {
    alignItems: 'center',
    display: 'flex'
  },
  title: {
    fontWeight: 500
  },
  avatar: {
    backgroundColor: theme.palette.white,
    color: theme.palette.primary.main,
    height: 56,
    width: 56
  },
  icon: {
    height: 32,
    width: 32
  }
}));

const query = {
  "measures": [
    "LineItems.price"
  ],
  "timeDimensions": [],
  "filters": []
};

const TotalProfit = props => {
  const { className, cubejsApi, ...rest } = props;

  const classes = useStyles();

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
              TOTAL PROFIT
            </Typography>
            <Typography
              color="inherit"
              variant="h3"
            >
              $&nbsp;
              <QueryRenderer
                query={query}
                cubejsApi={cubejsApi}
                render={({ resultSet }) => {
                  if (!resultSet) {
                    return <div className="loader"/>;
                  }
                  let data = parseInt(resultSet.tablePivot()[0]['LineItems.price'])
                  return (
                    <CountUp end={data}
                             duration={2.25}
                             separator=","/>
                  );
                }}
              />
            </Typography>
          </Grid>
        </Grid>
      </CardContent>
    </Card>
  );
};

TotalProfit.propTypes = {
  className: PropTypes.string
};

export default TotalProfit;
