import React from 'react';
import clsx from 'clsx';
import PropTypes from 'prop-types';
import { makeStyles } from '@material-ui/styles';
import { CardContent, Grid, Typography } from '@material-ui/core';
import { QueryRenderer } from "@cubejs-client/react";
import CountUp from 'react-countup';
import CustomCard from "../../../../components/CustomCard";

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
  avatar: {
    backgroundColor: theme.palette.error.main,
    height: 56,
    width: 56
  },
  icon: {
    height: 32,
    width: 32
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
    color: theme.palette.error.dark,
    marginRight: theme.spacing(1)
  }
}));

const query = { measures: ["Orders.count"] };

const Budget = props => {
  const { className, cubejsApi, ...rest } = props;

  const classes = useStyles();

  return (
    <CustomCard
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
              ORDERS
            </Typography>
            <Typography variant="h3">
              <QueryRenderer
                query={query}
                cubejsApi={cubejsApi}
                render={({ resultSet }) => {
                  if (!resultSet) {
                    return <div className="loader" />;
                  }
                  let data = +resultSet.tablePivot()[0]['Orders.count'];
                  return (
                    <div>
                      <CountUp end={data}
                               duration={1.25}
                               separator=","/>
                    </div>
                  );
                }}
              />
            </Typography>
          </Grid>
        </Grid>
        <div className={classes.difference}>
          <Typography
            className={classes.differenceValue}
            variant="body2"
          >
            - 12%
          </Typography>
          <Typography
            className={classes.caption}
            variant="caption"
          >
            Since last month
          </Typography>
        </div>
      </CardContent>
    </CustomCard>
  );
};

Budget.propTypes = {
  className: PropTypes.string
};

export default Budget;
