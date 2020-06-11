import React from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import moment from 'moment';
import { makeStyles } from '@material-ui/styles';
import {
  Card,
  CardContent,
  Avatar,
  Typography,
  Divider,
} from '@material-ui/core';

const useStyles = makeStyles(theme => ({
  root: {
    padding: 10
  },
  details: {
    display: 'flex',
    padding: 5
  },
  avatar: {
    height: 60,
    width: 60,
    flexShrink: 0,
    flexGrow: 0,
    marginRight: 25
  },
  progress: {
    marginTop: theme.spacing(2)
  },
  uploadButton: {
    marginRight: theme.spacing(2)
  }
}));

const AccountProfile = props => {
  const { className, ...rest } = props;

  const classes = useStyles();

  const user = {
    name: 'Shen Zhi',
    city: 'Los Angeles',
    country: 'USA',
    timezone: 'GTM-7',
    avatar: '/images/avatars/avatar_11.png',
    sex: 'male',
    old: 27
  };

  return (
    <Card
      {...rest}
      className={clsx(classes.root, className)}
    >
      <CardContent>
        <div className={classes.details}>
          <Avatar
            className={classes.avatar}
            src={user.avatar}
          />
          <div>
            <Typography
              variant="h4"
            >
              John Doe
            </Typography>
            <Typography
              className={classes.locationText}
              color="textSecondary"
            >
              {capitalizeFirstLetter(user.sex)}, {user.old} years
            </Typography>
          </div>
        </div>
        <div className={classes.progress}>
          <Divider />
          <Typography
            className={classes.dateText}
            color="textSecondary"
            variant="body1"
          >
            {moment().format('hh:mm A')} ({user.timezone})
          </Typography>
        </div>
      </CardContent>
    </Card>
  );
};

AccountProfile.propTypes = {
  className: PropTypes.string
};

export default AccountProfile;

function capitalizeFirstLetter(string) {
  return string.charAt(0).toUpperCase() + string.slice(1);
}
