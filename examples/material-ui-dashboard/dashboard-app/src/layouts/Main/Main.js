import React from 'react';
import PropTypes from 'prop-types';
import clsx from 'clsx';
import { makeStyles, useTheme } from '@material-ui/styles';
import { useMediaQuery } from '@material-ui/core';

import palette from '../../theme/palette';
import { Sidebar } from './components';

const useStyles = makeStyles(() => ({
  root: {
    backgroundColor: palette.primary.light,
    position: 'relative',
  },
  shiftContent: {
    paddingLeft: 240,
  },
  content: {
    height: '100%',
    overflow: 'auto'
  },
}));

const Main = (props) => {
  const { children } = props;

  const classes = useStyles();
  const theme = useTheme();
  const isDesktop = useMediaQuery(theme.breakpoints.up('lg'), {
    defaultMatches: true,
  });

  return (
    <div
      className={clsx({
        [classes.root]: true,
        [classes.shiftContent]: isDesktop,
      })}
    >
      <Sidebar onClose={() => {}} open variant={isDesktop ? 'persistent' : 'temporary'} />
      <main className={classes.content}>{children}</main>
    </div>
  );
};

Main.propTypes = {
  children: PropTypes.node,
};

export default Main;
